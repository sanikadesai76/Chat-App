
const { io } = require('socket.io-client');
const { performance } = require('node:perf_hooks');

const HOSTS = [
  'http://localhost:3000',
  'http://localhost:3001',
];

const ROOMS = Number(process.env.ROOMS || 50);
const MESSAGES_PER_CLIENT = Number(
  process.env.MESSAGES_PER_CLIENT || 20,
);
const CONNECT_TIMEOUT_MS = 10000;
const ROOM_SETTLE_MS = 1000;
const DELIVERY_TIMEOUT_MS = 10000;

const clients = [];
const pairs = [];
const receivedCounts = new Map();
const deliveryLatencies = [];
const sendAckLatencies = [];
const expectedIds = new Set();

let sendAttempts = 0;
let acceptedSends = 0;
let rejectedSends = 0;
let receivedEvents = 0;
let connectionFailures = 0;
let unexpectedDisconnects = 0;
let finished = false;

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(
    sorted.length - 1,
    Math.ceil((p / 100) * sorted.length) - 1,
  )];
}

function stats(values) {
  if (!values.length) {
    return { min: null, median: null, p95: null, p99: null, max: null };
  }

  return {
    min: Number(Math.min(...values).toFixed(2)),
    median: Number(percentile(values, 50).toFixed(2)),
    p95: Number(percentile(values, 95).toFixed(2)),
    p99: Number(percentile(values, 99).toFixed(2)),
    max: Number(Math.max(...values).toFixed(2)),
  };
}

function createClient(url) {
  return new Promise((resolve, reject) => {
    const socket = io(url, {
      transports: ['websocket'],
      reconnection: false,
      timeout: CONNECT_TIMEOUT_MS,
    });

    clients.push(socket);

    const timer = setTimeout(() => {
      socket.disconnect();
      reject(new Error(`Connection timeout: ${url}`));
    }, CONNECT_TIMEOUT_MS + 1000);

    socket.once('connect', () => {
      clearTimeout(timer);

      socket.on('disconnect', (reason) => {
        if (!finished && reason !== 'io client disconnect') {
          unexpectedDisconnects++;
        }
      });

      socket.on('newMessage', (message) => {
        receivedEvents++;

        const key = message.id;
        receivedCounts.set(key, (receivedCounts.get(key) || 0) + 1);

        // The gateway timestamps messages immediately before broadcasting.
        const serverTime = Date.parse(message.timestamp);
        if (Number.isFinite(serverTime)) {
          const latency = Date.now() - serverTime;
          if (latency >= 0) deliveryLatencies.push(latency);
        }
      });

      resolve(socket);
    });

    socket.once('connect_error', (err) => {
      clearTimeout(timer);
      reject(new Error(`${url}: ${err.message}`));
    });
  });
}

function emitWithAck(socket, event, payload) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${event} acknowledgement timeout`)),
      CONNECT_TIMEOUT_MS,
    );

    const started = performance.now();

    socket.emit(event, payload, (ack) => {
      clearTimeout(timer);
      const elapsed = performance.now() - started;

      if (!ack || ack.success === false) {
        reject(new Error(ack?.error || `${event} rejected`));
        return;
      }

      resolve({ ack, elapsed });
    });
  });
}

async function main() {
  if (!Number.isInteger(ROOMS) || ROOMS < 1 ||
      !Number.isInteger(MESSAGES_PER_CLIENT) ||
      MESSAGES_PER_CLIENT < 1) {
    throw new Error('ROOMS and MESSAGES_PER_CLIENT must be positive integers');
  }

  console.log('=== Distributed chat delivery benchmark ===');
  console.log(`Instance A: ${HOSTS[0]}`);
  console.log(`Instance B: ${HOSTS[1]}`);
  console.log(`Rooms: ${ROOMS}`);
  console.log(`Clients: ${ROOMS * 2}`);
  console.log(`Messages per client: ${MESSAGES_PER_CLIENT}`);
  console.log(`Expected sends: ${ROOMS * 2 * MESSAGES_PER_CLIENT}`);

  const connectionStart = performance.now();

  // Each room has exactly two clients: one on each backend instance.
  for (let i = 0; i < ROOMS; i++) {
    const [a, b] = await Promise.all([
      createClient(HOSTS[0]),
      createClient(HOSTS[1]),
    ]).catch((err) => {
      connectionFailures++;
      throw err;
    });

    const roomId = `delivery-benchmark-${Date.now()}-${i}`;

    await Promise.all([
      emitWithAck(a, 'joinRoom', { roomId }),
      emitWithAck(b, 'joinRoom', { roomId }),
    ]);

    pairs.push({ roomId, a, b });
  }

  const connectionSeconds = (performance.now() - connectionStart) / 1000;

  console.log('\nAll clients connected and joined rooms.');
  console.log(`Connection and room setup: ${connectionSeconds.toFixed(2)}s`);

  // Allow room membership and adapter propagation to settle.
  await sleep(ROOM_SETTLE_MS);

  const benchmarkStart = performance.now();

  // Both clients in each room send messages. Each accepted message should
  // be delivered to both room members, including the sender.
  for (const pair of pairs) {
    for (const socket of [pair.a, pair.b]) {
      for (let i = 0; i < MESSAGES_PER_CLIENT; i++) {
        sendAttempts++;

        try {
          const { ack, elapsed } = await emitWithAck(
            socket,
            'sendMessage',
            {
              roomId: pair.roomId,
              text: `Delivery benchmark ${i}`,
            },
          );

          acceptedSends++;
          sendAckLatencies.push(elapsed);

          if (ack.messageId) {
            expectedIds.add(ack.messageId);
          } else {
            rejectedSends++;
            console.error('Missing messageId in successful acknowledgement');
          }
        } catch (err) {
          rejectedSends++;
          console.error(`Send failed: ${err.message}`);
        }
      }
    }
  }

  const sendPhaseSeconds = (performance.now() - benchmarkStart) / 1000;

  // Wait for broadcasts to arrive before calculating missing deliveries.
  await sleep(DELIVERY_TIMEOUT_MS);

  const expectedDeliveries = acceptedSends * 2;
  let missing = 0;
  let duplicates = 0;
  let wrongRecipientCounts = 0;

  for (const id of expectedIds) {
    const count = receivedCounts.get(id) || 0;

    if (count < 2) missing += 2 - count;
    if (count > 2) duplicates += count - 2;
    if (count !== 2) wrongRecipientCounts++;
  }

  // If a send was accepted but its ID never appeared at any recipient,
  // count both expected recipient deliveries as missing.
  missing += Math.max(0, acceptedSends - expectedIds.size) * 2;

  const deliveryRate = expectedDeliveries
    ? (Math.min(receivedEvents, expectedDeliveries) / expectedDeliveries) * 100
    : 0;

  const report = {
    instances: HOSTS,
    rooms: ROOMS,
    clients: clients.length,
    connectionFailures,
    unexpectedDisconnects,
    sendAttempts,
    acceptedSends,
    rejectedSends,
    expectedDeliveries,
    receivedEvents,
    missingDeliveries: missing,
    duplicateDeliveries: duplicates,
    messagesWithUnexpectedRecipientCount: wrongRecipientCounts,
    deliveryRatePercent: Number(deliveryRate.toFixed(2)),
    sendPhaseSeconds: Number(sendPhaseSeconds.toFixed(2)),
    acceptedMessagesPerSecond: Number(
      (acceptedSends / sendPhaseSeconds).toFixed(2),
    ),
    deliveryLatencyMs: stats(deliveryLatencies),
    sendAcknowledgementLatencyMs: stats(sendAckLatencies),
  };

  finished = true;
  for (const socket of clients) socket.disconnect();

  console.log('\n=== BENCHMARK REPORT ===');
  console.log(JSON.stringify(report, null, 2));

  const passed =
    connectionFailures === 0 &&
    rejectedSends === 0 &&
    unexpectedDisconnects === 0 &&
    acceptedSends === sendAttempts &&
    receivedEvents === expectedDeliveries &&
    missing === 0 &&
    duplicates === 0 &&
    wrongRecipientCounts === 0;

  console.log(`\nRESULT: ${passed ? 'PASS' : 'CHECK REQUIRED'}`);
  if (!passed) process.exitCode = 1;
}

main().catch((err) => {
  console.error('\nBENCHMARK FAILED:', err.message);
  finished = true;
  for (const socket of clients) socket.disconnect();
  process.exitCode = 1;
});
