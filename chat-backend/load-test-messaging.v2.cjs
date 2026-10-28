const { io } = require('socket.io-client');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { performance } = require('node:perf_hooks');

const URL = process.env.SERVER_URL || 'http://localhost:3000';
const N = Number(process.env.CLIENTS || 1000);
const M = Number(process.env.MESSAGES_PER_CLIENT || 10);
const SEND_MODE = process.env.SEND_MODE || 'sequential'; // 'sequential' or 'burst'
const DELIVERY_WAIT_MS = Number(process.env.DELIVERY_WAIT_MS || 15000);
const TIMEOUT = 60000;

// Each client chats with 10 distinct partners (ring offsets on a shuffled order).
const PARTNER_OFFSETS = [1, 2, 3, 4, 5, -1, -2, -3, -4, -5];
const CONVERSATIONS_PER_CLIENT = PARTNER_OFFSETS.length;

const runId = randomUUID();
const shortRun = runId.slice(0, 8);
const PREFIX = `LOADTEST-${runId}-`;

// Random pairing: seeded shuffle so a failing run can be reproduced.
const SEED = Number(process.env.SEED || Math.floor(Math.random() * 2 ** 31));
function mulberry32(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = mulberry32(SEED);
const order = Array.from({ length: N }, (_, i) => i); // order[position] = client
for (let i = N - 1; i > 0; i--) {
  const j = Math.floor(rand() * (i + 1));
  [order[i], order[j]] = [order[j], order[i]];
}
const position = new Array(N);
order.forEach((client, pos) => { position[client] = pos; });

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const partnerOf = (i, k) => {
  const offset = PARTNER_OFFSETS[(k - 1) % CONVERSATIONS_PER_CLIENT];
  return order[(((position[i] + offset) % N) + N) % N];
};
const roomFor = (a, b) => `load-${shortRun}-${Math.min(a, b)}-${Math.max(a, b)}`;

const sockets = new Array(N);
const expected = new Map(); // key "sender-recipient-k" -> message record

let accepted = 0;
let rejected = 0;
let deliveredUnique = 0;
let duplicateDeliveries = 0;
let misdelivered = 0;
let wrongRoomEvents = 0;
let unknownEvents = 0;
const ackLatencies = [];

function emitAck(socket, event, payload) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${event} acknowledgement timed out`)),
      TIMEOUT
    );
    socket.emit(event, payload, result => {
      clearTimeout(timer);
      if (result?.success) resolve(result);
      else reject(new Error(result?.error || `${event} failed`));
    });
  });
}

async function connectClient(index) {
  const socket = io(URL, {
    transports: ['websocket'],
    reconnection: false,
    timeout: TIMEOUT,
  });
  sockets[index] = socket;

  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Client ${index} connection timed out`)),
      TIMEOUT
    );
    socket.once('connect', () => {
      clearTimeout(timer);
      resolve();
    });
    socket.once('connect_error', error => {
      clearTimeout(timer);
      reject(error);
    });
  });

  socket.on('newMessage', message => {
    const text = message?.text;
    if (typeof text !== 'string' || !text.startsWith(PREFIX)) return;

    const entry = expected.get(text.slice(PREFIX.length));
    if (!entry) {
      unknownEvents++;
      return;
    }

    if (index === entry.recipient) {
      if (message.roomId !== entry.room) wrongRoomEvents++;
      entry.deliveries++;
      if (entry.deliveries === 1) {
        entry.deliveredAt = performance.now();
        deliveredUnique++;
      } else {
        duplicateDeliveries++;
      }
    } else if (index !== entry.sender) {
      misdelivered++; // neither sender nor intended recipient
    }
    // The sender also receives its own message (server emits to the whole room); ignored.
  });

  return socket;
}

async function sendOne(client, entry) {
  entry.sentAt = performance.now();
  try {
    await emitAck(client, 'sendMessage', { roomId: entry.room, text: entry.text });
    entry.acked = true;
    accepted++;
    ackLatencies.push(performance.now() - entry.sentAt);
  } catch (error) {
    rejected++;
    if (rejected <= 10) console.error('Send error:', error.message);
  }
}

function percentile(sorted, p) {
  if (!sorted.length) return 'N/A';
  const idx = Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1);
  return `${sorted[idx].toFixed(2)} ms`;
}

async function main() {
  const started = performance.now();

  if (N < 11) {
    console.error('CLIENTS must be at least 11 for 10 distinct partners.');
    process.exit(1);
  }

  // Build the full list of expected messages up front.
  for (let i = 0; i < N; i++) {
    for (let k = 1; k <= M; k++) {
      const r = partnerOf(i, k);
      const key = `${i}-${r}-${k}`;
      expected.set(key, {
        key,
        sender: i,
        recipient: r,
        room: roomFor(i, r),
        text: `${PREFIX}${key}`,
        sentAt: 0,
        acked: false,
        deliveries: 0,
        deliveredAt: null,
      });
    }
  }
  const expectedConversations = new Set([...expected.values()].map(e => e.room)).size;

  console.log(`Test run: ${runId}`);
  console.log(`Pairing seed: ${SEED}`);
  console.log(`Server: ${URL}`);
  console.log(`Clients: ${N}`);
  console.log(`Conversations per client: ${CONVERSATIONS_PER_CLIENT}`);
  console.log(`Messages per client: ${M}`);
  console.log(`Send mode: ${SEND_MODE}`);
  console.log(`Expected messages: ${N * M}`);
  console.log(`Expected distinct conversations: ${expectedConversations}`);

  try {
    console.log('\n1. Connecting clients...');
    const results = await Promise.allSettled(
      Array.from({ length: N }, (_, i) => connectClient(i))
    );
    const failedConnections = results.filter(r => r.status === 'rejected').length;
    console.log(`Connected: ${N - failedConnections}/${N}`);
    console.log(`Failed connections: ${failedConnections}`);
    if (failedConnections > 0) {
      throw new Error('Not all clients connected; stopping the test.');
    }
    const clients = sockets;

    console.log('\n2. Joining rooms (both participants join every room)...');
    const joins = [];
    for (let i = 0; i < N; i++) {
      for (let k = 1; k <= CONVERSATIONS_PER_CLIENT; k++) {
        joins.push(
          emitAck(clients[i], 'joinRoom', { roomId: roomFor(i, partnerOf(i, k)) })
        );
      }
    }
    const joinResults = await Promise.allSettled(joins);
    const joinFailures = joinResults.filter(r => r.status === 'rejected').length;
    console.log(`Room memberships requested: ${joins.length}`);
    console.log(`Failed room joins: ${joinFailures}`);
    if (joinFailures > 0) {
      throw new Error('Some room joins failed; stopping the test.');
    }
    await sleep(500);

    console.log('\n3. Sending messages concurrently...');
    const sendStarted = performance.now();

    if (SEND_MODE === 'burst') {
      const all = [];
      for (let i = 0; i < N; i++) {
        for (let k = 1; k <= M; k++) {
          all.push(sendOne(clients[i], expected.get(`${i}-${partnerOf(i, k)}-${k}`)));
        }
      }
      await Promise.all(all);
    } else {
      // Each client sends its messages one after another; all clients run in parallel.
      await Promise.all(
        clients.map(async (client, i) => {
          for (let k = 1; k <= M; k++) {
            await sendOne(client, expected.get(`${i}-${partnerOf(i, k)}-${k}`));
          }
        })
      );
    }
    const sendSeconds = (performance.now() - sendStarted) / 1000;

    console.log('\n4. Waiting for recipient deliveries...');
    const countMissingDeliveries = () => {
      let missing = 0;
      for (const e of expected.values()) if (e.acked && e.deliveries === 0) missing++;
      return missing;
    };
    const waitStart = performance.now();
    while (
      countMissingDeliveries() > 0 &&
      performance.now() - waitStart < DELIVERY_WAIT_MS
    ) {
      await sleep(100);
    }
    const missingDeliveries = countMissingDeliveries();

    const deliveryLatencies = [];
    for (const e of expected.values()) {
      if (e.deliveredAt !== null) deliveryLatencies.push(e.deliveredAt - e.sentAt);
    }
    deliveryLatencies.sort((a, b) => a - b);

    console.log('\n5. Checking PostgreSQL...');
    const sql = `
      SELECT conversation_id::text || '|' || sender_id::text || '|' || message_text
      FROM chat_messages
      WHERE message_text LIKE '${PREFIX}%'
    `;
    const output = execFileSync(
      'docker',
      [
        'exec', 'chat-postgres',
        'psql', '-U', 'chatuser', '-d', 'chatdb',
        '-t', '-A', '-c', sql,
      ],
      { encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 }
    );

    const rows = output.split(/\r?\n/).filter(Boolean);
    const dbByText = new Map();
    let duplicateDbRows = 0;
    for (const row of rows) {
      const [conversationId, senderId, ...rest] = row.split('|');
      const text = rest.join('|');
      if (dbByText.has(text)) duplicateDbRows++;
      dbByText.set(text, { conversationId, senderId });
    }

    let missingDbRows = 0;
    let wrongConversationRows = 0;
    let wrongSenderRows = 0;
    for (const e of expected.values()) {
      if (!e.acked) continue;
      const row = dbByText.get(e.text);
      if (!row) missingDbRows++;
      else {
        if (row.conversationId !== e.room) wrongConversationRows++;
        if (row.senderId !== clients[e.sender].id) wrongSenderRows++;
      }
    }
    let dbRowsWithoutAck = 0;
    for (const text of dbByText.keys()) {
      const entry = expected.get(text.slice(PREFIX.length));
      if (!entry || !entry.acked) dbRowsWithoutAck++;
    }

    ackLatencies.sort((a, b) => a - b);

    console.log('\n===== LOAD TEST RESULTS =====');
    console.log(`Connected clients: ${N - failedConnections}/${N}`);
    console.log(`Room joins failed: ${joinFailures}`);
    console.log(`Send attempts: ${N * M}`);
    console.log(`Accepted sends (acked): ${accepted}`);
    console.log(`Send failures: ${rejected}`);
    console.log(`Unique recipient deliveries: ${deliveredUnique}`);
    console.log(`Missing recipient deliveries (acked but never received): ${missingDeliveries}`);
    console.log(`Duplicate deliveries to recipient: ${duplicateDeliveries}`);
    console.log(`Deliveries to wrong client: ${misdelivered}`);
    console.log(`Deliveries with wrong roomId: ${wrongRoomEvents}`);
    console.log(`Unrecognised events for this run: ${unknownEvents}`);
    console.log(`DB rows for this run: ${rows.length}`);
    console.log(`Missing DB rows (acked but not saved): ${missingDbRows}`);
    console.log(`DB rows with wrong conversation_id: ${wrongConversationRows}`);
    console.log(`DB rows with wrong sender_id: ${wrongSenderRows}`);
    console.log(`Duplicate DB rows: ${duplicateDbRows}`);
    console.log(`DB rows without an accepted ack: ${dbRowsWithoutAck}`);
    console.log(
      `Send throughput: ${(accepted / Math.max(sendSeconds, 0.001)).toFixed(1)} messages/sec`
    );
    console.log(
      `ACK latency p50 / p95 / p99: ${percentile(ackLatencies, 0.5)} / ${percentile(ackLatencies, 0.95)} / ${percentile(ackLatencies, 0.99)}`
    );
    console.log(
      `Send-to-recipient latency p50 / p95 / p99: ${percentile(deliveryLatencies, 0.5)} / ${percentile(deliveryLatencies, 0.95)} / ${percentile(deliveryLatencies, 0.99)}`
    );
    console.log(`Total elapsed: ${((performance.now() - started) / 1000).toFixed(1)} seconds`);

    const passed =
      failedConnections === 0 &&
      joinFailures === 0 &&
      accepted === N * M &&
      rejected === 0 &&
      missingDeliveries === 0 &&
      duplicateDeliveries === 0 &&
      misdelivered === 0 &&
      wrongRoomEvents === 0 &&
      missingDbRows === 0 &&
      wrongConversationRows === 0 &&
      wrongSenderRows === 0 &&
      duplicateDbRows === 0 &&
      dbRowsWithoutAck === 0;

    console.log(
      passed
        ? '\nPASS: every message was acknowledged, delivered to its intended recipient, and saved correctly.'
        : '\nFAIL: check the counts above.'
    );
    if (!passed) process.exitCode = 1;
  } catch (error) {
    console.error('\nTEST FAILED:', error.message);
    process.exitCode = 1;
  } finally {
    for (const socket of sockets) if (socket) socket.disconnect();
  }
}

main();