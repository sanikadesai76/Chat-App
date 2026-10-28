const { io } = require('socket.io-client');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { performance } = require('node:perf_hooks');
const fs = require('node:fs');

const URL = process.env.SERVER_URL || 'http://localhost:3000';
const N = Number(process.env.CLIENTS || 1000);
const DURATION_MIN = Number(process.env.DURATION_MINUTES || 10);
const THINK_MIN_MS = Number(process.env.THINK_MIN_MS || 500);
const THINK_MAX_MS = Number(process.env.THINK_MAX_MS || 3000);
const DRAIN_WAIT_MS = Number(process.env.DELIVERY_WAIT_MS || 15000);
const TARGET_MS = Number(process.env.TARGET_MS || 500);
const REPORT_EVERY_MS = 10000;
const TIMEOUT = 60000;

// Each client chats with 10 distinct partners (ring offsets on a shuffled order).
const PARTNER_OFFSETS = [1, 2, 3, 4, 5, -1, -2, -3, -4, -5];
const CONVERSATIONS_PER_CLIENT = PARTNER_OFFSETS.length;

const MESSAGE_TEXTS = [
  'hey', 'hello', 'hi', 'how are you?', "what's up?", 'good morning',
  'ok', 'cool', 'thanks!', 'see you soon', 'are you free today?',
  'call me later', 'sounds good', 'on my way', 'nice',
];

const runId = randomUUID();
const shortRun = runId.slice(0, 8);
const ROOM_PREFIX = `sus-${shortRun}-`;

function mulberry32(a) {
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Random pairing: seeded shuffle so a run can be reproduced.
const SEED = Number(process.env.SEED || Math.floor(Math.random() * 2 ** 31));
const rand = mulberry32(SEED);
const order = Array.from({ length: N }, (_, i) => i);
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
const roomFor = (a, b) => `${ROOM_PREFIX}${Math.min(a, b)}-${Math.max(a, b)}`;
const sqlStr = s => `'${String(s).replace(/'/g, "''")}'`;

const sockets = new Array(N);
const pending = new Map(); // message id -> acknowledged message waiting for deliveries
const early = new Map();   // message id -> events that arrived before the ack

let sendStarted = 0;
let stopAt = 0;
let accepted = 0;
let rejected = 0;
let delivered = 0;
let duplicateDeliveries = 0;
let duplicateSelf = 0;
let misdelivered = 0;
let wrongRoom = 0;
let wrongText = 0;

const buckets = []; // one bucket per minute of sending
function bucketFor(t) {
  const idx = Math.max(0, Math.floor((t - sendStarted) / 60000));
  if (!buckets[idx]) buckets[idx] = { ack: [], del: [], failed: 0 };
  return buckets[idx];
}

let win = { ack: [], del: [] };
let winAcked = 0;
let lastReport = 0;

function pct(sorted, p) {
  if (!sorted.length) return NaN;
  const idx = Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1);
  return sorted[idx];
}
function stats(arr) {
  const s = Float64Array.from(arr).sort();
  return { p50: pct(s, 0.5), p95: pct(s, 0.95), p99: pct(s, 0.99), count: s.length };
}
const fmt = v => (Number.isNaN(v) ? 'N/A' : `${v.toFixed(0)} ms`);
const mmss = sec => `${String(Math.floor(sec / 60)).padStart(2, '0')}:${String(Math.floor(sec % 60)).padStart(2, '0')}`;

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

function processEvent(id, e, ev) {
  if (ev.client === e.recipient) {
    if (e.recipientGot) {
      duplicateDeliveries++;
    } else {
      e.recipientGot = true;
      delivered++;
      const lat = ev.at - e.sentAt;
      bucketFor(e.sentAt).del.push(lat);
      win.del.push(lat);
    }
    if (ev.roomId !== e.room) wrongRoom++;
    if (ev.text !== e.text) wrongText++;
  } else if (ev.client === e.sender) {
    // The sender also receives its own message (server emits to the whole room).
    if (e.senderGot) duplicateSelf++;
    else e.senderGot = true;
  } else {
    misdelivered++;
  }
  if (e.recipientGot && e.senderGot) pending.delete(id);
}

function onAck(id, e) {
  const lat = performance.now() - e.sentAt;
  accepted++;
  winAcked++;
  bucketFor(e.sentAt).ack.push(lat);
  win.ack.push(lat);
  e.recipientGot = false;
  e.senderGot = false;
  pending.set(id, e);
  const evs = early.get(id);
  if (evs) {
    early.delete(id);
    for (const ev of evs) processEvent(id, e, ev);
  }
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
    if (
      !message?.id ||
      typeof message.roomId !== 'string' ||
      !message.roomId.startsWith(ROOM_PREFIX)
    ) {
      return;
    }
    const ev = {
      client: index,
      roomId: message.roomId,
      text: message.text,
      at: performance.now(),
    };
    const e = pending.get(message.id);
    if (e) {
      processEvent(message.id, e, ev);
      return;
    }
    let list = early.get(message.id);
    if (!list) {
      list = [];
      early.set(message.id, list);
    }
    list.push(ev);
  });

  return socket;
}

async function clientLoop(i, client) {
  await sleep(Math.random() * THINK_MAX_MS); // stagger the start
  while (performance.now() < stopAt) {
    const k = 1 + Math.floor(Math.random() * CONVERSATIONS_PER_CLIENT);
    const r = partnerOf(i, k);
    const room = roomFor(i, r);
    const text = MESSAGE_TEXTS[Math.floor(Math.random() * MESSAGE_TEXTS.length)];
    const sentAt = performance.now();
    try {
      const result = await emitAck(client, 'sendMessage', { roomId: room, text });
      onAck(result.messageId, { sender: i, recipient: r, room, text, sentAt });
    } catch (error) {
      rejected++;
      bucketFor(sentAt).failed++;
      if (rejected <= 10) console.error('Send error:', error.message);
    }
    await sleep(THINK_MIN_MS + Math.random() * (THINK_MAX_MS - THINK_MIN_MS));
  }
}

async function main() {
  const started = performance.now();

  if (N < 11) {
    console.error('CLIENTS must be at least 11 for 10 distinct partners.');
    process.exit(1);
  }

  const avgThink = (THINK_MIN_MS + THINK_MAX_MS) / 2;
  console.log(`Test run: ${runId}`);
  console.log(`Pairing seed: ${SEED}`);
  console.log(`Server: ${URL}`);
  console.log(`Clients: ${N}`);
  console.log(`Conversations per client: ${CONVERSATIONS_PER_CLIENT}`);
  console.log(`Duration: ${DURATION_MIN} minutes`);
  console.log(`Pause between each client's messages: ${THINK_MIN_MS}-${THINK_MAX_MS} ms (random)`);
  console.log(`Approximate offered load: ${(N / (avgThink / 1000)).toFixed(0)} messages/sec`);

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

    console.log(`\n3. Sending messages for ${DURATION_MIN} minutes (live line every 10 s)...`);
    sendStarted = performance.now();
    stopAt = sendStarted + DURATION_MIN * 60000;
    lastReport = sendStarted;

    const reporter = setInterval(() => {
      const now = performance.now();
      const secs = (now - lastReport) / 1000;
      const a = stats(win.ack);
      const d = stats(win.del);
      console.log(
        `[${mmss((now - sendStarted) / 1000)}] acked=${accepted} failed=${rejected} ` +
        `rate=${(winAcked / secs).toFixed(0)}/s | ack p50/p95/p99 = ${fmt(a.p50)} / ${fmt(a.p95)} / ${fmt(a.p99)} ` +
        `| to-recipient p50 = ${fmt(d.p50)}`
      );
      win = { ack: [], del: [] };
      winAcked = 0;
      lastReport = now;
    }, REPORT_EVERY_MS);

    await Promise.all(clients.map((client, i) => clientLoop(i, client)));
    clearInterval(reporter);
    const sendSeconds = (performance.now() - sendStarted) / 1000;

    console.log('\n4. Waiting for the last deliveries...');
    const countMissing = () => {
      let missing = 0;
      for (const e of pending.values()) if (!e.recipientGot) missing++;
      return missing;
    };
    const waitStart = performance.now();
    while (countMissing() > 0 && performance.now() - waitStart < DRAIN_WAIT_MS) {
      await sleep(100);
    }
    const missingDeliveries = countMissing();
    let unmatchedEvents = 0;
    for (const list of early.values()) unmatchedEvents += list.length;

    console.log('\n5. Checking PostgreSQL...');
    const mapping = clients.map((c, i) => `(${sqlStr(c.id)}, ${i})`).join(',');
    const restFrom = ROOM_PREFIX.length + 1;
    const sql = `
WITH m(sid, uid) AS (VALUES ${mapping})
SELECT
  (SELECT COUNT(*) FROM chat_messages WHERE conversation_id LIKE '${ROOM_PREFIX}%'),
  (SELECT COUNT(*) FROM chat_messages c LEFT JOIN m ON m.sid = c.sender_id
     WHERE c.conversation_id LIKE '${ROOM_PREFIX}%'
       AND (m.uid IS NULL OR m.uid NOT IN (
         split_part(substr(c.conversation_id, ${restFrom}), '-', 1)::int,
         split_part(substr(c.conversation_id, ${restFrom}), '-', 2)::int))),
  (SELECT COUNT(*) FROM chat_messages WHERE conversation_id LIKE '${ROOM_PREFIX}%'
     AND message_text NOT IN (${MESSAGE_TEXTS.map(sqlStr).join(',')})),
  (SELECT COUNT(DISTINCT conversation_id) FROM chat_messages WHERE conversation_id LIKE '${ROOM_PREFIX}%');
`;
    const dbOutput = execFileSync(
      'docker',
      ['exec', '-i', 'chat-postgres', 'psql', '-U', 'chatuser', '-d', 'chatdb', '-t', '-A', '-F', '|'],
      { input: sql, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }
    );
    const line = dbOutput.split(/\r?\n/).filter(Boolean).pop() || '';
    const [dbRows, badSenderRows, badTextRows, dbConversations] = line.split('|').map(Number);

    // Per-minute summary + overall stats.
    const allAck = [];
    const allDel = [];
    const csvLines = [
      'minute,acked,failed,msgs_per_sec,ack_p50_ms,ack_p95_ms,ack_p99_ms,recipient_p50_ms,recipient_p95_ms,recipient_p99_ms',
    ];
    console.log('\n===== PER-MINUTE LATENCY =====');
    buckets.forEach((b, idx) => {
      if (!b) return;
      for (const v of b.ack) allAck.push(v);
      for (const v of b.del) allDel.push(v);
      const a = stats(b.ack);
      const d = stats(b.del);
      const secs = Math.max(1, Math.min(60, sendSeconds - idx * 60));
      const rate = b.ack.length / secs;
      console.log(
        `minute ${idx + 1}: acked=${b.ack.length} failed=${b.failed} rate=${rate.toFixed(0)}/s | ` +
        `ack p50/p95/p99 = ${fmt(a.p50)} / ${fmt(a.p95)} / ${fmt(a.p99)}`
      );
      csvLines.push([
        idx + 1, b.ack.length, b.failed, rate.toFixed(1),
        a.p50.toFixed(1), a.p95.toFixed(1), a.p99.toFixed(1),
        d.p50.toFixed(1), d.p95.toFixed(1), d.p99.toFixed(1),
      ].join(','));
    });
    const csvPath = `sustained-${shortRun}-per-minute.csv`;
    fs.writeFileSync(csvPath, csvLines.join('\n'), 'utf8');

    const ack = stats(allAck);
    const del = stats(allDel);
    const verdict = v => (v < TARGET_MS ? 'met' : 'NOT met');

    console.log('\n===== SUSTAINED LOAD TEST RESULTS =====');
    console.log(`Connected clients: ${N - failedConnections}/${N}`);
    console.log(`Room joins failed: ${joinFailures}`);
    console.log(`Sending duration: ${(sendSeconds / 60).toFixed(1)} minutes`);
    console.log(`Accepted sends (acked): ${accepted}`);
    console.log(`Send failures: ${rejected}`);
    console.log(`Unique recipient deliveries: ${delivered}`);
    console.log(`Missing recipient deliveries (acked but never received): ${missingDeliveries}`);
    console.log(`Duplicate deliveries to recipient: ${duplicateDeliveries}`);
    console.log(`Duplicate copies to sender: ${duplicateSelf}`);
    console.log(`Deliveries to wrong client: ${misdelivered}`);
    console.log(`Deliveries with wrong roomId: ${wrongRoom}`);
    console.log(`Deliveries with wrong text: ${wrongText}`);
    console.log(`Unmatched events (no acknowledged message): ${unmatchedEvents}`);
    console.log(`DB rows for this run: ${dbRows} (expected ${accepted})`);
    console.log(`DB conversations with messages: ${dbConversations}`);
    console.log(`DB rows whose sender is not in the conversation: ${badSenderRows}`);
    console.log(`DB rows with unexpected message text: ${badTextRows}`);
    console.log(`Average throughput: ${(accepted / Math.max(sendSeconds, 0.001)).toFixed(1)} messages/sec`);
    console.log(`ACK latency p50 / p95 / p99: ${fmt(ack.p50)} / ${fmt(ack.p95)} / ${fmt(ack.p99)}`);
    console.log(`Send-to-recipient latency p50 / p95 / p99: ${fmt(del.p50)} / ${fmt(del.p95)} / ${fmt(del.p99)}`);
    console.log(`Latency target (< ${TARGET_MS} ms) on ACK latency: p50 ${verdict(ack.p50)}, p95 ${verdict(ack.p95)}, p99 ${verdict(ack.p99)}`);
    console.log(`Total elapsed: ${((performance.now() - started) / 1000 / 60).toFixed(1)} minutes`);
    console.log(`Per-minute summary saved to: ${csvPath}`);

    const passed =
      failedConnections === 0 &&
      joinFailures === 0 &&
      rejected === 0 &&
      missingDeliveries === 0 &&
      duplicateDeliveries === 0 &&
      misdelivered === 0 &&
      wrongRoom === 0 &&
      wrongText === 0 &&
      unmatchedEvents === 0 &&
      dbRows === accepted &&
      badSenderRows === 0 &&
      badTextRows === 0;

    console.log(
      passed
        ? '\nPASS (correctness): every message was acknowledged, delivered to its intended recipient, and saved. Check the latency lines above separately.'
        : '\nFAIL (correctness): check the counts above.'
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