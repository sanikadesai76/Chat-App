const { io } = require('socket.io-client');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');

const URL = process.env.SERVER_URL || 'http://localhost:3000';
const ROOM = `bidirectional-${randomUUID()}`;
const TIMEOUT_MS = 15000;
const OUTSIDER_WAIT_MS = 1000;
const TEXT_A = 'Hello from A';
const TEXT_B = 'Hello from B';

const options = { transports: ['websocket'], reconnection: false };
const a = io(URL, options);
const b = io(URL, options);
const c = io(URL, options); // outsider: connects but never joins ROOM

const received = { a: 0, b: 0, outsider: 0 };
const acks = { a: null, b: null };

let finished = false;
let started = false;
let scheduled = false;

const timeout = setTimeout(() => finish(false, 'Timed out'), TIMEOUT_MS);

function finish(success, message, problems = []) {
  if (finished) return;
  finished = true;
  clearTimeout(timeout);

  console.log(`\n${success ? 'PASS' : 'FAIL'}: ${message}`);
  console.log(`User A received B's message: ${received.a} time(s) (expected 1)`);
  console.log(`User B received A's message: ${received.b} time(s) (expected 1)`);
  console.log(`Outsider C received messages from the room: ${received.outsider} (expected 0)`);
  for (const problem of problems) console.log(`  - ${problem}`);

  a.disconnect();
  b.disconnect();
  c.disconnect();

  if (!success) process.exitCode = 1;
}

a.on('connect_error', err => finish(false, `User A connection failed: ${err.message}`));
b.on('connect_error', err => finish(false, `User B connection failed: ${err.message}`));
c.on('connect_error', err => finish(false, `Outsider C connection failed: ${err.message}`));

a.on('newMessage', msg => {
  if (msg.roomId === ROOM && msg.text === TEXT_B) received.a++;
  maybeVerify();
});

b.on('newMessage', msg => {
  if (msg.roomId === ROOM && msg.text === TEXT_A) received.b++;
  maybeVerify();
});

c.on('newMessage', msg => {
  if (msg.roomId === ROOM) received.outsider++;
});

a.on('connect', startIfReady);
b.on('connect', startIfReady);
c.on('connect', startIfReady);

function ackHandler(who) {
  return result => {
    if (!result?.success) {
      finish(false, `Message from ${who.toUpperCase()} rejected: ${result?.error || 'unknown error'}`);
      return;
    }
    acks[who] = result;
    maybeVerify();
  };
}

function startIfReady() {
  if (started || !a.connected || !b.connected || !c.connected) return;
  started = true;

  let joined = 0;

  function onJoined() {
    joined++;
    if (joined !== 2) return;

    console.log('Users A and B joined the same conversation; outsider C did not.');
    a.emit('sendMessage', { roomId: ROOM, text: TEXT_A }, ackHandler('a'));
    b.emit('sendMessage', { roomId: ROOM, text: TEXT_B }, ackHandler('b'));
  }

  a.emit('joinRoom', { roomId: ROOM }, result => {
    if (!result?.success) return finish(false, 'User A could not join room');
    onJoined();
  });

  b.emit('joinRoom', { roomId: ROOM }, result => {
    if (!result?.success) return finish(false, 'User B could not join room');
    onJoined();
  });
}

function maybeVerify() {
  if (scheduled || finished) return;
  if (acks.a && acks.b && received.a >= 1 && received.b >= 1) {
    scheduled = true;
    // Short wait so late duplicates or wrong deliveries (e.g. to C) can show up.
    setTimeout(verifyDatabase, OUTSIDER_WAIT_MS);
  }
}

function verifyDatabase() {
  if (finished) return;

  const problems = [];

  try {
    const sql = `
      SELECT id::text || '|' || sender_id || '|' || message_text
      FROM chat_messages
      WHERE conversation_id = '${ROOM}'
    `;

    const output = execFileSync('docker', [
      'exec', 'chat-postgres', 'psql',
      '-U', 'chatuser', '-d', 'chatdb',
      '-t', '-A', '-c', sql,
    ], { encoding: 'utf8' });

    const rows = output
      .split(/\r?\n/)
      .filter(Boolean)
      .map(line => {
        const [id, senderId, ...rest] = line.split('|');
        return { id, senderId, text: rest.join('|') };
      });

    const rowA = rows.find(r => r.text === TEXT_A);
    const rowB = rows.find(r => r.text === TEXT_B);

    if (rows.length !== 2) problems.push(`Expected 2 database rows for this room, found ${rows.length}`);

    if (!rowA) problems.push("No database row for A's message");
    else {
      if (rowA.senderId !== a.id) problems.push("Row for A's message has the wrong sender_id");
      if (rowA.id !== acks.a.messageId) problems.push("Row for A's message does not match the acknowledged message id");
    }

    if (!rowB) problems.push("No database row for B's message");
    else {
      if (rowB.senderId !== b.id) problems.push("Row for B's message has the wrong sender_id");
      if (rowB.id !== acks.b.messageId) problems.push("Row for B's message does not match the acknowledged message id");
    }

    if (received.a !== 1) problems.push(`User A received B's message ${received.a} times, expected 1`);
    if (received.b !== 1) problems.push(`User B received A's message ${received.b} times, expected 1`);
    if (received.outsider !== 0) problems.push(`Outsider C received ${received.outsider} message(s) from a room it never joined`);
  } catch (err) {
    finish(false, `Database verification failed: ${err.message}`);
    return;
  }

  if (problems.length === 0) {
    finish(true, 'A and B each received the other\'s message exactly once, outsider C received nothing, and both messages are correctly saved in PostgreSQL');
  } else {
    finish(false, 'One or more checks failed', problems);
  }
}