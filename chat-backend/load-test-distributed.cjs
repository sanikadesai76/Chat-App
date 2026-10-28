
const { io } = require('socket.io-client');

const ROOM = 'bidirectional-test-room';
const TIMEOUT_MS = 8000;

const a = io('http://localhost:3000', {
  transports: ['websocket'],
  reconnection: false,
  timeout: TIMEOUT_MS,
});

const b = io('http://localhost:3001', {
  transports: ['websocket'],
  reconnection: false,
  timeout: TIMEOUT_MS,
});

let receivedByA = false;
let receivedByB = false;
let started = false;
let finished = false;

const timeout = setTimeout(() => {
  finish(false, 'Timed out waiting for cross-instance messages');
}, TIMEOUT_MS);

function finish(success, message) {
  if (finished) return;
  finished = true;
  clearTimeout(timeout);

  console.log(success ? `PASS: ${message}` : `FAIL: ${message}`);
  console.log(`A (:3000) received B's message: ${receivedByA}`);
  console.log(`B (:3001) received A's message: ${receivedByB}`);

  a.disconnect();
  b.disconnect();

  if (!success) process.exitCode = 1;
}

a.on('connect_error', (err) => {
  finish(false, `Port 3000 connection error: ${err.message}`);
});

b.on('connect_error', (err) => {
  finish(false, `Port 3001 connection error: ${err.message}`);
});

a.on('newMessage', (message) => {
  if (message.text === 'Hello from B') {
    receivedByA = true;
    console.log('A received B message');
    checkResults();
  }
});

b.on('newMessage', (message) => {
  if (message.text === 'Hello from A') {
    receivedByB = true;
    console.log('B received A message');
    checkResults();
  }
});

a.on('connect', maybeStart);
b.on('connect', maybeStart);

function maybeStart() {
  if (started || !a.connected || !b.connected) return;
  started = true;

  // Both clients join the same room. Wait briefly for joins to settle.
  a.emit('joinRoom', { roomId: ROOM });
  b.emit('joinRoom', { roomId: ROOM });

  setTimeout(() => {
    if (finished) return;

    console.log('Both clients connected; sending messages...');

    a.emit('sendMessage', {
      roomId: ROOM,
      text: 'Hello from A',
    });

    b.emit('sendMessage', {
      roomId: ROOM,
      text: 'Hello from B',
    });
  }, 500);
}

function checkResults() {
  if (receivedByA && receivedByB) {
    finish(true, 'Bidirectional cross-instance delivery works');
  }
}
