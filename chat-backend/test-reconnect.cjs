
const { io } = require('socket.io-client');

const URL = 'http://localhost:8080';
const ROOM_ID = 'reconnect-test-room';

const client = io(URL, {
  transports: ['websocket'],
  reconnection: true,
  reconnectionAttempts: Infinity,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 3000,
  timeout: 10000,
});

let connectionCount = 0;
let joined = false;
let testStarted = false;
let timeout;

function fail(message) {
  console.error(`FAIL: ${message}`);
  clearTimeout(timeout);
  client.disconnect();
  process.exit(1);
}

client.on('connect', () => {
  connectionCount++;

  console.log(`Connected: ${client.id}`);
  console.log(`Connection count: ${connectionCount}`);

  client.emit('joinRoom', { roomId: ROOM_ID }, (result) => {
    if (!result?.success) {
      fail(`Room join failed: ${result?.error}`);
      return;
    }

    joined = true;
    console.log(`Joined room: ${ROOM_ID}`);

    if (connectionCount >= 2) {
      console.log('PASS: Client reconnected and rejoined room.');

      client.emit(
        'sendMessage',
        {
          roomId: ROOM_ID,
          text: 'Message after reconnection',
        },
        (ack) => {
          if (!ack?.success) {
            fail('Message send failed after reconnection.');
            return;
          }

          console.log('Message acknowledged by server.');
        },
      );
    }
  });
});

client.on('disconnect', (reason) => {
  joined = false;
  console.log('Disconnected:', reason);
});

client.on('connect_error', (error) => {
  console.log('Connection error:', error.message);
});

client.io.on('reconnect_attempt', (attempt) => {
  console.log(`Reconnect attempt: ${attempt}`);
});

client.io.on('reconnect', (attempt) => {
  console.log(`Reconnected after ${attempt} attempt(s).`);
});

client.on('newMessage', (message) => {
  console.log('Received message:', message.text);

  if (
    connectionCount >= 2 &&
    message.text === 'Message after reconnection'
  ) {
    console.log('PASS: Message received after reconnection.');
    clearTimeout(timeout);
    client.disconnect();
    process.exit(0);
  }
});

timeout = setTimeout(() => {
  fail(
    `Test timed out. Connections: ${connectionCount}, joined: ${joined}`,
  );
}, 120000);
