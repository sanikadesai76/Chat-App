
const { io } = require('socket.io-client');

const URL = 'http://localhost:3000';
const ROOM_ID = 'test-room';
const TEST_MESSAGE = 'Hello from Client A';

const clientA = io(URL, {
  transports: ['websocket'],
});

const clientB = io(URL, {
  transports: ['websocket'],
});

let aJoined = false;
let bJoined = false;
let sent = false;
let received = false;
let finished = false;

const timeout = setTimeout(() => {
  finish(new Error('Test timed out after 10 seconds'));
}, 10000);

function finish(error) {
  if (finished) return;
  finished = true;
  clearTimeout(timeout);

  clientA.disconnect();
  clientB.disconnect();

  if (error) {
    console.error('FAIL:', error.message);
    process.exitCode = 1;
  } else {
    console.log('\nPASS: Real-time room messaging works!');
  }
}

function failOnConnectionError(client, name) {
  client.on('connect_error', (error) => {
    console.error(`${name} connection error:`, error.message);
    finish(error);
  });
}

failOnConnectionError(clientA, 'Client A');
failOnConnectionError(clientB, 'Client B');

function trySendingMessage() {
  if (!aJoined || !bJoined || sent || finished) return;
  sent = true;

  console.log('\nBoth clients joined the room. Sending message...');

  clientB.on('newMessage', (message) => {
    if (finished || message.text !== TEST_MESSAGE) return;

    received = true;
    console.log('Client B received:', message.text);
    console.log('Sender ID:', message.senderId);
    console.log('Room ID:', message.roomId);
    console.log('Timestamp:', message.timestamp);

    finish();
  });

  clientA.emit(
    'sendMessage',
    { roomId: ROOM_ID, text: TEST_MESSAGE },
    (result) => {
      if (!result?.success) {
        finish(new Error(result?.error || 'Message send failed'));
        return;
      }

      console.log('Server acknowledged message:', result.messageId);

      setTimeout(() => {
        if (!received && !finished) {
          finish(new Error('Client B did not receive the message'));
        }
      }, 500);
    },
  );
}

function joinRoom(client, name, markJoined) {
  client.emit('joinRoom', { roomId: ROOM_ID }, (result) => {
    if (!result?.success) {
      finish(new Error(`${name} failed to join: ${result?.error}`));
      return;
    }

    console.log(`${name} joined room: ${ROOM_ID}`);
    markJoined();
    trySendingMessage();
  });
}

clientA.on('connect', () => {
  console.log('Client A connected:', clientA.id);
  joinRoom(clientA, 'Client A', () => {
    aJoined = true;
  });
});

clientB.on('connect', () => {
  console.log('Client B connected:', clientB.id);
  joinRoom(clientB, 'Client B', () => {
    bJoined = true;
  });
});
