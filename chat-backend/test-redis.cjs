
const { io } = require('socket.io-client');

const clientA = io('http://localhost:3000', {
  transports: ['websocket'],
});

const clientB = io('http://localhost:3001', {
  transports: ['websocket'],
});

const roomId = 'distributed-test-room';
const expectedText = 'Hello across instances';

let joinedA = false;
let joinedB = false;
let sent = false;
let received = false;
let finished = false;

const timeout = setTimeout(() => {
  finish(new Error('Timed out waiting for cross-instance delivery'));
}, 10000);

function finish(error) {
  if (finished) return;
  finished = true;
  clearTimeout(timeout);
  clientA.disconnect();
  clientB.disconnect();

  if (error) {
    console.error('\nFAIL:', error.message);
    process.exitCode = 1;
  } else {
    console.log('\nPASS: Redis distributed messaging works!');
  }
}

clientA.on('connect_error', (err) =>
  finish(new Error(`Instance A connection: ${err.message}`)),
);
clientB.on('connect_error', (err) =>
  finish(new Error(`Instance B connection: ${err.message}`)),
);

function join(client, name, markJoined) {
  client.emit('joinRoom', { roomId }, (result) => {
    if (!result?.success) {
      finish(new Error(`${name} could not join the room`));
      return;
    }

    console.log(`${name} joined ${roomId}`);
    markJoined();
    trySend();
  });
}

clientA.on('connect', () => {
  console.log('Client A connected to port 3000');
  join(clientA, 'Client A', () => { joinedA = true; });
});

clientB.on('connect', () => {
  console.log('Client B connected to port 3001');
  join(clientB, 'Client B', () => { joinedB = true; });
});

function trySend() {
  if (!joinedA || !joinedB || sent || finished) return;
  sent = true;

  clientB.on('newMessage', (message) => {
    if (message.text !== expectedText || finished) return;

    received = true;
    console.log('Client B received:', message.text);
    console.log('Room:', message.roomId);
  });

  clientA.emit(
    'sendMessage',
    { roomId, text: expectedText },
    (result) => {
      if (!result?.success) {
        finish(new Error(result?.error || 'Message send failed'));
        return;
      }

      console.log('Instance A acknowledged message');

      setTimeout(() => {
        if (!received) {
          finish(new Error('Message did not reach Client B on instance B'));
        } else {
          finish();
        }
      }, 500);
    },
  );
}
