
const { io } = require('socket.io-client');
const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');

const HOST_A = 'http://localhost:3000';
const HOST_B = 'http://localhost:3001';
const REDIS_CONTAINER = 'chat-redis';
const roomId = `recovery-${randomUUID()}`;

const clients = [];
let redisStopped = false;
let unexpectedDisconnects = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function docker(...args) {
  return execFileSync('docker', args, {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: 15000,
  }).trim();
}

async function waitForRedis(timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      if (docker('exec', REDIS_CONTAINER, 'redis-cli', 'ping') === 'PONG') {
        return;
      }
    } catch {
      // Redis may still be starting.
    }

    await sleep(1000);
  }

  throw new Error('Redis did not become ready within 30 seconds');
}

function connectClient(url, label) {
  return new Promise((resolve, reject) => {
    const socket = io(url, {
      transports: ['websocket'],
      reconnection: false,
      timeout: 10000,
    });

    clients.push(socket);

    const timer = setTimeout(() => {
      socket.disconnect();
      reject(new Error(`${label} connection timed out`));
    }, 11000);

    socket.once('connect', () => {
      clearTimeout(timer);
      console.log(`${label} connected: ${socket.id}`);
      resolve(socket);
    });

    socket.once('connect_error', (error) => {
      clearTimeout(timer);
      reject(new Error(`${label}: ${error.message}`));
    });

    socket.on('disconnect', (reason) => {
      unexpectedDisconnects++;
      console.error(`${label} DISCONNECTED: ${reason}`);
    });
  });
}

function emitAck(socket, event, payload) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${event} acknowledgement timed out`)),
      5000,
    );

    socket.emit(event, payload, (ack) => {
      clearTimeout(timer);

      if (!ack || ack.success !== true) {
        reject(new Error(ack?.error || `${event} failed`));
        return;
      }

      resolve(ack);
    });
  });
}

async function joinRoom(socket, label) {
  const ack = await emitAck(socket, 'joinRoom', { roomId });
  console.log(`${label} joined room: ${ack.roomId}`);
}

async function sendAndVerify(sender, receiver, label) {
  const messageText = `${label}-${randomUUID()}`;

  const received = new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${label}: receiver did not get the message`)),
      8000,
    );

    const handler = (message) => {
      if (message.text !== messageText) return;

      clearTimeout(timer);
      receiver.off('newMessage', handler);
      resolve(message);
    };

    receiver.on('newMessage', handler);
  });

  const ack = await emitAck(sender, 'sendMessage', {
    roomId,
    text: messageText,
  });

  const message = await received;

  if (message.id !== ack.messageId) {
    throw new Error(`${label}: message ID mismatch`);
  }

  console.log(`${label}: PASS; receiver got ${message.id}`);
}

async function main() {
  try {
    console.log('=== Redis recovery test ===');

    if (docker('exec', REDIS_CONTAINER, 'redis-cli', 'ping') !== 'PONG') {
      throw new Error('Redis is not healthy before the test');
    }

    const [a, b] = await Promise.all([
      connectClient(HOST_A, 'Client A (port 3000)'),
      connectClient(HOST_B, 'Client B (port 3001)'),
    ]);

    await Promise.all([
      joinRoom(a, 'Client A'),
      joinRoom(b, 'Client B'),
    ]);

    // Establish that cross-instance delivery works before the outage.
    await sendAndVerify(a, b, 'BEFORE-OUTAGE');

    console.log('\nStopping Redis; keep both NestJS processes running...');
    docker('stop', REDIS_CONTAINER);
    redisStopped = true;

    await sleep(5000);

    console.log('Redis is stopped.');
    console.log(`Client A connected: ${a.connected}`);
    console.log(`Client B connected: ${b.connected}`);

    console.log('\nRestarting Redis...');
    docker('start', REDIS_CONTAINER);
    await waitForRedis();
    redisStopped = false;

    console.log('Redis returned PONG.');
    await sleep(5000);

    console.log(`Client A still connected: ${a.connected}`);
    console.log(`Client B still connected: ${b.connected}`);

    if (!a.connected || !b.connected) {
      throw new Error(
        'An original WebSocket client disconnected during the Redis outage',
      );
    }

    if (unexpectedDisconnects !== 0) {
      throw new Error('Unexpected WebSocket disconnection was observed');
    }

    // Reuse the SAME sockets and room memberships from before the outage.
    await sendAndVerify(a, b, 'AFTER-OUTAGE-A-TO-B');
    await sendAndVerify(b, a, 'AFTER-OUTAGE-B-TO-A');

    if (unexpectedDisconnects !== 0) {
      throw new Error('Unexpected WebSocket disconnection was observed');
    }

    console.log('\nPASS: Existing clients recovered cross-instance messaging.');
    console.log('Both original sockets remained connected.');
    console.log('Both message directions worked after Redis restarted.');
  } catch (error) {
    console.error('\nFAIL:', error.message);
    process.exitCode = 1;
  } finally {
    // Make a best-effort attempt to restore Redis if the test failed.
    if (redisStopped) {
      try {
        docker('start', REDIS_CONTAINER);
        await waitForRedis();
        console.log('Redis was restored during cleanup.');
      } catch (error) {
        console.error('WARNING: Could not restore Redis:', error.message);
      }
    }

    for (const socket of clients) socket.disconnect();
  }
}

main();
