

const { io } = require('socket.io-client');

const TARGET = process.env.TARGET || 'http://localhost:3000';
const TOTAL_CLIENTS = Number(process.env.CLIENTS || 1000);
const HOLD_SECONDS = Number(process.env.HOLD_SECONDS || 120);
const CONNECT_TIMEOUT_MS = 10000;

let connected = 0;
let failed = 0;
let unexpectedDisconnects = 0;
let testFinished = false;

const clients = [];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log('=== Concurrent WebSocket Connection Test ===');
  console.log(`Target: ${TARGET}`);
  console.log(`Clients: ${TOTAL_CLIENTS}`);
  console.log(`Hold duration: ${HOLD_SECONDS}s`);
  console.log('Connecting clients...');

  const startedAt = Date.now();

  try {
    const tasks = Array.from({ length: TOTAL_CLIENTS }, async (_, i) => {
      await sleep(i * 20);

      if (testFinished) return;

      const socket = io(TARGET, {
        transports: ['websocket'],
        reconnection: false,
        timeout: CONNECT_TIMEOUT_MS,
      });

      clients.push(socket);

      await new Promise((resolve) => {
        let settled = false;

        const settle = () => {
          if (settled) return;
          settled = true;
          resolve();
        };

        socket.once('connect', () => {
          connected++;
          settle();
        });

        socket.once('connect_error', (error) => {
          failed++;
          console.error(`Connection failed: ${error.message}`);
          settle();
        });

        socket.on('disconnect', (reason) => {
          if (!testFinished) {
            unexpectedDisconnects++;
            console.error(`Unexpected disconnect: ${reason}`);
          }
        });

        setTimeout(settle, CONNECT_TIMEOUT_MS + 1000);
      });
    });

    const progress = setInterval(() => {
      console.log(
        `Connected: ${connected}/${TOTAL_CLIENTS} | ` +
        `Failed: ${failed} | ` +
        `Unexpected disconnects: ${unexpectedDisconnects}`,
      );
    }, 5000);

    await Promise.all(tasks);

    console.log('\nConnection attempts finished.');
    console.log(`Successful connections: ${connected}`);
    console.log(`Failed connections: ${failed}`);
    console.log(
      `Connection phase: ${((Date.now() - startedAt) / 1000).toFixed(1)}s`,
    );
    console.log(`Holding connections for ${HOLD_SECONDS}s...`);

    await sleep(HOLD_SECONDS * 1000);

    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);

    testFinished = true;
    clearInterval(progress);

    console.log('\n=== Final Results ===');
    console.log(`Target clients: ${TOTAL_CLIENTS}`);
    console.log(`Successful connections: ${connected}`);
    console.log(`Failed connections: ${failed}`);
    console.log(`Unexpected disconnects during test: ${unexpectedDisconnects}`);
    console.log(`Total elapsed time: ${elapsed}s`);

    const passed =
      connected === TOTAL_CLIENTS &&
      failed === 0 &&
      unexpectedDisconnects === 0;

    console.log(
      passed
        ? 'PASS: All clients connected with no unexpected disconnects observed.'
        : 'FAIL: Review connection failures or unexpected disconnects.',
    );

    if (!passed) process.exitCode = 1;
  } finally {
    testFinished = true;
    for (const socket of clients) {
      socket.disconnect();
    }
  }
}

main().catch((error) => {
  console.error('Test failed:', error.message);
  process.exitCode = 1;
});

