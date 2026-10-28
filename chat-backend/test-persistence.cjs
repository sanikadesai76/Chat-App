const { io } = require('socket.io-client');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');

const URL = 'http://localhost:3000';
const roomId = `test-${randomUUID()}`;
const sender = io(URL, { transports: ['websocket'], reconnection: false });
const receiver = io(URL, { transports: ['websocket'], reconnection: false });

const timeout = setTimeout(() => {
  console.error('FAIL: Test timed out');
  sender.disconnect();
  receiver.disconnect();
  process.exit(1);
}, 15000);

function emitAck(socket, event, payload) {
  return new Promise((resolve, reject) => {
    socket.emit(event, payload, (ack) => {
      if (ack?.success) resolve(ack);
      else reject(new Error(ack?.error || `No successful acknowledgement for ${event}`));
    });
  });
}

async function main() {
  try {
    await Promise.all([
      new Promise((resolve, reject) => {
        sender.once('connect', resolve);
        sender.once('connect_error', reject);
      }),
      new Promise((resolve, reject) => {
        receiver.once('connect', resolve);
        receiver.once('connect_error', reject);
      }),
    ]);

    await Promise.all([
      emitAck(sender, 'joinRoom', { roomId }),
      emitAck(receiver, 'joinRoom', { roomId }),
    ]);

    const received = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Receiver did not receive message')), 5000);
      receiver.once('newMessage', (message) => {
        clearTimeout(timer);
        resolve(message);
      });
    });

    const text = `PostgreSQL verification ${randomUUID()}`;
    const ack = await emitAck(sender, 'sendMessage', { roomId, text });
    const message = await received;

    if (message.id !== ack.messageId || message.text !== text) {
      throw new Error('Delivered message does not match acknowledged message');
    }

    const rows = execFileSync('docker', [
      'exec', 'chat-postgres', 'psql', '-U', 'chatuser', '-d', 'chatdb',
      '-t', '-A', '-c',
      `SELECT COUNT(*) FROM chat_messages WHERE id = '${ack.messageId}' AND conversation_id = '${roomId}' AND message_text = '${text}'`
    ], { encoding: 'utf8' }).trim();

    if (rows !== '1') throw new Error(`Expected one saved message, found ${rows}`);

    console.log('PASS: Both clients connected.');
    console.log('PASS: Receiver received the message.');
    console.log('PASS: Message was saved in PostgreSQL.');
    console.log(`Message ID: ${ack.messageId}`);
  } catch (error) {
    console.error('FAIL:', error.message);
    process.exitCode = 1;
  } finally {
    clearTimeout(timeout);
    sender.disconnect();
    receiver.disconnect();
  }
}

main();
