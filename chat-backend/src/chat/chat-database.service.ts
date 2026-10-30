import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Pool } from 'pg';
import { chatStats } from './chat-stats.js';

export interface StoredMessage {
  id: string;
  roomId: string;
  senderId: string;
  text: string;
  timestamp: string;
}

interface PendingWrite {
  message: StoredMessage;
  enqueuedAt: number;
  resolve: () => void;
  reject: (error: unknown) => void;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// SQLSTATE class 22 (data exception) or 23 (integrity violation): the data
// itself is bad, so retrying the same batch cannot help.
function isDataError(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === 'string' && /^(22|23)/.test(code);
}

@Injectable()
export class ChatDatabaseService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(ChatDatabaseService.name);

  private readonly pool = new Pool({
    host: process.env.DB_HOST ?? '127.0.0.1',
    port: Number(process.env.DB_PORT ?? 5433),
    database: process.env.DB_NAME ?? 'chatdb',
    user: process.env.DB_USER ?? 'chatuser',
    password: process.env.DB_PASSWORD ?? 'chat_dev_password_change_me',
    max: 30,
    connectionTimeoutMillis: 5000,
  });

  // Group-commit writer settings.
  private readonly maxBatchSize = 500;
  private readonly maxParallelFlushes = 4;
  private readonly maxPending = 20_000;

  private pending: PendingWrite[] = [];
  private activeFlushes = 0;
  private pumpScheduled = false;
  private closing = false;

  async onModuleInit(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        id UUID PRIMARY KEY,
        conversation_id VARCHAR(100) NOT NULL,
        sender_id VARCHAR(100) NOT NULL,
        message_text TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_chat_messages_conversation_time
      ON chat_messages (conversation_id, created_at)
    `);

    // Pre-open the connections the writer will use so the first burst of
    // messages does not pay connection setup.
    await Promise.all(
      Array.from({ length: this.maxParallelFlushes }, () =>
        this.pool.query('SELECT 1'),
      ),
    );

    this.logger.log(
      'PostgreSQL connected; chat_messages table is ready.',
    );
  }

  /**
   * Resolves only after the message has been committed to PostgreSQL.
   * Rejects if it could not be saved.
   */
  saveMessage(message: StoredMessage): Promise<void> {
    if (this.closing) {
      return Promise.reject(new Error('Database service is shutting down'));
    }
    if (this.pending.length >= this.maxPending) {
      return Promise.reject(new Error('Persistence backlog is full'));
    }

    return new Promise<void>((resolve, reject) => {
      this.pending.push({
        message,
        enqueuedAt: performance.now(),
        resolve,
        reject,
      });
      this.schedulePump();
    });
  }

  // Coalesce messages that arrive in the same event-loop turn into one batch.
  private schedulePump(): void {
    if (this.pumpScheduled) return;
    this.pumpScheduled = true;
    setImmediate(() => {
      this.pumpScheduled = false;
      this.pump();
    });
  }

  private pump(): void {
    while (
      this.activeFlushes < this.maxParallelFlushes &&
      this.pending.length > 0
    ) {
      const batch = this.pending.splice(0, this.maxBatchSize);
      this.activeFlushes++;

      void this.writeBatch(batch)
        .catch((error: unknown) => {
          // writeBatch settles every waiter itself; this is a last-resort guard.
          this.logger.error(
            'Unexpected group-commit failure',
            error instanceof Error ? error.stack : String(error),
          );
          for (const w of batch) w.reject(error);
        })
        .finally(() => {
          this.activeFlushes--;
          this.pump();
        });
    }
  }

  private async writeBatch(batch: PendingWrite[]): Promise<void> {
    const start = performance.now();
    let lastError: unknown;

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        await this.insertBatch(batch.map((w) => w.message));
        this.recordBatch(batch, start);
        for (const w of batch) w.resolve();
        return;
      } catch (error) {
        lastError = error;
        if (isDataError(error)) break;
        if (attempt < 2) await sleep(100);
      }
    }

    // Every attempt failed. If the data is at fault, isolate the offending
    // message(s) so one bad message cannot fail its neighbours.
    if (batch.length > 1 && isDataError(lastError)) {
      const mid = Math.ceil(batch.length / 2);
      await Promise.all([
        this.writeBatch(batch.slice(0, mid)),
        this.writeBatch(batch.slice(mid)),
      ]);
      return;
    }

    this.logger.error(
      `Batch of ${batch.length} message(s) could not be saved`,
      lastError instanceof Error ? lastError.stack : String(lastError),
    );
    for (const w of batch) w.reject(lastError);
  }

  private async insertBatch(messages: StoredMessage[]): Promise<void> {
    await this.pool.query(
      `INSERT INTO chat_messages
         (id, conversation_id, sender_id, message_text, created_at)
       SELECT * FROM unnest(
         $1::uuid[], $2::text[], $3::text[], $4::text[], $5::timestamptz[]
       )
       WHERE true
       ON CONFLICT (id) DO NOTHING`,
      [
        messages.map((m) => m.id),
        messages.map((m) => m.roomId),
        messages.map((m) => m.senderId),
        messages.map((m) => m.text),
        messages.map((m) => m.timestamp),
      ],
    );
  }

  private recordBatch(batch: PendingWrite[], start: number): void {
    const now = performance.now();
    const insertMs = now - start;

    chatStats.batches++;
    chatStats.inserts += batch.length;
    if (batch.length > chatStats.maxBatchSize) {
      chatStats.maxBatchSize = batch.length;
    }
    if (insertMs > 100) chatStats.slowInserts++;
    if (insertMs > chatStats.maxInsertMs) chatStats.maxInsertMs = insertMs;

    for (const w of batch) {
      const saveMs = now - w.enqueuedAt;
      if (saveMs > chatStats.maxSaveMs) chatStats.maxSaveMs = saveMs;
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.closing = true;

    // Let in-flight and pending writes finish (bounded) before closing.
    const deadline = Date.now() + 10_000;
    while (
      (this.pending.length > 0 || this.activeFlushes > 0) &&
      Date.now() < deadline
    ) {
      this.pump();
      await sleep(25);
    }

    await this.pool.end();
  }
}