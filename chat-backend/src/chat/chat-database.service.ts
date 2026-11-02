
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

// SQLSTATE class 22 (data exception) or 23 (integrity violation).
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

    // Pre-open connections for the writer.
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
   */
  saveMessage(message: StoredMessage): Promise<void> {
    if (this.closing) {
      return Promise.reject(
        new Error('Database service is shutting down'),
      );
    }

    if (this.pending.length >= this.maxPending) {
      return Promise.reject(
        new Error('Persistence backlog is full'),
      );
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

  // Coalesce messages arriving in the same event-loop turn.
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
          this.logger.error(
            'Unexpected group-commit failure',
            error instanceof Error
              ? error.stack
              : String(error),
          );

          for (const w of batch) {
            w.reject(error);
          }
        })
        .finally(() => {
          this.activeFlushes--;
          this.pump();
        });
    }
  }

  private async writeBatch(batch: PendingWrite[]): Promise<void> {
    const batchStart = performance.now();

    let lastError: unknown;

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        await this.insertBatch(
          batch.map((w) => w.message),
        );

        this.recordBatch(batch, batchStart);

        for (const w of batch) {
          w.resolve();
        }

        return;
      } catch (error) {
        lastError = error;

        if (isDataError(error)) break;

        if (attempt < 2) {
          await sleep(100);
        }
      }
    }

    // Isolate invalid messages instead of failing the whole batch.
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
      lastError instanceof Error
        ? lastError.stack
        : String(lastError),
    );

    for (const w of batch) {
      w.reject(lastError);
    }
  }

  // Measures PostgreSQL query round-trip, including pool acquisition.
  private async insertBatch(
    messages: StoredMessage[],
  ): Promise<void> {
    const dbStart = performance.now();

    try {
      await this.pool.query(
        `INSERT INTO chat_messages
           (id, conversation_id, sender_id, message_text, created_at)
         SELECT * FROM unnest(
           $1::uuid[],
           $2::text[],
           $3::text[],
           $4::text[],
           $5::timestamptz[]
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
    } finally {
      const dbMs = performance.now() - dbStart;

      // Count every query attempt, including retries and failures.
      chatStats.dbQueries++;
      chatStats.totalDbInsertMs += dbMs;

      if (dbMs > chatStats.maxInsertMs) {
        chatStats.maxInsertMs = dbMs;
      }

      if (dbMs > 100) {
        chatStats.slowInserts++;
      }
    }
  }

  // Records successful batch persistence timings.
  private recordBatch(
    batch: PendingWrite[],
    batchStart: number,
  ): void {
    const completedAt = performance.now();

    chatStats.batches++;
    chatStats.inserts += batch.length;

    if (batch.length > chatStats.maxBatchSize) {
      chatStats.maxBatchSize = batch.length;
    }

    for (const w of batch) {
      // Time spent waiting before batch processing started.
      const queueWaitMs = batchStart - w.enqueuedAt;

      chatStats.totalQueueWaitMs += queueWaitMs;

      if (queueWaitMs > chatStats.maxQueueWaitMsActive) {
        chatStats.maxQueueWaitMsActive = queueWaitMs;
      }

      if (queueWaitMs > 100) {
        chatStats.slowQueueWaits++;
      }

      // Total time from enqueue until successful persistence.
      const saveMs = completedAt - w.enqueuedAt;

      chatStats.totalSaveMs += saveMs;

      if (saveMs > chatStats.maxSaveMs) {
        chatStats.maxSaveMs = saveMs;
      }
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.closing = true;

    // Allow pending and active writes to finish (bounded).
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
