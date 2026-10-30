import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue, QueueEvents } from 'bullmq';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { StoredMessage } from './chat-database.service.js';
import { chatStats, snapshotAndResetChatStats } from './chat-stats.js';

@Injectable()
export class ChatQueueService implements OnModuleInit, OnModuleDestroy {
  private queueEvents!: QueueEvents;
  private readonly logger = new Logger(ChatQueueService.name);
  private readonly loopMonitor = monitorEventLoopDelay({ resolution: 10 });
  private loopTimer?: NodeJS.Timeout;

  constructor(
    @InjectQueue('chat-persistence')
    private readonly queue: Queue<StoredMessage>,
  ) {}

  async onModuleInit(): Promise<void> {
    this.queueEvents = new QueueEvents('chat-persistence', {
      connection: {
        host: process.env.REDIS_HOST ?? '127.0.0.1',
        port: Number(process.env.REDIS_PORT ?? 6379),
      },
    });

    await this.queueEvents.waitUntilReady();

    // Diagnostic only: one summary line every 5 s (no per-event logging).
    this.loopMonitor.enable();
    this.loopTimer = setInterval(() => {
      const p99 = this.loopMonitor.percentile(99) / 1e6;
      const max = this.loopMonitor.max / 1e6;
      const rss = Math.round(process.memoryUsage().rss / 1048576);
      const s = snapshotAndResetChatStats();
      this.logger.log(
        `LOOP p99=${p99.toFixed(0)}ms max=${max.toFixed(0)}ms rss=${rss}MB` +
          ` | msgs=${s.inserts} batches=${s.batches}` +
          ` maxBatch=${s.maxBatchSize} slowInsert=${s.slowInserts}` +
          ` maxInsert=${s.maxInsertMs.toFixed(0)}ms` +
          ` maxSave=${s.maxSaveMs.toFixed(0)}ms`,
      );
      this.loopMonitor.reset();
    }, 5000);
  }

  // Legacy BullMQ path: no longer used by the gateway.
  async persistMessage(message: StoredMessage): Promise<void> {
    const t0 = performance.now();

    const job = await this.queue.add('save-message', message, {
      jobId: message.id,
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 250,
      },
      removeOnComplete: { age: 300, count: 50000 },
      removeOnFail: { age: 86400 },
    });

    const t1 = performance.now();

    await job.waitUntilFinished(this.queueEvents, 30_000);

    const t2 = performance.now();
    if (t1 - t0 > chatStats.maxAddMs) chatStats.maxAddMs = t1 - t0;
    if (t2 - t1 > chatStats.maxWaitMs) chatStats.maxWaitMs = t2 - t1;
  }

  async onModuleDestroy(): Promise<void> {
    if (this.loopTimer) {
      clearInterval(this.loopTimer);
    }
    this.loopMonitor.disable();

    if (this.queueEvents) {
      await this.queueEvents.close();
    }
  }
}