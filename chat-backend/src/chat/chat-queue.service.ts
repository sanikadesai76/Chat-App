
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

    this.loopMonitor.enable();

    this.loopTimer = setInterval(() => {
      const loopP99 = this.loopMonitor.percentile(99) / 1e6;
      const loopMax = this.loopMonitor.max / 1e6;
      const rss = Math.round(process.memoryUsage().rss / 1048576);
      const s = snapshotAndResetChatStats();

      const avgQueue =
        s.inserts > 0 ? s.totalQueueWaitMs / s.inserts : 0;

      const avgDb =
        s.dbQueries > 0 ? s.totalDbInsertMs / s.dbQueries : 0;

      const avgSave =
        s.inserts > 0 ? s.totalSaveMs / s.inserts : 0;

      this.logger.log(
        `EVENT_LOOP p99=${loopP99.toFixed(1)}ms max=${loopMax.toFixed(1)}ms rss=${rss}MB` +
          ` | msgs=${s.inserts} batches=${s.batches} maxBatch=${s.maxBatchSize}` +
          ` | queue avg=${avgQueue.toFixed(2)}ms max=${s.maxQueueWaitMsActive.toFixed(2)}ms` +
          ` | db avg=${avgDb.toFixed(2)}ms max=${s.maxInsertMs.toFixed(2)}ms slow=${s.slowInserts}` +
          ` | save avg=${avgSave.toFixed(2)}ms max=${s.maxSaveMs.toFixed(2)}ms`,
      );

      this.loopMonitor.reset();
    }, 5000);
  }

  // Legacy BullMQ path: not used by the current gateway.
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

    if (t1 - t0 > chatStats.maxAddMs) {
      chatStats.maxAddMs = t1 - t0;
    }

    if (t2 - t1 > chatStats.maxWaitMs) {
      chatStats.maxWaitMs = t2 - t1;
    }
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
