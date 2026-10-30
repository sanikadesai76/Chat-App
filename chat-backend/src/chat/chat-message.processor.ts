import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { ChatDatabaseService, StoredMessage } from './chat-database.service.js';
import { chatStats } from './chat-stats.js';

@Processor('chat-persistence', { concurrency: 20 })
export class ChatMessageProcessor extends WorkerHost {
  constructor(private readonly database: ChatDatabaseService) {
    super();
  }

  async process(job: Job<StoredMessage>): Promise<{ messageId: string }> {
    if (job.name !== 'save-message') {
      throw new Error(`Unknown job type: ${job.name}`);
    }

    // Time from queue.add() until a worker picked the job up.
    const queueWaitMs = Date.now() - job.timestamp;
    chatStats.jobs++;
    if (queueWaitMs > 100) chatStats.slowQueueWaits++;
    if (queueWaitMs > chatStats.maxQueueWaitMs) {
      chatStats.maxQueueWaitMs = queueWaitMs;
    }

    await this.database.saveMessage(job.data);

    return { messageId: job.data.id };
  }
}