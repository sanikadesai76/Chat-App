
import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ChatGateway } from './chat.gateway.js';
import { ChatDatabaseService } from './chat-database.service.js';
import { ChatMessageProcessor } from './chat-message.processor.js';
import { ChatQueueService } from './chat-queue.service.js';

@Module({
  imports: [
    BullModule.forRoot({
      connection: {
        host: process.env.REDIS_HOST ?? '127.0.0.1',
        port: Number(process.env.REDIS_PORT ?? 6379),
      },
    }),
    BullModule.registerQueue({
      name: 'chat-persistence',
    }),
  ],
  providers: [
    ChatGateway,
    ChatDatabaseService,
    ChatMessageProcessor,
    ChatQueueService,
  ],
})
export class ChatModule {}
