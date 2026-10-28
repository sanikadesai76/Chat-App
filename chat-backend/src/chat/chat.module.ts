import { Module } from '@nestjs/common';
import { ChatGateway } from './chat.gateway.js';
import { ChatDatabaseService } from './chat-database.service.js';

@Module({
  providers: [ChatGateway, ChatDatabaseService],
})
export class ChatModule {}
