import { Module } from '@nestjs/common';
import { AppController } from './app.controller.js';
import { AppService } from './app.service.js';
import { HealthController } from './health.controller.js';
import { RedisHealthService } from './redis-health.service.js';
import { ChatModule } from './chat/chat.module.js';

@Module({
  imports: [ChatModule],
  controllers: [
    AppController,
    HealthController,
  ],
  providers: [
    AppService,
    RedisHealthService,
  ],
})
export class AppModule {}