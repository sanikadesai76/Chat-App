import { Controller, Get, Res } from '@nestjs/common';
import type { Response } from 'express';
import { RedisHealthService } from './redis-health.service.js';

@Controller('health')
export class HealthController {
  constructor(
    private readonly redisHealth: RedisHealthService,
  ) {}

  @Get('live')
  live() {
    return { status: 'ok' };
  }

  @Get('ready')
  ready(@Res({ passthrough: true }) response: Response) {
    const redisReady = this.redisHealth.isRedisReady();

    response.status(redisReady ? 200 : 503);

    return {
      status: redisReady ? 'ready' : 'not_ready',
      redis: this.redisHealth.getStatus(),
    };
  }
}