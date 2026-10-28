import { Injectable } from '@nestjs/common';

@Injectable()
export class RedisHealthService {
  private redisReady = false;

  setRedisReady(ready: boolean): void {
    this.redisReady = ready;
  }

  isRedisReady(): boolean {
    return this.redisReady;
  }

  getStatus(): 'ready' | 'not_ready' {
    return this.redisReady ? 'ready' : 'not_ready';
  }
}