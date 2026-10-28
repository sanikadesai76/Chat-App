
import {
  INestApplicationContext,
  Logger,
} from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import { createClient } from 'redis';
import type { Server, ServerOptions } from 'socket.io';
import { RedisHealthService } from './redis-health.service.js';

export class RedisIoAdapter extends IoAdapter {
  private readonly redisLogger = new Logger(RedisIoAdapter.name);

  private adapterConstructor?: ReturnType<typeof createAdapter>;

  private pubClient?: ReturnType<typeof createClient>;
  private subClient?: ReturnType<typeof createClient>;

  private lastErrorLogAt = 0;
  private disposed = false;

  constructor(
    app: INestApplicationContext,
    private readonly redisHealth: RedisHealthService,
  ) {
    super(app);
  }

  async connectToRedis(): Promise<void> {
    const redisUrl =
      process.env.REDIS_URL ?? 'redis://localhost:6379';

    const reconnectStrategy = (retries: number): number =>
      Math.min(250 * (retries + 1), 3000);

    this.pubClient = createClient({
      url: redisUrl,
      socket: { reconnectStrategy },
    });

    this.subClient = this.pubClient.duplicate();

    this.attachClientEvents(this.pubClient, 'publisher');
    this.attachClientEvents(this.subClient, 'subscriber');

    try {
      await Promise.all([
        this.pubClient.connect(),
        this.subClient.connect(),
      ]);

      this.updateReadiness();

      this.adapterConstructor = createAdapter(
        this.pubClient,
        this.subClient,
      );

      this.redisLogger.log(
        'Redis publisher and subscriber connected',
      );
    } catch (error) {
      this.redisHealth.setRedisReady(false);

      await Promise.allSettled([
        this.closeClient(this.pubClient),
        this.closeClient(this.subClient),
      ]);

      throw error;
    }
  }

  createIOServer(
    port: number,
    options?: ServerOptions,
  ): Server {
    if (!this.adapterConstructor) {
      throw new Error(
        'Redis adapter is not initialized. Connect to Redis before starting the server.',
      );
    }

    const server = super.createIOServer(
      port,
      options,
    ) as Server;

    server.adapter(this.adapterConstructor);

    return server;
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;

    this.disposed = true;
    this.redisHealth.setRedisReady(false);

    await super.dispose();

    await Promise.allSettled([
      this.closeClient(this.pubClient),
      this.closeClient(this.subClient),
    ]);

    this.redisLogger.log('Redis adapter disposed');
  }

  private attachClientEvents(
    client: ReturnType<typeof createClient>,
    role: 'publisher' | 'subscriber',
  ): void {
    client.on('ready', () => {
      this.updateReadiness();
      this.redisLogger.log(`Redis ${role} is ready`);
    });

    client.on('reconnecting', () => {
      this.redisHealth.setRedisReady(false);
      this.redisLogger.warn(`Redis ${role} reconnecting`);
    });

    client.on('end', () => {
      this.redisHealth.setRedisReady(false);
      this.redisLogger.warn(`Redis ${role} connection ended`);
    });

    client.on('error', (error) => {
      this.updateReadiness();

      const now = Date.now();

      // Limit repeated error logs during a Redis outage.
      if (now - this.lastErrorLogAt >= 10_000) {
        this.lastErrorLogAt = now;

        this.redisLogger.error(
          `Redis ${role} error: ${error.message}`,
        );
      }
    });
  }

  private updateReadiness(): void {
    this.redisHealth.setRedisReady(
      this.pubClient?.isReady === true &&
        this.subClient?.isReady === true,
    );
  }

  private async closeClient(
    client: ReturnType<typeof createClient> | undefined,
  ): Promise<void> {
    if (!client) return;

    if (client.isReady) {
      await client.quit();
    } else if (client.isOpen) {
      client.destroy();
    }
  }
}
