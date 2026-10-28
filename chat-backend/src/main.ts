import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import { AppModule } from './app.module.js';
import { RedisHealthService } from './redis-health.service.js';
import { RedisIoAdapter } from './redis-io.adapter.js';

async function bootstrap(): Promise<void> {
  let app: INestApplication | undefined;
  let redisIoAdapter: RedisIoAdapter | undefined;
  let shuttingDown = false;

  try {
    app = await NestFactory.create(AppModule);

    const origins = process.env.CORS_ORIGINS
      ?.split(',')
      .map((origin) => origin.trim())
      .filter(Boolean);

    if (
      process.env.NODE_ENV === 'production' &&
      !origins?.length
    ) {
      throw new Error(
        'Set CORS_ORIGINS in production to your allowed frontend origins.',
      );
    }

    redisIoAdapter = new RedisIoAdapter(
      app,
      app.get(RedisHealthService),
    );

    await redisIoAdapter.connectToRedis();

    app.useWebSocketAdapter(redisIoAdapter);

    const port = Number(process.env.PORT ?? 3000);

    if (
      !Number.isInteger(port) ||
      port < 1 ||
      port > 65535
    ) {
      throw new Error(`Invalid PORT: ${process.env.PORT}`);
    }

    await app.listen(port);

    console.log(`Chat backend listening on port ${port}`);

    const gracefulShutdown = async (
      signal: string,
    ): Promise<void> => {
      if (shuttingDown) return;
      shuttingDown = true;

      console.log(`${signal} received. Shutting down gracefully...`);

      try {
        // Close HTTP and WebSocket servers first.
        await app?.close();
      } catch (error) {
        console.error('Error closing Nest application:', error);
      }

      try {
        // Then close Redis publisher and subscriber clients.
        await redisIoAdapter?.dispose();
      } catch (error) {
        console.error('Error disposing Redis adapter:', error);
        process.exitCode = 1;
      }
    };

    process.once('SIGINT', () => {
      void gracefulShutdown('SIGINT');
    });

    process.once('SIGTERM', () => {
      void gracefulShutdown('SIGTERM');
    });
  } catch (error) {
    console.error('Chat backend failed to start:', error);

    try {
      await app?.close();
    } catch (closeError) {
      console.error('Error closing application:', closeError);
    }

    try {
      await redisIoAdapter?.dispose();
    } catch (disposeError) {
      console.error('Error disposing Redis adapter:', disposeError);
    }

    process.exitCode = 1;
  }
}

void bootstrap();