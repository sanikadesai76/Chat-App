
import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Pool } from 'pg';

export interface StoredMessage {
  id: string;
  roomId: string;
  senderId: string;
  text: string;
  timestamp: string;
}

@Injectable()
export class ChatDatabaseService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(ChatDatabaseService.name);

  private readonly pool = new Pool({
    host: process.env.DB_HOST ?? '127.0.0.1',
    port: Number(process.env.DB_PORT ?? 5433),
    database: process.env.DB_NAME ?? 'chatdb',
    user: process.env.DB_USER ?? 'chatuser',
    password: process.env.DB_PASSWORD ?? 'chat_dev_password_change_me',
    max: 30,
    connectionTimeoutMillis: 5000,
  });

  async onModuleInit(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        id UUID PRIMARY KEY,
        conversation_id VARCHAR(100) NOT NULL,
        sender_id VARCHAR(100) NOT NULL,
        message_text TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_chat_messages_conversation_time
      ON chat_messages (conversation_id, created_at)
    `);

    await this.pool.query('SELECT 1');

    this.logger.log(
      'PostgreSQL connected; chat_messages table is ready.',
    );
  }

  async saveMessage(message: StoredMessage): Promise<void> {
    await this.pool.query(
      `INSERT INTO chat_messages
       (id, conversation_id, sender_id, message_text, created_at)
       VALUES ($1, $2, $3, $4, $5)`,
      [
        message.id,
        message.roomId,
        message.senderId,
        message.text,
        message.timestamp,
      ],
    );
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
