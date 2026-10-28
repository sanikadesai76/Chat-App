
import { randomUUID } from 'node:crypto';
import { Logger } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import type { Server, Socket } from 'socket.io';
import { ChatDatabaseService } from './chat-database.service.js';

interface EventAck {
  success: boolean;
  error?: string;
  roomId?: string;
  clientId?: string;
  messageId?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const configuredOrigins = process.env.CORS_ORIGINS
  ?.split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

@WebSocketGateway({
  transports: ['websocket'],
  cors: {
    origin: configuredOrigins?.length
      ? configuredOrigins
      : process.env.NODE_ENV === 'production'
        ? []
        : '*',
  },
})
export class ChatGateway
  implements OnGatewayConnection, OnGatewayDisconnect
{
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(ChatGateway.name);

  constructor(private readonly database: ChatDatabaseService) {}

  handleConnection(client: Socket): void {
    this.logger.log(`Client connected: ${client.id}`);
  }

  handleDisconnect(client: Socket): void {
    this.logger.log(`Client disconnected: ${client.id}`);
  }

  @SubscribeMessage('joinRoom')
  async handleJoinRoom(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: unknown,
  ): Promise<EventAck> {
    if (!isRecord(payload) || typeof payload.roomId !== 'string') {
      return { success: false, error: 'Invalid room ID' };
    }

    const roomId = payload.roomId.trim();

    if (!roomId || roomId.length > 100) {
      return { success: false, error: 'Invalid room ID' };
    }

    await client.join(roomId);

    return { success: true, roomId, clientId: client.id };
  }

  @SubscribeMessage('sendMessage')
  async handleSendMessage(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: unknown,
  ): Promise<EventAck> {
    if (
      !isRecord(payload) ||
      typeof payload.roomId !== 'string' ||
      typeof payload.text !== 'string'
    ) {
      return { success: false, error: 'Invalid message payload' };
    }

    const roomId = payload.roomId.trim();
    const text = payload.text.trim();

    if (!roomId || roomId.length > 100) {
      return { success: false, error: 'Invalid room ID' };
    }

    if (!text || text.length > 4000) {
      return {
        success: false,
        error: 'Message must contain 1-4000 characters',
      };
    }

    if (!client.rooms.has(roomId)) {
      return {
        success: false,
        error: 'Join the room before sending messages',
      };
    }

    const message = {
      id: randomUUID(),
      roomId,
      senderId: client.id,
      text,
      timestamp: new Date().toISOString(),
    };

    try {
      // Persist the message before broadcasting it.
      await this.database.saveMessage(message);

      this.server.to(roomId).emit('newMessage', message);

      return { success: true, messageId: message.id };
    } catch (error) {
      this.logger.error(
        'Message persistence failed',
        error instanceof Error ? error.stack : String(error),
      );

      return {
        success: false,
        error: 'Message could not be saved. Please retry.',
      };
    }
  }
}
