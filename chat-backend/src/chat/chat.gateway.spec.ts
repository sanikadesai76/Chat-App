import {
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import type { Server, Socket } from 'socket.io';
import { ChatGateway } from './chat.gateway.js';

describe('ChatGateway', () => {
  let gateway: ChatGateway;

  beforeEach(() => {
    gateway = new ChatGateway();
  });

  it('should be defined', () => {
    expect(gateway).toBeDefined();
  });

  it('rejects malformed join payloads', async () => {
    const client = {
      id: 'client-1',
      rooms: new Set(['client-1']),
      join: vi.fn(),
    } as unknown as Socket;

    const result = await gateway.handleJoinRoom(
      client,
      { roomId: 123 },
    );

    expect(result).toEqual({
      success: false,
      error: 'Invalid room ID',
    });

    expect(client.join).not.toHaveBeenCalled();
  });

  it('joins a valid normalized room', async () => {
    const rooms = new Set(['client-1']);

    const client = {
      id: 'client-1',
      rooms,
      join: vi.fn(async (roomId: string) => {
        rooms.add(roomId);
      }),
    } as unknown as Socket;

    const result = await gateway.handleJoinRoom(
      client,
      { roomId: ' room-1 ' },
    );

    expect(result).toMatchObject({
      success: true,
      roomId: 'room-1',
      clientId: 'client-1',
    });

    expect(client.join).toHaveBeenCalledWith('room-1');
  });

  it('rejects messages from clients outside the room', () => {
    const client = {
      id: 'client-1',
      rooms: new Set(['client-1']),
    } as unknown as Socket;

    const result = gateway.handleSendMessage(
      client,
      {
        roomId: 'room-1',
        text: 'hello',
      },
    );

    expect(result).toEqual({
      success: false,
      error: 'Join the room before sending messages',
    });
  });

  it('rejects malformed message payloads', () => {
    const client = {
      id: 'client-1',
      rooms: new Set(['client-1', 'room-1']),
    } as unknown as Socket;

    expect(
      gateway.handleSendMessage(client, {
        roomId: 'room-1',
        text: 42,
      }),
    ).toEqual({
      success: false,
      error: 'Invalid message payload',
    });
  });

  it('broadcasts a valid message and returns its ID', () => {
    const emit = vi.fn();
    const to = vi.fn().mockReturnValue({ emit });

    gateway.server = { to } as unknown as Server;

    const client = {
      id: 'client-1',
      rooms: new Set(['client-1', 'room-1']),
    } as unknown as Socket;

    const result = gateway.handleSendMessage(
      client,
      {
        roomId: 'room-1',
        text: ' hello ',
      },
    );

    expect(result.success).toBe(true);
    expect(result.messageId).toBeTruthy();

    expect(to).toHaveBeenCalledWith('room-1');

    expect(emit).toHaveBeenCalledWith(
      'newMessage',
      expect.objectContaining({
        id: result.messageId,
        roomId: 'room-1',
        senderId: 'client-1',
        text: 'hello',
      }),
    );
  });
});