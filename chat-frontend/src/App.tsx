
import { useEffect, useRef, useState } from 'react'
import { io, type Socket } from 'socket.io-client'

type ChatMessage = {
  id: string
  roomId: string
  senderId: string
  text: string
  timestamp: string
}

const SOCKET_URL = 'http://localhost:3000'
const CONVERSATION_ID = 'general'

function App() {
  const [socket, setSocket] = useState<Socket | null>(null)
  const [connected, setConnected] = useState(false)
  const [joined, setJoined] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [messageInput, setMessageInput] = useState('')
  const [error, setError] = useState('')
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const client = io(SOCKET_URL, {
      transports: ['websocket'],
    })

    setSocket(client)

    client.on('connect', () => {
      setConnected(true)

      client.emit(
        'joinRoom',
        { roomId: CONVERSATION_ID },
        (response: { success: boolean; message?: string }) => {
          setJoined(Boolean(response?.success))

          if (!response?.success) {
            setError(response?.message ?? 'Could not connect to conversation.')
          } else {
            setError('')
          }
        },
      )
    })

    client.on('disconnect', () => {
      setConnected(false)
      setJoined(false)
    })

    client.on('connect_error', () => {
      setConnected(false)
      setError('Unable to connect to the chat server.')
    })

    client.on('newMessage', (message: ChatMessage) => {
      setMessages((previous) => [...previous, message])
    })

    return () => {
      client.disconnect()
    }
  }, [])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({
      behavior: 'smooth',
      block: 'end',
    })
  }, [messages])

  const sendMessage = () => {
    const text = messageInput.trim()

    if (!text || !socket?.connected || !joined) return

    socket.emit(
      'sendMessage',
      { roomId: CONVERSATION_ID, text },
      (response: { success: boolean; message?: string }) => {
        if (response?.success) {
          setMessageInput('')
          setError('')
        } else {
          setError(response?.message ?? 'Message could not be sent.')
        }
      },
    )
  }

  return (
    <main className="flex min-h-screen items-center justify-center bg-slate-100 p-3 sm:p-6">
      <section className="flex h-[92vh] w-full max-w-3xl flex-col overflow-hidden rounded-2xl bg-white shadow-xl">
        <header className="flex items-center gap-3 bg-blue-600 px-5 py-4 text-white">
          <div className="flex h-11 w-11 items-center justify-center rounded-full bg-white/20 text-lg font-bold">
            C
          </div>

          <div className="flex-1">
            <h1 className="font-semibold">Chat</h1>
            <p className="text-xs text-blue-100">
              {connected && joined ? 'Online' : 'Connecting...'}
            </p>
          </div>

          <span
            className={`h-2.5 w-2.5 rounded-full ${
              connected && joined ? 'bg-green-300' : 'bg-red-300'
            }`}
          />
        </header>

        <div className="flex flex-1 flex-col gap-3 overflow-y-auto bg-slate-50 p-4 sm:p-6">
          {messages.length === 0 && (
            <p className="m-auto text-center text-sm text-slate-400">
              {joined
                ? 'No messages yet. Send a message to start chatting.'
                : 'Connecting to the conversation...'}
            </p>
          )}

          {messages.map((message) => {
            const ownMessage = message.senderId === socket?.id

            return (
              <div
                key={message.id}
                className={`flex ${ownMessage ? 'justify-end' : 'justify-start'}`}
              >
                <div
                  className={`max-w-[85%] break-words rounded-2xl px-4 py-3 ${
                    ownMessage
                      ? 'rounded-br-sm bg-blue-600 text-white'
                      : 'rounded-bl-sm border border-slate-200 bg-white text-slate-800'
                  }`}
                >
                  <p className="whitespace-pre-wrap">{message.text}</p>

                  <p
                    className={`mt-1 text-right text-[11px] ${
                      ownMessage ? 'text-blue-100' : 'text-slate-400'
                    }`}
                  >
                    {new Date(message.timestamp).toLocaleTimeString([], {
                      hour: '2-digit',
                      minute: '2-digit',
                    })}
                  </p>
                </div>
              </div>
            )
          })}

          <div ref={messagesEndRef} />
        </div>

        {error && (
          <p role="alert" className="bg-red-50 px-4 py-2 text-sm text-red-600">
            {error}
          </p>
        )}

        <footer className="flex items-center gap-3 border-t border-slate-200 bg-white p-3 sm:p-4">
          <input
            value={messageInput}
            onChange={(event) => setMessageInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') sendMessage()
            }}
            placeholder="Type a message..."
            maxLength={4000}
            disabled={!connected || !joined}
            className="min-w-0 flex-1 rounded-full border border-slate-300 px-4 py-3 outline-none focus:border-blue-500 disabled:bg-slate-100"
          />

          <button
            onClick={sendMessage}
            disabled={!connected || !joined || !messageInput.trim()}
            aria-label="Send message"
            className="flex h-12 w-12 shrink-0 items-center justify-center rounded-full bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
          >
            <svg viewBox="0 0 24 24" fill="currentColor" className="h-5 w-5">
              <path d="M2.01 21 23 12 2.01 3 2 10l15 2-15 2z" />
            </svg>
          </button>
        </footer>
      </section>
    </main>
  )
}

export default App
