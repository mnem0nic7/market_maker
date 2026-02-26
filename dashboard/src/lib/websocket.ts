import type { ServerMessage, ClientMessage } from "./types";

export type ConnectionStatus = "connected" | "reconnecting" | "disconnected";
export type MessageHandler = (msg: ServerMessage) => void;
export type StatusHandler = (status: ConnectionStatus) => void;

const BASE_DELAY = 1000;
const MAX_DELAY = 30000;
const MAX_ATTEMPTS = 30;

export class WebSocketManager {
  private ws: WebSocket | null = null;
  private url: string;
  private onMessage: MessageHandler;
  private onStatus: StatusHandler;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private intentionalClose = false;

  constructor(url: string, onMessage: MessageHandler, onStatus: StatusHandler) {
    this.url = url;
    this.onMessage = onMessage;
    this.onStatus = onStatus;
  }

  connect() {
    this.intentionalClose = false;
    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      this.reconnectAttempts = 0;
      this.onStatus("connected");
      this.send({ action: "subscribe", channel: "all" });
    };

    this.ws.onmessage = (event) => {
      try {
        const msg: ServerMessage = JSON.parse(event.data);
        this.onMessage(msg);
      } catch {
        console.warn("Failed to parse WS message:", event.data);
      }
    };

    this.ws.onclose = () => {
      if (!this.intentionalClose) {
        this.onStatus("reconnecting");
        this.scheduleReconnect();
      } else {
        this.onStatus("disconnected");
      }
    };

    this.ws.onerror = (event) => {
      console.error("WebSocket error on", this.url, event);
      // onclose will fire after onerror, so status update happens there
    };
  }

  disconnect() {
    this.intentionalClose = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.ws?.close();
    this.ws = null;
    this.reconnectAttempts = 0;
  }

  send(msg: ClientMessage) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    }
  }

  private getReconnectDelay(): number {
    const exponentialDelay = Math.min(BASE_DELAY * Math.pow(2, this.reconnectAttempts), MAX_DELAY);
    const jitter = exponentialDelay * (0.9 + Math.random() * 0.2); // +/-10%
    return Math.floor(jitter);
  }

  private scheduleReconnect() {
    if (this.reconnectAttempts >= MAX_ATTEMPTS) {
      console.error(`WebSocket: giving up after ${MAX_ATTEMPTS} reconnect attempts`);
      this.onStatus("disconnected");
      return;
    }
    const delay = this.getReconnectDelay();
    this.reconnectAttempts++;
    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }
}
