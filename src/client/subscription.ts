// SSE subscription with auto-reconnect and dual API (callback + async iterator)

export type SubscriptionListener<T> = (data: T) => void;
export type ErrorListener = (error: Error) => void;

export class Subscription<T> {
  data: T | null = null;
  private url: string;
  private eventSource: EventSource | null = null;
  private dataListeners = new Set<SubscriptionListener<T>>();
  private errorListeners = new Set<ErrorListener>();
  private reconnectDelay = 1000;
  private maxReconnectDelay = 30000;
  private closed = false;
  private iteratorQueue: { resolve: (value: IteratorResult<T>) => void }[] = [];

  constructor(url: string) {
    this.url = url;
    this.connect();
  }

  on(event: "data", cb: SubscriptionListener<T>): this;
  on(event: "error", cb: ErrorListener): this;
  on(event: string, cb: any): this {
    if (event === "data") {
      this.dataListeners.add(cb);
      // Send current data if available
      if (this.data !== null) {
        cb(this.data);
      }
    } else if (event === "error") {
      this.errorListeners.add(cb);
    }
    return this;
  }

  off(event: "data", cb: SubscriptionListener<T>): this;
  off(event: "error", cb: ErrorListener): this;
  off(event: string, cb: any): this {
    if (event === "data") this.dataListeners.delete(cb);
    else if (event === "error") this.errorListeners.delete(cb);
    return this;
  }

  close(): void {
    this.closed = true;
    this.eventSource?.close();
    this.eventSource = null;

    // Resolve any pending iterator
    for (const item of this.iteratorQueue) {
      item.resolve({ value: undefined as any, done: true });
    }
    this.iteratorQueue = [];
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.closed) {
          return Promise.resolve({ value: undefined as any, done: true });
        }

        return new Promise((resolve) => {
          this.iteratorQueue.push({ resolve });
        });
      },
      return: (): Promise<IteratorResult<T>> => {
        this.close();
        return Promise.resolve({ value: undefined as any, done: true });
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }

  private connect(): void {
    if (this.closed) return;

    this.eventSource = new EventSource(this.url);

    this.eventSource.onmessage = (event) => {
      try {
        const parsed = JSON.parse(event.data) as T;
        this.data = parsed;
        this.reconnectDelay = 1000; // Reset on success

        for (const listener of this.dataListeners) {
          try {
            listener(parsed);
          } catch { /* don't propagate */ }
        }

        // Resolve pending iterators
        if (this.iteratorQueue.length > 0) {
          const item = this.iteratorQueue.shift()!;
          item.resolve({ value: parsed, done: false });
        }
      } catch (err) {
        this.emitError(err instanceof Error ? err : new Error("Parse error"));
      }
    };

    this.eventSource.addEventListener("error", () => {
      if (this.closed) return;

      this.emitError(new Error("SSE connection error"));
      this.eventSource?.close();

      // Reconnect with exponential backoff
      setTimeout(() => {
        this.reconnectDelay = Math.min(
          this.reconnectDelay * 2,
          this.maxReconnectDelay,
        );
        this.connect();
      }, this.reconnectDelay);
    });
  }

  private emitError(error: Error): void {
    for (const listener of this.errorListeners) {
      try {
        listener(error);
      } catch { /* don't propagate */ }
    }
  }
}
