// In-process pub/sub for table change events

export interface ChangeEvent {
  table: string;
  op: "insert" | "update" | "delete";
  rowId: string;
  data?: Record<string, unknown>;
}

export type ChangeListener = (event: ChangeEvent) => void;

export class PubSub {
  private listeners = new Map<string, Set<ChangeListener>>();
  private globalListeners = new Set<ChangeListener>();

  subscribe(tables: string[], callback: ChangeListener): () => void {
    for (const table of tables) {
      if (!this.listeners.has(table)) {
        this.listeners.set(table, new Set());
      }
      this.listeners.get(table)!.add(callback);
    }

    return () => {
      for (const table of tables) {
        this.listeners.get(table)?.delete(callback);
      }
    };
  }

  subscribeAll(callback: ChangeListener): () => void {
    this.globalListeners.add(callback);
    return () => {
      this.globalListeners.delete(callback);
    };
  }

  publish(event: ChangeEvent): void {
    const listeners = this.listeners.get(event.table);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(event);
        } catch {
          // Don't let listener errors propagate
        }
      }
    }

    for (const listener of this.globalListeners) {
      try {
        listener(event);
      } catch {
        // Don't let listener errors propagate
      }
    }
  }

  listenerCount(table?: string): number {
    if (table) {
      return this.listeners.get(table)?.size ?? 0;
    }
    let total = this.globalListeners.size;
    for (const set of this.listeners.values()) {
      total += set.size;
    }
    return total;
  }
}
