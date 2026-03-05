// Flop<T> client class — typed endpoint calls with auto-batched views.

import { type TokenStore, LocalStorageTokenStore, MemoryTokenStore } from "./token_store.ts";
import { AuthClient } from "./auth_client.ts";

export interface FlopClientConfig {
  host: string;
  tokenStore?: TokenStore;
  batchViews?: "frame" | "none";
  autoRefetch?: boolean;
}

export class FlopRequestError extends Error {
  readonly status: number;
  readonly payload: unknown;

  constructor(status: number, message: string, payload: unknown) {
    super(message);
    this.name = "FlopRequestError";
    this.status = status;
    this.payload = payload;
  }
}

type ViewDef = { input: any; output: any };
type ReducerDef = { input: any; output: any };

type ViewInput<V, K extends keyof V> = V[K] extends ViewDef ? V[K]["input"] : never;
type ViewOutput<V, K extends keyof V> = V[K] extends ViewDef ? V[K]["output"] : unknown;
type ReducerInput<R, K extends keyof R> = R[K] extends ReducerDef ? R[K]["input"] : never;
type ReducerOutput<R, K extends keyof R> = R[K] extends ReducerDef ? R[K]["output"] : unknown;

interface BatchCall {
  id: string;
  name: string;
  params: unknown;
}

interface BatchResult {
  id: string;
  data?: unknown;
  reads?: string[];
  error?: string;
}

interface PendingViewCall {
  id: string;
  name: string;
  params: unknown;
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  watchId?: number;
}

interface WatchEntry {
  id: number;
  name: string;
  params: unknown;
  reads: Set<string>;
  onData: (value: unknown) => void;
  onError?: (error: Error) => void;
}

export class Flop<T extends { reducers: Record<string, any>; views: Record<string, any> }> {
  private readonly host: string;
  private readonly tokenStore: TokenStore;
  private readonly refreshTokenStore: TokenStore;
  private readonly batchViews: "frame" | "none";
  private readonly autoRefetch: boolean;

  private nextBatchCallID = 1;
  private nextWatchID = 1;
  private pendingViewCalls: PendingViewCall[] = [];
  private batchFlushScheduled = false;
  private readonly watches = new Map<number, WatchEntry>();
  private readonly dirtyWatches = new Set<number>();
  private watchFlushScheduled = false;

  readonly users: AuthClient;

  constructor(config: FlopClientConfig) {
    this.host = config.host.replace(/\/$/, "");
    this.batchViews = config.batchViews ?? "frame";
    this.autoRefetch = config.autoRefetch ?? true;

    if (config.tokenStore) {
      this.tokenStore = config.tokenStore;
    } else if (typeof localStorage !== "undefined") {
      this.tokenStore = new LocalStorageTokenStore("flop_token");
    } else {
      this.tokenStore = new MemoryTokenStore();
    }

    this.refreshTokenStore = typeof localStorage !== "undefined"
      ? new LocalStorageTokenStore("flop_refresh_token")
      : new MemoryTokenStore();

    this.users = new AuthClient(this.host, this.tokenStore, this.refreshTokenStore);
  }

  view<K extends keyof T["views"]>(name: K, params: ViewInput<T["views"], K>): Promise<ViewOutput<T["views"], K>> {
    return this.enqueueViewCall(String(name), params) as Promise<ViewOutput<T["views"], K>>;
  }

  async reducer<K extends keyof T["reducers"]>(
    name: K,
    params: ReducerInput<T["reducers"], K>,
  ): Promise<ReducerOutput<T["reducers"], K>> {
    const { data, headers } = await this.requestJSON("POST", `/api/reduce/${String(name)}`, params);
    const writes = parseCSVHeader(headers.get("X-Flop-Writes"));
    if (this.autoRefetch && writes.length > 0) {
      this.invalidateViewsByTouchedTables(writes);
    }
    return data as ReducerOutput<T["reducers"], K>;
  }

  watch<K extends keyof T["views"]>(
    name: K,
    params: ViewInput<T["views"], K>,
    onData: (value: ViewOutput<T["views"], K>) => void,
    onError?: (error: Error) => void,
  ): () => void {
    const watchID = this.nextWatchID++;
    const entry: WatchEntry = {
      id: watchID,
      name: String(name),
      params,
      reads: new Set<string>(),
      onData: onData as (value: unknown) => void,
      onError,
    };
    this.watches.set(watchID, entry);
    this.enqueueViewCall(entry.name, entry.params, watchID).then(
      (value) => entry.onData(value),
      (err) => entry.onError?.(toError(err)),
    );
    return () => {
      this.watches.delete(watchID);
      this.dirtyWatches.delete(watchID);
    };
  }

  private enqueueViewCall(name: string, params: unknown, watchId?: number): Promise<unknown> {
    if (this.batchViews === "none") {
      return this.runSingleView(name, params, watchId);
    }
    return new Promise((resolve, reject) => {
      this.pendingViewCalls.push({
        id: String(this.nextBatchCallID++),
        name,
        params,
        resolve,
        reject,
        watchId,
      });
      this.scheduleBatchFlush();
    });
  }

  private scheduleBatchFlush(): void {
    if (this.batchFlushScheduled) return;
    this.batchFlushScheduled = true;
    const flush = () => {
      this.batchFlushScheduled = false;
      void this.flushViewBatch();
    };
    const raf = getAnimationFrame();
    if (raf) {
      raf(flush);
      return;
    }
    setTimeout(flush, 0);
  }

  private async flushViewBatch(): Promise<void> {
    if (this.pendingViewCalls.length === 0) return;
    const calls = this.pendingViewCalls.splice(0, this.pendingViewCalls.length);

    // Avoid batch endpoint overhead when a flush contains a single view call.
    if (calls.length === 1) {
      const call = calls[0];
      try {
        const value = await this.runSingleView(call.name, call.params, call.watchId);
        call.resolve(value);
      } catch (err) {
        call.reject(err);
      }
      return;
    }

    const payload = {
      calls: calls.map((c): BatchCall => ({ id: c.id, name: c.name, params: c.params ?? {} })),
    };

    try {
      const { data } = await this.requestJSON("POST", "/api/view/_batch", payload);
      const results = asBatchResults(data);
      const byID = new Map(results.map((r) => [r.id, r]));
      for (const call of calls) {
        const result = byID.get(call.id);
        if (!result) {
          call.reject(new Error(`Missing batch result for call ${call.id}`));
          continue;
        }
        if (result.error) {
          call.reject(new Error(result.error));
          continue;
        }
        if (call.watchId !== undefined) {
          const watch = this.watches.get(call.watchId);
          if (watch) {
            watch.reads = new Set(result.reads ?? []);
          }
        }
        call.resolve(result.data);
      }
    } catch (err) {
      for (const call of calls) call.reject(err);
    }
  }

  private async runSingleView(name: string, params: unknown, watchId?: number): Promise<unknown> {
    const query = new URLSearchParams();
    if (params && typeof params === "object") {
      for (const [key, value] of Object.entries(params as Record<string, unknown>)) {
        if (value !== undefined && value !== null) query.set(key, String(value));
      }
    }
    const path = `/api/view/${name}${query.toString() ? `?${query}` : ""}`;
    const { data, headers } = await this.requestJSON("GET", path);
    if (watchId !== undefined) {
      const watch = this.watches.get(watchId);
      if (watch) {
        watch.reads = new Set(parseCSVHeader(headers.get("X-Flop-Reads")));
      }
    }
    return data;
  }

  private invalidateViewsByTouchedTables(touchedTables: string[]): void {
    const touched = new Set(touchedTables);
    for (const [watchID, watch] of this.watches.entries()) {
      for (const table of watch.reads) {
        if (touched.has(table)) {
          this.dirtyWatches.add(watchID);
          break;
        }
      }
    }
    this.scheduleWatchFlush();
  }

  private scheduleWatchFlush(): void {
    if (this.watchFlushScheduled) return;
    this.watchFlushScheduled = true;
    const flush = () => {
      this.watchFlushScheduled = false;
      void this.flushDirtyWatches();
    };
    const raf = getAnimationFrame();
    if (raf) {
      raf(flush);
      return;
    }
    setTimeout(flush, 0);
  }

  private async flushDirtyWatches(): Promise<void> {
    if (this.dirtyWatches.size === 0) return;
    const ids = [...this.dirtyWatches];
    this.dirtyWatches.clear();
    await Promise.all(ids.map(async (watchID) => {
      const watch = this.watches.get(watchID);
      if (!watch) return;
      try {
        const value = await this.enqueueViewCall(watch.name, watch.params, watchID);
        watch.onData(value);
      } catch (err) {
        watch.onError?.(toError(err));
      }
    }));
  }

  private async requestJSON(
    method: "GET" | "POST",
    path: string,
    body?: unknown,
  ): Promise<{ data: unknown; headers: Headers }> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    const token = this.tokenStore.get();
    if (token) headers.Authorization = `Bearer ${token}`;

    const execute = async (): Promise<Response> => {
      return fetch(`${this.host}${path}`, {
        method,
        headers,
        body: method === "POST" ? JSON.stringify(body ?? {}) : undefined,
      });
    };

    let res = await execute();
    if (res.status === 401) {
      try {
        await this.users.refresh();
        const nextToken = this.tokenStore.get();
        if (nextToken) headers.Authorization = `Bearer ${nextToken}`;
        res = await execute();
      } catch {
        // Keep original 401 response and surface it below.
      }
    }

    const payload = await safeJSON(res);
    if (!res.ok) {
      throw new FlopRequestError(res.status, String(payload?.error ?? `Request failed: ${res.status}`), payload);
    }
    return { data: payload?.data ?? payload, headers: res.headers };
  }
}

function parseCSVHeader(value: string | null): string[] {
  if (!value) return [];
  return value.split(",").map((x) => x.trim()).filter(Boolean);
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

async function safeJSON(res: Response): Promise<any> {
  try {
    return await res.json();
  } catch {
    return {};
  }
}

function asBatchResults(data: unknown): BatchResult[] {
  if (!data || typeof data !== "object") return [];
  const raw = (data as { results?: unknown }).results;
  if (!Array.isArray(raw)) return [];
  const out: BatchResult[] = [];
  for (const item of raw) {
    if (!item || typeof item !== "object") continue;
    const r = item as Record<string, unknown>;
    const id = typeof r.id === "string" ? r.id : "";
    if (!id) continue;
    out.push({
      id,
      data: r.data,
      reads: Array.isArray(r.reads) ? r.reads.filter((x): x is string => typeof x === "string") : undefined,
      error: typeof r.error === "string" ? r.error : undefined,
    });
  }
  return out;
}

function getAnimationFrame(): ((cb: (time: number) => void) => number) | null {
  const g = globalThis as unknown as {
    requestAnimationFrame?: (cb: (time: number) => void) => number;
  };
  if (typeof g.requestAnimationFrame === "function") {
    return g.requestAnimationFrame.bind(g);
  }
  return null;
}
