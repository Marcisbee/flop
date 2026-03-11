/**
 * Flop Go2 TypeScript Client SDK
 *
 * Features:
 * - Type-safe view and reducer calls
 * - Automatic view batching via requestAnimationFrame
 * - SSE real-time subscriptions
 * - Auth with auto token refresh
 * - Auto-refetch views after reducer writes
 */

// ---- Types ----

export interface FlopConfig {
  host: string;
  batchViews?: "frame" | "none";
  autoRefetch?: boolean;
  realtime?: "sse" | "none";
  getToken?: () => string | null;
  setToken?: (token: string | null) => void;
  getRefreshToken?: () => string | null;
  setRefreshToken?: (token: string | null) => void;
}

export interface ViewResult<T = any> {
  data: T[];
  total: number;
}

export interface BatchCall {
  id: string;
  name: string;
  params?: Record<string, any>;
}

export interface BatchResult {
  id: string;
  data?: any;
  total?: number;
  error?: string;
}

interface PendingCall {
  id: string;
  name: string;
  params: Record<string, any>;
  resolve: (value: any) => void;
  reject: (error: any) => void;
}

interface WatchEntry {
  id: string;
  name: string;
  params: Record<string, any>;
  onData: (data: ViewResult) => void;
  onError?: (error: Error) => void;
  table?: string;
  realtime: boolean;
}

// ---- Client ----

let callId = 0;
function nextId(): string {
  return String(++callId);
}

export class Flop {
  private config: Required<FlopConfig>;
  private pendingCalls: PendingCall[] = [];
  private flushScheduled = false;
  private watches = new Map<string, WatchEntry>();
  private dirtyWatches = new Set<string>();
  private watchFlushScheduled = false;
  private eventSource: EventSource | null = null;
  private sseReconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private viewTableMap = new Map<string, string>(); // viewName -> tableName
  private refreshPromise: Promise<string | null> | null = null;

  constructor(config: FlopConfig) {
    this.config = {
      host: config.host.replace(/\/$/, ""),
      batchViews: config.batchViews ?? "frame",
      autoRefetch: config.autoRefetch ?? true,
      realtime: config.realtime ?? "sse",
      getToken: config.getToken ?? (() => {
        try { return localStorage.getItem("flop_token"); } catch { return null; }
      }),
      setToken: config.setToken ?? ((t) => {
        try {
          if (t) localStorage.setItem("flop_token", t);
          else localStorage.removeItem("flop_token");
        } catch {}
      }),
      getRefreshToken: config.getRefreshToken ?? (() => {
        try { return localStorage.getItem("flop_refresh_token"); } catch { return null; }
      }),
      setRefreshToken: config.setRefreshToken ?? ((t) => {
        try {
          if (t) localStorage.setItem("flop_refresh_token", t);
          else localStorage.removeItem("flop_refresh_token");
        } catch {}
      }),
    };
  }

  // ---- Views ----

  async view<T = any>(name: string, params: Record<string, any> = {}): Promise<ViewResult<T>> {
    if (this.config.batchViews === "none") {
      return this.execViewDirect<T>(name, params);
    }
    return this.enqueueView<T>(name, params);
  }

  private async execViewDirect<T>(name: string, params: Record<string, any>): Promise<ViewResult<T>> {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null) qs.set(k, String(v));
    }
    const url = `${this.config.host}/api/view/${name}?${qs}`;
    const res = await this.fetchJSON(url);
    return res as ViewResult<T>;
  }

  private enqueueView<T>(name: string, params: Record<string, any>): Promise<ViewResult<T>> {
    return new Promise((resolve, reject) => {
      this.pendingCalls.push({ id: nextId(), name, params, resolve, reject });
      this.scheduleBatchFlush();
    });
  }

  private scheduleBatchFlush() {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    const raf = typeof requestAnimationFrame === "function" ? requestAnimationFrame : (fn: () => void) => setTimeout(fn, 0);
    raf(() => this.flushBatch());
  }

  private async flushBatch() {
    this.flushScheduled = false;
    const calls = this.pendingCalls.splice(0);
    if (calls.length === 0) return;

    if (calls.length === 1) {
      const c = calls[0];
      try {
        const result = await this.execViewDirect(c.name, c.params);
        c.resolve(result);
      } catch (err) {
        c.reject(err);
      }
      return;
    }

    try {
      const payload = {
        calls: calls.map(c => ({ id: c.id, name: c.name, params: c.params })),
      };
      const res = await this.fetchJSON(`${this.config.host}/api/view/_batch`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      const results: BatchResult[] = (res as any).results || [];
      const resultMap = new Map<string, BatchResult>();
      for (const r of results) resultMap.set(r.id, r);

      for (const c of calls) {
        const r = resultMap.get(c.id);
        if (!r) {
          c.reject(new Error("no result for call " + c.id));
        } else if (r.error) {
          c.reject(new Error(r.error));
        } else {
          c.resolve({ data: r.data || [], total: r.total || 0 });
        }
      }
    } catch (err) {
      for (const c of calls) c.reject(err);
    }
  }

  // ---- Reducers ----

  async reduce<T = any>(name: string, data: Record<string, any> = {}): Promise<T> {
    const res = await this.fetchJSON(`${this.config.host}/api/reduce/${name}`, {
      method: "POST",
      body: JSON.stringify(data),
    });

    // Auto-refetch affected views
    if (this.config.autoRefetch) {
      this.invalidateAllWatches();
    }

    return res as T;
  }

  // ---- Watches ----

  watch(name: string, params: Record<string, any>, onData: (data: ViewResult) => void, onError?: (error: Error) => void): () => void {
    const id = nextId();
    const entry: WatchEntry = { id, name, params, onData, onError, realtime: false };
    this.watches.set(id, entry);

    // Initial fetch
    this.view(name, params)
      .then(data => onData(data))
      .catch(err => onError?.(toError(err)));

    return () => {
      this.watches.delete(id);
      this.syncSSE();
    };
  }

  subscribe(name: string, params: Record<string, any>, onData: (data: ViewResult) => void, onError?: (error: Error) => void): () => void {
    const id = nextId();
    const entry: WatchEntry = { id, name, params, onData, onError, realtime: true };
    this.watches.set(id, entry);

    // Initial fetch
    this.view(name, params)
      .then(data => onData(data))
      .catch(err => onError?.(toError(err)));

    this.syncSSE();

    return () => {
      this.watches.delete(id);
      this.syncSSE();
    };
  }

  private invalidateAllWatches() {
    for (const [id] of this.watches) {
      this.dirtyWatches.add(id);
    }
    this.scheduleWatchFlush();
  }

  private invalidateByTable(table: string) {
    for (const [id, entry] of this.watches) {
      if (entry.table === table || entry.name.includes(table)) {
        this.dirtyWatches.add(id);
      }
    }
    this.scheduleWatchFlush();
  }

  private scheduleWatchFlush() {
    if (this.watchFlushScheduled) return;
    this.watchFlushScheduled = true;
    const raf = typeof requestAnimationFrame === "function" ? requestAnimationFrame : (fn: () => void) => setTimeout(fn, 0);
    raf(() => this.flushDirtyWatches());
  }

  private async flushDirtyWatches() {
    this.watchFlushScheduled = false;
    const ids = [...this.dirtyWatches];
    this.dirtyWatches.clear();

    for (const id of ids) {
      const entry = this.watches.get(id);
      if (!entry) continue;
      try {
        const data = await this.view(entry.name, entry.params);
        entry.onData(data);
      } catch (err) {
        entry.onError?.(toError(err));
      }
    }
  }

  // ---- SSE ----

  private syncSSE() {
    if (this.config.realtime !== "sse" || typeof EventSource === "undefined") return;

    const hasRealtimeWatches = [...this.watches.values()].some(w => w.realtime);
    if (!hasRealtimeWatches) {
      this.closeSSE();
      return;
    }

    if (this.eventSource) return; // already connected

    const token = this.config.getToken();
    const url = `${this.config.host}/api/sse${token ? `?_token=${encodeURIComponent(token)}` : ""}`;

    this.eventSource = new EventSource(url);
    this.eventSource.addEventListener("message", (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.table) {
          this.invalidateByTable(data.table);
        } else {
          this.invalidateAllWatches();
        }
      } catch {
        this.invalidateAllWatches();
      }
    });
    this.eventSource.addEventListener("error", () => {
      this.closeSSE();
      this.sseReconnectTimer = setTimeout(() => this.syncSSE(), 1000);
    });
  }

  private closeSSE() {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    if (this.sseReconnectTimer) {
      clearTimeout(this.sseReconnectTimer);
      this.sseReconnectTimer = null;
    }
  }

  // ---- Auth ----

  async login(email: string, password: string): Promise<any> {
    const res = await this.fetchJSON(`${this.config.host}/api/auth/login`, {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    const data = res as any;
    if (data.token) this.config.setToken(data.token);
    if (data.refreshToken) this.config.setRefreshToken(data.refreshToken);
    return data;
  }

  async register(email: string, password: string, extra?: Record<string, any>): Promise<any> {
    const res = await this.fetchJSON(`${this.config.host}/api/auth/register`, {
      method: "POST",
      body: JSON.stringify({ email, password, ...extra }),
    });
    const data = res as any;
    if (data.token) this.config.setToken(data.token);
    if (data.refreshToken) this.config.setRefreshToken(data.refreshToken);
    return data;
  }

  async logout(): Promise<void> {
    const token = this.config.getToken();
    if (token) {
      try {
        await this.fetchJSON(`${this.config.host}/api/auth/logout`, { method: "POST" });
      } catch {}
    }
    this.config.setToken(null);
    this.config.setRefreshToken(null);
    this.closeSSE();
  }

  get token(): string | null {
    return this.config.getToken();
  }

  get isAuthenticated(): boolean {
    return !!this.config.getToken();
  }

  // ---- Token refresh ----

  private async refreshAccessToken(): Promise<string | null> {
    const rt = this.config.getRefreshToken();
    if (!rt) return null;

    try {
      const res = await fetch(`${this.config.host}/api/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refreshToken: rt }),
      });
      if (!res.ok) {
        // Refresh failed — clear tokens
        this.config.setToken(null);
        this.config.setRefreshToken(null);
        return null;
      }
      const data = await res.json();
      if (data.token) {
        this.config.setToken(data.token);
        return data.token;
      }
    } catch {}
    return null;
  }

  // ---- HTTP ----

  private async fetchJSON(url: string, init?: RequestInit): Promise<unknown> {
    const doFetch = (token: string | null) => {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        ...((init?.headers as Record<string, string>) || {}),
      };
      if (token) {
        headers["Authorization"] = `Bearer ${token}`;
      }
      return fetch(url, { ...init, headers });
    };

    const token = this.config.getToken();
    let res = await doFetch(token);

    // Auto-refresh on 401
    if (res.status === 401 && this.config.getRefreshToken()) {
      // Deduplicate concurrent refresh attempts
      if (!this.refreshPromise) {
        this.refreshPromise = this.refreshAccessToken().finally(() => {
          this.refreshPromise = null;
        });
      }
      const newToken = await this.refreshPromise;
      if (newToken) {
        res = await doFetch(newToken);
      }
    }

    if (!res.ok) {
      let payload: any;
      try { payload = await res.json(); } catch { payload = {}; }
      throw new FlopError(res.status, payload?.error || res.statusText, payload);
    }
    return res.json();
  }

  // ---- Cleanup ----

  destroy() {
    this.closeSSE();
    this.watches.clear();
    this.pendingCalls = [];
    this.dirtyWatches.clear();
  }
}

// ---- Errors ----

export class FlopError extends Error {
  status: number;
  payload: any;
  constructor(status: number, message: string, payload?: any) {
    super(message);
    this.name = "FlopError";
    this.status = status;
    this.payload = payload;
  }
}

function toError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}
