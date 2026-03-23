var __defProp = Object.defineProperty;
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __publicField = (obj, key, value) => __defNormalProp(obj, typeof key !== "symbol" ? key + "" : key, value);

// ts/src/client/token_store.ts
var LocalStorageTokenStore = class {
  constructor(key = "flop_token") {
    __publicField(this, "key");
    this.key = key;
  }
  get() {
    try {
      return localStorage.getItem(this.key);
    } catch {
      return null;
    }
  }
  set(token) {
    try {
      localStorage.setItem(this.key, token);
    } catch {
    }
  }
  clear() {
    try {
      localStorage.removeItem(this.key);
    } catch {
    }
  }
};
var MemoryTokenStore = class {
  constructor() {
    __publicField(this, "token", null);
  }
  get() {
    return this.token;
  }
  set(token) {
    this.token = token;
  }
  clear() {
    this.token = null;
  }
};

// ts/src/client/auth_client.ts
var AuthClient = class {
  constructor(host, tokenStore, refreshTokenStore) {
    __publicField(this, "host");
    __publicField(this, "tokenStore");
    __publicField(this, "refreshTokenStore");
    this.host = host;
    this.tokenStore = tokenStore;
    this.refreshTokenStore = refreshTokenStore;
  }
  async authWithPassword(email, password) {
    const res = await fetch(`${this.host}/api/auth/password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Authentication failed");
    }
    const result = await res.json();
    this.tokenStore.set(result.token);
    if (result.refreshToken) {
      this.refreshTokenStore.set(result.refreshToken);
    }
    return result;
  }
  async register(email, password, name) {
    const res = await fetch(`${this.host}/api/auth/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password, name })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Registration failed");
    }
    const result = await res.json();
    this.tokenStore.set(result.token);
    return result;
  }
  async authWithOAuth2(opts) {
    const win = globalThis;
    const width = 500;
    const height = 600;
    const left = (win.screen.width - width) / 2;
    const top = (win.screen.height - height) / 2;
    return new Promise((resolve, reject) => {
      const popup = win.open(
        `${this.host}/api/auth/oauth2/${opts.provider}/authorize`,
        "flop_oauth2",
        `width=${width},height=${height},left=${left},top=${top}`
      );
      if (!popup) {
        reject(new Error("Failed to open OAuth2 popup"));
        return;
      }
      const handler = (event) => {
        const msg = event;
        if (msg.origin !== new URL(this.host).origin) return;
        if (msg.data?.type !== "flop_oauth2_result") return;
        win.removeEventListener("message", handler);
        if (msg.data.error) {
          reject(new Error(msg.data.error));
        } else {
          const result = msg.data.result;
          this.tokenStore.set(result.token);
          if (result.refreshToken) {
            this.refreshTokenStore.set(result.refreshToken);
          }
          resolve(result);
        }
      };
      win.addEventListener("message", handler);
    });
  }
  async requestVerification(email) {
    const res = await fetch(`${this.host}/api/auth/verify`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.authHeaders()
      },
      body: JSON.stringify({ email })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Verification request failed");
    }
  }
  async requestPasswordReset(email) {
    const res = await fetch(`${this.host}/api/auth/reset-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Password reset request failed");
    }
  }
  async requestEmailChange(newEmail) {
    const res = await fetch(`${this.host}/api/auth/change-email`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.authHeaders()
      },
      body: JSON.stringify({ newEmail })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Email change request failed");
    }
  }
  async refresh() {
    const refreshToken = this.refreshTokenStore.get();
    if (!refreshToken) throw new Error("No refresh token");
    const res = await fetch(`${this.host}/api/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refreshToken })
    });
    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Token refresh failed");
    }
    const { token } = await res.json();
    this.tokenStore.set(token);
    return token;
  }
  logout() {
    this.tokenStore.clear();
    this.refreshTokenStore.clear();
  }
  get isAuthenticated() {
    return this.tokenStore.get() !== null;
  }
  get token() {
    return this.tokenStore.get();
  }
  authHeaders() {
    const token = this.tokenStore.get();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }
};

// ts/src/client/flop_client.ts
var FlopRequestError = class extends Error {
  constructor(status, message, payload) {
    super(message);
    __publicField(this, "status");
    __publicField(this, "payload");
    this.name = "FlopRequestError";
    this.status = status;
    this.payload = payload;
  }
};
var Flop = class {
  constructor(config) {
    __publicField(this, "host");
    __publicField(this, "tokenStore");
    __publicField(this, "refreshTokenStore");
    __publicField(this, "batchViews");
    __publicField(this, "autoRefetch");
    __publicField(this, "nextBatchCallID", 1);
    __publicField(this, "nextWatchID", 1);
    __publicField(this, "pendingViewCalls", []);
    __publicField(this, "batchFlushScheduled", false);
    __publicField(this, "watches", /* @__PURE__ */ new Map());
    __publicField(this, "dirtyWatches", /* @__PURE__ */ new Set());
    __publicField(this, "watchFlushScheduled", false);
    __publicField(this, "users");
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
    this.refreshTokenStore = typeof localStorage !== "undefined" ? new LocalStorageTokenStore("flop_refresh_token") : new MemoryTokenStore();
    this.users = new AuthClient(this.host, this.tokenStore, this.refreshTokenStore);
  }
  view(name, params) {
    return this.enqueueViewCall(String(name), params);
  }
  async reducer(name, params) {
    const { data, headers } = await this.requestJSON("POST", `/api/reduce/${String(name)}`, params);
    const writes = parseCSVHeader(headers.get("X-Flop-Writes"));
    if (this.autoRefetch && writes.length > 0) {
      this.invalidateViewsByTouchedTables(writes);
    }
    return data;
  }
  watch(name, params, onData, onError) {
    const watchID = this.nextWatchID++;
    const entry = {
      id: watchID,
      name: String(name),
      params,
      reads: /* @__PURE__ */ new Set(),
      onData,
      onError
    };
    this.watches.set(watchID, entry);
    this.enqueueViewCall(entry.name, entry.params, watchID).then(
      (value) => entry.onData(value),
      (err) => entry.onError?.(toError(err))
    );
    return () => {
      this.watches.delete(watchID);
      this.dirtyWatches.delete(watchID);
    };
  }
  enqueueViewCall(name, params, watchId) {
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
        watchId
      });
      this.scheduleBatchFlush();
    });
  }
  scheduleBatchFlush() {
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
  async flushViewBatch() {
    if (this.pendingViewCalls.length === 0) return;
    const calls = this.pendingViewCalls.splice(0, this.pendingViewCalls.length);
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
      calls: calls.map((c) => ({ id: c.id, name: c.name, params: c.params ?? {} }))
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
        if (call.watchId !== void 0) {
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
  async runSingleView(name, params, watchId) {
    const query = new URLSearchParams();
    if (params && typeof params === "object") {
      for (const [key, value] of Object.entries(params)) {
        if (value !== void 0 && value !== null) query.set(key, String(value));
      }
    }
    const path = `/api/view/${name}${query.toString() ? `?${query}` : ""}`;
    const { data, headers } = await this.requestJSON("GET", path);
    if (watchId !== void 0) {
      const watch = this.watches.get(watchId);
      if (watch) {
        watch.reads = new Set(parseCSVHeader(headers.get("X-Flop-Reads")));
      }
    }
    return data;
  }
  invalidateViewsByTouchedTables(touchedTables) {
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
  scheduleWatchFlush() {
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
  async flushDirtyWatches() {
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
  async requestJSON(method, path, body) {
    const headers = { "Content-Type": "application/json" };
    const token = this.tokenStore.get();
    if (token) headers.Authorization = `Bearer ${token}`;
    const execute = async () => {
      return fetch(`${this.host}${path}`, {
        method,
        headers,
        body: method === "POST" ? JSON.stringify(body ?? {}) : void 0
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
      }
    }
    const payload = await safeJSON(res);
    if (!res.ok) {
      throw new FlopRequestError(res.status, String(payload?.error ?? `Request failed: ${res.status}`), payload);
    }
    return { data: payload?.data ?? payload, headers: res.headers };
  }
};
function parseCSVHeader(value) {
  if (!value) return [];
  return value.split(",").map((x) => x.trim()).filter(Boolean);
}
function toError(err) {
  return err instanceof Error ? err : new Error(String(err));
}
async function safeJSON(res) {
  try {
    return await res.json();
  } catch {
    return {};
  }
}
function asBatchResults(data) {
  if (!data || typeof data !== "object") return [];
  const raw = data.results;
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const item of raw) {
    if (!item || typeof item !== "object") continue;
    const r = item;
    const id = typeof r.id === "string" ? r.id : "";
    if (!id) continue;
    out.push({
      id,
      data: r.data,
      reads: Array.isArray(r.reads) ? r.reads.filter((x) => typeof x === "string") : void 0,
      error: typeof r.error === "string" ? r.error : void 0
    });
  }
  return out;
}
function getAnimationFrame() {
  const g = globalThis;
  if (typeof g.requestAnimationFrame === "function") {
    return g.requestAnimationFrame.bind(g);
  }
  return null;
}
export {
  AuthClient,
  Flop,
  FlopRequestError,
  LocalStorageTokenStore,
  MemoryTokenStore
};
//# sourceMappingURL=flop-client.js.map
