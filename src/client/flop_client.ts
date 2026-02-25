// Flop<T> client class — Proxy-based typed namespaces for views, reducers, subscriptions

import { type TokenStore, LocalStorageTokenStore, MemoryTokenStore } from "./token_store.ts";
import { AuthClient } from "./auth_client.ts";
import { Subscription } from "./subscription.ts";

export interface FlopClientConfig {
  host: string;
  tokenStore?: TokenStore;
}

// Type helpers for the client
export type ViewNamespace<V> = {
  [K in keyof V]: (params: V[K] extends { params: infer P } ? P : never) => Promise<
    V[K] extends { result: infer R } ? R : unknown
  >;
};

export type SubscribeNamespace<V> = {
  [K in keyof V]: (params: V[K] extends { params: infer P } ? P : never) => Subscription<
    V[K] extends { result: infer R } ? R : unknown
  >;
};

export type ReduceNamespace<R> = {
  [K in keyof R]: (params: R[K] extends { params: infer P } ? P : never) => Promise<
    R[K] extends { result: infer Res } ? Res : unknown
  >;
};

export class Flop<T extends { reducers: any; views: any } = { reducers: any; views: any }> {
  private host: string;
  private tokenStore: TokenStore;
  private refreshTokenStore: TokenStore;

  readonly users: AuthClient;
  readonly view: ViewNamespace<T["views"]>;
  readonly subscribe: SubscribeNamespace<T["views"]>;
  readonly reduce: ReduceNamespace<T["reducers"]>;

  constructor(config: FlopClientConfig) {
    this.host = config.host.replace(/\/$/, "");

    // Detect environment for token store
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

    // Proxy-based namespaces
    this.view = new Proxy({} as any, {
      get: (_: any, name: string) => (params: any) => this.request("GET", `/view/${name}`, params),
    });

    this.reduce = new Proxy({} as any, {
      get: (_: any, name: string) => (params: any) =>
        this.request("POST", `/reduce/${name}`, params),
    });

    this.subscribe = new Proxy({} as any, {
      get: (_: any, name: string) => (params: any) => this.createSubscription(name, params),
    });
  }

  private async request(method: "GET" | "POST", path: string, params: any): Promise<any> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    const token = this.tokenStore.get();
    if (token) {
      headers["Authorization"] = `Bearer ${token}`;
    }

    let url = `${this.host}${path}`;
    let body: string | undefined;

    if (method === "GET" && params) {
      const searchParams = new URLSearchParams();
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null) {
          searchParams.set(key, String(value));
        }
      }
      const qs = searchParams.toString();
      if (qs) url += `?${qs}`;
    } else if (method === "POST") {
      body = JSON.stringify(params ?? {});
    }

    const res = await fetch(url, { method, headers, body });

    if (res.status === 401) {
      // Try to refresh token
      try {
        await this.users.refresh();
        // Retry with new token
        headers["Authorization"] = `Bearer ${this.tokenStore.get()}`;
        const retryRes = await fetch(url, { method, headers, body });
        if (!retryRes.ok) {
          const data = await retryRes.json();
          throw new Error(data.error ?? `Request failed: ${retryRes.status}`);
        }
        const data = await retryRes.json();
        return data.data;
      } catch {
        throw new Error("Authentication required");
      }
    }

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? `Request failed: ${res.status}`);
    }

    const data = await res.json();
    return data.data;
  }

  private createSubscription<R>(viewName: string, params: any): Subscription<R> {
    const searchParams = new URLSearchParams();

    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null) {
          searchParams.set(key, String(value));
        }
      }
    }

    // Add token to URL for EventSource (can't set custom headers)
    const token = this.tokenStore.get();
    if (token) {
      searchParams.set("_token", token);
    }

    const qs = searchParams.toString();
    const url = `${this.host}/view/${viewName}${qs ? `?${qs}` : ""}`;

    return new Subscription<R>(url);
  }
}
