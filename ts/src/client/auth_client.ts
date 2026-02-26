// Client-side auth methods

import type { TokenStore } from "./token_store.ts";

export interface AuthResult {
  token: string;
  refreshToken?: string;
  user: {
    id: string;
    email: string;
    roles: string[];
  };
}

export class AuthClient {
  private host: string;
  private tokenStore: TokenStore;
  private refreshTokenStore: TokenStore;

  constructor(host: string, tokenStore: TokenStore, refreshTokenStore: TokenStore) {
    this.host = host;
    this.tokenStore = tokenStore;
    this.refreshTokenStore = refreshTokenStore;
  }

  async authWithPassword(email: string, password: string): Promise<AuthResult> {
    const res = await fetch(`${this.host}/api/auth/password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Authentication failed");
    }

    const result = await res.json() as AuthResult;
    this.tokenStore.set(result.token);
    if (result.refreshToken) {
      this.refreshTokenStore.set(result.refreshToken);
    }
    return result;
  }

  async register(email: string, password: string, name?: string): Promise<AuthResult> {
    const res = await fetch(`${this.host}/api/auth/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password, name }),
    });

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Registration failed");
    }

    const result = await res.json() as AuthResult;
    this.tokenStore.set(result.token);
    return result;
  }

  async authWithOAuth2(opts: { provider: string }): Promise<AuthResult> {
    // Browser-only: open popup for OAuth2 flow
    const win = globalThis as unknown as {
      screen: { width: number; height: number };
      open: (url: string, name: string, features: string) => { closed: boolean } | null;
      addEventListener: (type: string, handler: (event: Event) => void) => void;
      removeEventListener: (type: string, handler: (event: Event) => void) => void;
    };

    const width = 500;
    const height = 600;
    const left = (win.screen.width - width) / 2;
    const top = (win.screen.height - height) / 2;

    return new Promise((resolve, reject) => {
      const popup = win.open(
        `${this.host}/api/auth/oauth2/${opts.provider}/authorize`,
        "flop_oauth2",
        `width=${width},height=${height},left=${left},top=${top}`,
      );

      if (!popup) {
        reject(new Error("Failed to open OAuth2 popup"));
        return;
      }

      // Listen for message from popup
      const handler = (event: Event) => {
        const msg = event as MessageEvent;
        if (msg.origin !== new URL(this.host).origin) return;
        if (msg.data?.type !== "flop_oauth2_result") return;

        win.removeEventListener("message", handler);

        if (msg.data.error) {
          reject(new Error(msg.data.error));
        } else {
          const result = msg.data.result as AuthResult;
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

  async requestVerification(email: string): Promise<void> {
    const res = await fetch(`${this.host}/api/auth/verify`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.authHeaders(),
      },
      body: JSON.stringify({ email }),
    });

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Verification request failed");
    }
  }

  async requestPasswordReset(email: string): Promise<void> {
    const res = await fetch(`${this.host}/api/auth/reset-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email }),
    });

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Password reset request failed");
    }
  }

  async requestEmailChange(newEmail: string): Promise<void> {
    const res = await fetch(`${this.host}/api/auth/change-email`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.authHeaders(),
      },
      body: JSON.stringify({ newEmail }),
    });

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Email change request failed");
    }
  }

  async refresh(): Promise<string> {
    const refreshToken = this.refreshTokenStore.get();
    if (!refreshToken) throw new Error("No refresh token");

    const res = await fetch(`${this.host}/api/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refreshToken }),
    });

    if (!res.ok) {
      const data = await res.json();
      throw new Error(data.error ?? "Token refresh failed");
    }

    const { token } = await res.json();
    this.tokenStore.set(token);
    return token;
  }

  logout(): void {
    this.tokenStore.clear();
    this.refreshTokenStore.clear();
  }

  get isAuthenticated(): boolean {
    return this.tokenStore.get() !== null;
  }

  get token(): string | null {
    return this.tokenStore.get();
  }

  private authHeaders(): Record<string, string> {
    const token = this.tokenStore.get();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }
}
