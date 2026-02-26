// JWT token persistence — pluggable storage for auth tokens

export interface TokenStore {
  get(): string | null;
  set(token: string): void;
  clear(): void;
}

export class LocalStorageTokenStore implements TokenStore {
  private key: string;

  constructor(key = "flop_token") {
    this.key = key;
  }

  get(): string | null {
    try {
      return localStorage.getItem(this.key);
    } catch {
      return null;
    }
  }

  set(token: string): void {
    try {
      localStorage.setItem(this.key, token);
    } catch {
      // localStorage not available
    }
  }

  clear(): void {
    try {
      localStorage.removeItem(this.key);
    } catch {
      // localStorage not available
    }
  }
}

export class MemoryTokenStore implements TokenStore {
  private token: string | null = null;

  get(): string | null {
    return this.token;
  }

  set(token: string): void {
    this.token = token;
  }

  clear(): void {
    this.token = null;
  }
}
