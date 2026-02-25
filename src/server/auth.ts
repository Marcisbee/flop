// Built-in auth system — password auth, JWT tokens, role management

import type { AuthContext } from "../types.ts";
import type { TableInstance } from "../database.ts";

const encoder = new TextEncoder();

// HMAC-SHA256 JWT implementation using Web Crypto API

export interface JWTPayload {
  sub: string;
  email: string;
  name: string;
  roles: string[];
  iat: number;
  exp: number;
}

function base64url(data: Uint8Array): string {
  return btoa(String.fromCharCode(...data))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function base64urlDecode(str: string): Uint8Array {
  const padded = str.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

// Cache imported CryptoKey per secret to avoid re-importing on every request
const keyCache = new Map<string, CryptoKey>();

async function getHmacKey(secret: string): Promise<CryptoKey> {
  let key = keyCache.get(secret);
  if (key) return key;
  key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  keyCache.set(secret, key);
  return key;
}

async function hmacSign(data: string, secret: string): Promise<string> {
  const key = await getHmacKey(secret);
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  return base64url(new Uint8Array(signature));
}

async function hmacVerify(data: string, signature: string, secret: string): Promise<boolean> {
  const expected = await hmacSign(data, secret);
  return expected === signature;
}

export async function createJWT(payload: JWTPayload, secret: string): Promise<string> {
  const header = base64url(encoder.encode(JSON.stringify({ alg: "HS256", typ: "JWT" })));
  const body = base64url(encoder.encode(JSON.stringify(payload)));
  const signature = await hmacSign(`${header}.${body}`, secret);
  return `${header}.${body}.${signature}`;
}

export async function verifyJWT(token: string, secret: string): Promise<JWTPayload | null> {
  const parts = token.split(".");
  if (parts.length !== 3) return null;

  const [header, body, signature] = parts;

  const valid = await hmacVerify(`${header}.${body}`, signature, secret);
  if (!valid) return null;

  const payload = JSON.parse(new TextDecoder().decode(base64urlDecode(body))) as JWTPayload;

  // Check expiration
  if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
    return null;
  }

  return payload;
}

export function jwtToAuthContext(payload: JWTPayload): AuthContext {
  return {
    id: payload.sub,
    email: payload.email,
    roles: payload.roles ?? [],
  };
}

export function extractBearerToken(req: Request): string | null {
  const header = req.headers.get("Authorization");
  if (header?.startsWith("Bearer ")) {
    return header.slice(7);
  }
  // Also check query param for SSE/WS
  const url = new URL(req.url);
  return url.searchParams.get("_token");
}

// Simple bcrypt using Web Crypto (not real bcrypt — placeholder using PBKDF2)
// For production, use a proper bcrypt library
export async function hashPassword(password: string, _rounds = 10): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(password),
    "PBKDF2",
    false,
    ["deriveBits"],
  );
  const derived = await crypto.subtle.deriveBits(
    { name: "PBKDF2", salt, iterations: 100000, hash: "SHA-256" },
    key,
    256,
  );
  const saltHex = [...salt].map((b) => b.toString(16).padStart(2, "0")).join("");
  const hashHex = [...new Uint8Array(derived)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `$pbkdf2$${saltHex}$${hashHex}`;
}

export async function verifyPassword(password: string, hash: string): Promise<boolean> {
  const parts = hash.split("$");
  if (parts[1] !== "pbkdf2" || parts.length !== 4) return false;

  const salt = new Uint8Array(
    parts[2].match(/.{2}/g)!.map((h) => parseInt(h, 16)),
  );
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(password),
    "PBKDF2",
    false,
    ["deriveBits"],
  );
  const derived = await crypto.subtle.deriveBits(
    { name: "PBKDF2", salt, iterations: 100000, hash: "SHA-256" },
    key,
    256,
  );
  const hashHex = [...new Uint8Array(derived)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return hashHex === parts[3];
}

// Auth handler for register/login endpoints
export class AuthService {
  private authTable: TableInstance;
  private secret: string;
  private accessTokenTTL: number;
  private refreshTokenTTL: number;

  constructor(
    authTable: TableInstance,
    secret: string,
    accessTokenTTL = 900, // 15 min
    refreshTokenTTL = 604800, // 7 days
  ) {
    this.authTable = authTable;
    this.secret = secret;
    this.accessTokenTTL = accessTokenTTL;
    this.refreshTokenTTL = refreshTokenTTL;
  }

  async register(email: string, password: string, name?: string): Promise<{ token: string; user: AuthContext }> {
    // Check if email already exists
    const existing = await this.findByEmail(email);
    if (existing) {
      throw new Error("Email already registered");
    }

    const hashedPassword = await hashPassword(password);
    const row = await this.authTable.insert({
      email,
      password: hashedPassword,
      name: name ?? "",
      roles: ["user"],
      verified: false,
    });

    const pk = row[this.authTable.primaryKeyField] as string;
    const payload = await this.issueToken(pk, email, name ?? "", row.roles as string[]);
    return {
      token: payload,
      user: { id: pk, email, roles: row.roles as string[] },
    };
  }

  async login(email: string, password: string): Promise<{ token: string; refreshToken: string; user: AuthContext }> {
    const user = await this.findByEmail(email);
    if (!user) {
      throw new Error("Invalid credentials");
    }

    const valid = await verifyPassword(password, user.password as string);
    if (!valid) {
      throw new Error("Invalid credentials");
    }

    const pk = user[this.authTable.primaryKeyField] as string;
    const token = await this.issueToken(
      pk,
      user.email as string,
      user.name as string,
      user.roles as string[],
    );
    const refreshToken = await this.issueRefreshToken(pk);

    return {
      token,
      refreshToken,
      user: { id: pk, email: user.email as string, roles: user.roles as string[] },
    };
  }

  async refresh(refreshToken: string): Promise<{ token: string }> {
    const payload = await verifyJWT(refreshToken, this.secret);
    if (!payload) {
      throw new Error("Invalid refresh token");
    }

    const user = await this.authTable.get(payload.sub);
    if (!user) {
      throw new Error("User not found");
    }

    const token = await this.issueToken(
      payload.sub,
      user.email as string,
      user.name as string,
      user.roles as string[],
    );

    return { token };
  }

  async hasSuperadmin(): Promise<boolean> {
    const rows = await this.authTable.scan(10000);
    return rows.some((r) => Array.isArray(r.roles) && (r.roles as string[]).includes("superadmin"));
  }

  async registerSuperadmin(email: string, password: string, name?: string): Promise<{ token: string; user: AuthContext }> {
    const existing = await this.findByEmail(email);
    if (existing) {
      throw new Error("Email already registered");
    }

    const hashedPassword = await hashPassword(password);
    const row = await this.authTable.insert({
      email,
      password: hashedPassword,
      name: name ?? "",
      roles: ["superadmin"],
      verified: true,
    });

    const pk = row[this.authTable.primaryKeyField] as string;
    const token = await this.issueToken(pk, email, name ?? "", row.roles as string[]);
    return {
      token,
      user: { id: pk, email, roles: row.roles as string[] },
    };
  }

  async setRoles(userId: string, roles: string[]): Promise<void> {
    await this.authTable.update(userId, { roles });
  }

  private async findByEmail(email: string): Promise<Record<string, unknown> | null> {
    // Scan for email match (ideally would use a secondary index)
    const rows = await this.authTable.scan(10000);
    return rows.find((r) => r.email === email) ?? null;
  }

  private async issueToken(id: string, email: string, name: string, roles: string[]): Promise<string> {
    const now = Math.floor(Date.now() / 1000);
    return createJWT(
      { sub: id, email, name, roles, iat: now, exp: now + this.accessTokenTTL },
      this.secret,
    );
  }

  private async issueRefreshToken(id: string): Promise<string> {
    const now = Math.floor(Date.now() / 1000);
    return createJWT(
      { sub: id, email: "", name: "", roles: [], iat: now, exp: now + this.refreshTokenTTL },
      this.secret,
    );
  }
}
