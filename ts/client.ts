// Client public API barrel — for browser/client-side usage

export { Flop, FlopRequestError, type FlopClientConfig } from "./src/client/flop_client.ts";
export { AuthClient, type AuthResult } from "./src/client/auth_client.ts";
export { LocalStorageTokenStore, MemoryTokenStore, type TokenStore } from "./src/client/token_store.ts";

// Re-export the FlopSchema type for convenience
export type { FlopSchema } from "./src/types.ts";
