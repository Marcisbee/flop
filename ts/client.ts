// Client public API barrel — for browser/client-side usage

export { Flop, type FlopClientConfig, type ViewNamespace, type SubscribeNamespace, type ReduceNamespace } from "./src/client/flop_client.ts";
export { Subscription } from "./src/client/subscription.ts";
export { AuthClient, type AuthResult } from "./src/client/auth_client.ts";
export { LocalStorageTokenStore, MemoryTokenStore, type TokenStore } from "./src/client/token_store.ts";

// Re-export the FlopSchema type for convenience
export type { FlopSchema } from "./src/types.ts";
