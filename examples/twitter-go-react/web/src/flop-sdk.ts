// Typed wrapper for the runtime SDK bundle used by this example.

export interface TokenStore {
  get(): string | null;
  set(token: string): void;
  clear(): void;
}

export interface FlopClientConfig {
  host: string;
  tokenStore?: TokenStore;
  batchViews?: 'frame' | 'none';
  autoRefetch?: boolean;
  realtime?: 'sse' | 'none';
}

type ViewDef = { input: any; output: any };
type ReducerDef = { input: any; output: any };

type ViewInput<V, K extends keyof V> = V[K] extends ViewDef ? V[K]['input'] : never;
type ViewOutput<V, K extends keyof V> = V[K] extends ViewDef ? V[K]['output'] : unknown;
type ReducerInput<R, K extends keyof R> = R[K] extends ReducerDef ? R[K]['input'] : never;
type ReducerOutput<R, K extends keyof R> = R[K] extends ReducerDef ? R[K]['output'] : unknown;

export interface FlopClient<T extends { reducers: Record<string, any>; views: Record<string, any> }> {
  view<K extends keyof T['views']>(name: K, params: ViewInput<T['views'], K>): Promise<ViewOutput<T['views'], K>>;
  reducer<K extends keyof T['reducers']>(
    name: K,
    params: ReducerInput<T['reducers'], K>,
  ): Promise<ReducerOutput<T['reducers'], K>>;
  subscribe<K extends keyof T['views']>(
    name: K,
    params: ViewInput<T['views'], K>,
    onData: (value: ViewOutput<T['views'], K>) => void,
    onError?: (error: Error) => void,
  ): () => void;
  watch<K extends keyof T['views']>(
    name: K,
    params: ViewInput<T['views'], K>,
    onData: (value: ViewOutput<T['views'], K>) => void,
    onError?: (error: Error) => void,
  ): () => void;
}

// @ts-expect-error runtime ESM bundle has no declaration file
import { Flop as RuntimeFlop } from '../assets/flop-client.js';

export const Flop = RuntimeFlop as unknown as new <
  T extends { reducers: Record<string, any>; views: Record<string, any> },
>(config: FlopClientConfig) => FlopClient<T>;
