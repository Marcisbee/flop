// Generic LRU cache using Map iteration order

export class LRUCache<K, V> {
  private cache = new Map<K, V>();
  private readonly maxSize: number;
  private onEvict?: (key: K, value: V) => void;

  constructor(maxSize: number, onEvict?: (key: K, value: V) => void) {
    this.maxSize = maxSize;
    this.onEvict = onEvict;
  }

  get(key: K): V | undefined {
    const value = this.cache.get(key);
    if (value === undefined) return undefined;
    // Move to end (most recently used)
    this.cache.delete(key);
    this.cache.set(key, value);
    return value;
  }

  set(key: K, value: V): void {
    if (this.cache.has(key)) {
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      // Evict least recently used (first entry)
      const firstKey = this.cache.keys().next().value!;
      const firstValue = this.cache.get(firstKey)!;
      this.cache.delete(firstKey);
      this.onEvict?.(firstKey, firstValue);
    }
    this.cache.set(key, value);
  }

  has(key: K): boolean {
    return this.cache.has(key);
  }

  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  get size(): number {
    return this.cache.size;
  }

  clear(): void {
    this.cache.clear();
  }

  *entries(): IterableIterator<[K, V]> {
    yield* this.cache.entries();
  }

  *values(): IterableIterator<V> {
    yield* this.cache.values();
  }

  *keys(): IterableIterator<K> {
    yield* this.cache.keys();
  }
}
