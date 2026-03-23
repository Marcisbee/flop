package util

import "container/list"

// LRUCache is a generic least-recently-used cache.
type LRUCache[K comparable, V any] struct {
	maxSize int
	ll      *list.List
	items   map[K]*list.Element
	OnEvict func(K, V)
}

type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

func NewLRUCache[K comparable, V any](maxSize int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		maxSize: maxSize,
		ll:      list.New(),
		items:   make(map[K]*list.Element),
	}
}

func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*lruEntry[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (c *LRUCache[K, V]) Set(key K, value V) {
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*lruEntry[K, V]).value = value
		return
	}
	if c.ll.Len() >= c.maxSize {
		// Evict oldest (back of list)
		oldest := c.ll.Back()
		if oldest != nil {
			e := oldest.Value.(*lruEntry[K, V])
			c.ll.Remove(oldest)
			delete(c.items, e.key)
			if c.OnEvict != nil {
				c.OnEvict(e.key, e.value)
			}
		}
	}
	el := c.ll.PushFront(&lruEntry[K, V]{key: key, value: value})
	c.items[key] = el
}

func (c *LRUCache[K, V]) Has(key K) bool {
	_, ok := c.items[key]
	return ok
}

func (c *LRUCache[K, V]) Delete(key K) bool {
	if el, ok := c.items[key]; ok {
		c.ll.Remove(el)
		delete(c.items, key)
		return true
	}
	return false
}

func (c *LRUCache[K, V]) Len() int {
	return c.ll.Len()
}

func (c *LRUCache[K, V]) Clear() {
	c.ll.Init()
	c.items = make(map[K]*list.Element)
}

// Range iterates over all entries. fn returning false stops iteration.
func (c *LRUCache[K, V]) Range(fn func(K, V) bool) {
	for el := c.ll.Front(); el != nil; el = el.Next() {
		e := el.Value.(*lruEntry[K, V])
		if !fn(e.key, e.value) {
			return
		}
	}
}
