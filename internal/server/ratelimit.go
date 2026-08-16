package server

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// RateLimiter is a small fixed-window, per-key rate limiter for
// unauthenticated credential endpoints. It exists to blunt online
// brute-force and email-bombing abuse; it is not a general traffic shaper.
type RateLimiter struct {
	mu     sync.Mutex
	hits   map[string]rateLimitBucket
	limit  int
	window time.Duration
}

type rateLimitBucket struct {
	windowStart time.Time
	count       int
}

// NewRateLimiter allows limit requests per window per key.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	if limit <= 0 || window <= 0 {
		return nil
	}
	return &RateLimiter{
		hits:   make(map[string]rateLimitBucket),
		limit:  limit,
		window: window,
	}
}

// Allow reports whether the key is within its budget for the current window.
func (l *RateLimiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	bucket, ok := l.hits[key]
	if !ok || now.Sub(bucket.windowStart) >= l.window {
		bucket = rateLimitBucket{windowStart: now}
	}
	bucket.count++
	l.hits[key] = bucket
	// Bound memory: once the map grows large, drop fully expired buckets.
	if len(l.hits) > 10000 {
		for k, b := range l.hits {
			if now.Sub(b.windowStart) >= l.window {
				delete(l.hits, k)
			}
		}
	}
	return bucket.count <= l.limit
}

// ClientIP returns the client address of a request for rate-limit keying.
// Only the direct peer is trusted; forwarded headers are spoofable.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return host
}
