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
	mu          sync.Mutex
	hits        map[string]rateLimitBucket
	limit       int
	window      time.Duration
	nextCleanup time.Time
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
	// Expiration maintenance is periodic, not a full map scan on every
	// rejected request. Keep a hard bound even while every bucket is live.
	if !now.Before(l.nextCleanup) {
		for k, b := range l.hits {
			if now.Sub(b.windowStart) >= l.window {
				delete(l.hits, k)
			}
		}
		l.nextCleanup = now.Add(l.window)
	}
	bucket, ok := l.hits[key]
	if !ok && len(l.hits) >= 10000 {
		return false
	}
	if !ok || now.Sub(bucket.windowStart) >= l.window {
		bucket = rateLimitBucket{windowStart: now}
	}
	if bucket.count >= l.limit {
		return false
	}
	bucket.count++
	l.hits[key] = bucket
	return true
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
