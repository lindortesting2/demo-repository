package ratelimit

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Strategy controls how limits are applied when multiple keys match.
type Strategy int

const (
	StrategyFixedWindow Strategy = iota
	StrategySlidingLog
	StrategyTokenBucket
)

// Limit defines the rate constraint for a particular bucket.
type Limit struct {
	// Requests is the maximum number of allowed requests within the window.
	Requests int
	// Window is the duration of each rate-limiting window.
	Window time.Duration
	// BurstMultiplier allows short bursts above the sustained rate. A value
	// of 1.0 means no burst headroom; 2.0 doubles the instantaneous capacity.
	BurstMultiplier float64
}

func (l Limit) effectiveBurst() int {
	if l.BurstMultiplier <= 0 {
		return l.Requests
	}
	return int(math.Ceil(float64(l.Requests) * l.BurstMultiplier))
}

// Entry tracks state for a single rate-limit bucket.
type Entry struct {
	Tokens    float64
	LastSeen  time.Time
	HitCount  int64
	WindowStart time.Time
}

// KeyFunc extracts the rate-limit key from an incoming request. Returning an
// empty string causes the request to bypass limiting entirely.
type KeyFunc func(r *http.Request) string

// IPKeyFunc returns the client IP, preferring X-Forwarded-For when present.
func IPKeyFunc(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the leftmost IP which is the original client.
		if idx := strings.IndexByte(xff, ','); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	// Strip the port from RemoteAddr.
	addr := r.RemoteAddr
	if idx := strings.LastIndexByte(addr, ':'); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// TenantKeyFunc builds a composite key from a header value, useful for
// multi-tenant APIs where each tenant has its own quota.
func TenantKeyFunc(headerName string) KeyFunc {
	return func(r *http.Request) string {
		tenant := r.Header.Get(headerName)
		if tenant == "" {
			return ""
		}
		return fmt.Sprintf("tenant:%s:%s", tenant, r.URL.Path)
	}
}

// Limiter is a concurrent-safe in-process rate limiter.
type Limiter struct {
	mu       sync.Mutex
	buckets  map[string]*Entry
	limit    Limit
	strategy Strategy
	keyFn    KeyFunc
	now      func() time.Time // for testing
}

// Option configures a Limiter.
type Option func(*Limiter)

func WithStrategy(s Strategy) Option {
	return func(l *Limiter) { l.strategy = s }
}

func WithKeyFunc(fn KeyFunc) Option {
	return func(l *Limiter) { l.keyFn = fn }
}

func WithClock(fn func() time.Time) Option {
	return func(l *Limiter) { l.now = fn }
}

// New creates a Limiter with the given base limit and options.
func New(limit Limit, opts ...Option) *Limiter {
	l := &Limiter{
		buckets:  make(map[string]*Entry),
		limit:    limit,
		strategy: StrategyTokenBucket,
		keyFn:    IPKeyFunc,
		now:      time.Now,
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Allow reports whether the request identified by key should be permitted.
func (l *Limiter) Allow(key string) (bool, *Entry) {
	if key == "" {
		return true, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	e, ok := l.buckets[key]
	if !ok {
		e = &Entry{
			Tokens:      float64(l.limit.effectiveBurst()),
			LastSeen:    now,
			WindowStart: now,
		}
		l.buckets[key] = e
	}

	switch l.strategy {
	case StrategyTokenBucket:
		return l.allowTokenBucket(e, now), e
	case StrategyFixedWindow:
		return l.allowFixedWindow(e, now), e
	default:
		return l.allowTokenBucket(e, now), e
	}
}

func (l *Limiter) allowTokenBucket(e *Entry, now time.Time) bool {
	elapsed := now.Sub(e.LastSeen).Seconds()
	rate := float64(l.limit.Requests) / l.limit.Window.Seconds()
	e.Tokens += elapsed * rate
	cap := float64(l.limit.effectiveBurst())
	if e.Tokens > cap {
		e.Tokens = cap
	}
	e.LastSeen = now
	e.HitCount++

	if e.Tokens < 1 {
		return false
	}
	e.Tokens--
	return true
}

func (l *Limiter) allowFixedWindow(e *Entry, now time.Time) bool {
	if now.Sub(e.WindowStart) >= l.limit.Window {
		e.WindowStart = now
		e.HitCount = 0
	}
	e.HitCount++
	e.LastSeen = now
	return e.HitCount <= int64(l.limit.Requests)
}

// Middleware returns an http.Handler that enforces the rate limit.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := l.keyFn(r)
		allowed, entry := l.Allow(key)

		if entry != nil {
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(l.limit.Requests))
			remaining := int(entry.Tokens)
			if remaining < 0 {
				remaining = 0
			}
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
			reset := entry.WindowStart.Add(l.limit.Window).Unix()
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset, 10))
		}

		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(l.limit.Window.Seconds())))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Cleanup removes entries that have not been seen within the given TTL.
// Call this periodically to prevent unbounded memory growth.
func (l *Limiter) Cleanup(ttl time.Duration) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := l.now().Add(-ttl)
	removed := 0
	for key, e := range l.buckets {
		if e.LastSeen.Before(cutoff) {
			delete(l.buckets, key)
			removed++
		}
	}
	return removed
}

// RunCleanupLoop starts a background goroutine that periodically evicts stale
// entries. It stops when the context is canceled.
func (l *Limiter) RunCleanupLoop(ctx context.Context, interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.Cleanup(ttl)
		}
	}
}

// Stats returns a snapshot of the limiter's current state.
type Stats struct {
	ActiveBuckets int
	TotalHits     int64
}

func (l *Limiter) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()

	var total int64
	for _, e := range l.buckets {
		total += e.HitCount
	}
	return Stats{
		ActiveBuckets: len(l.buckets),
		TotalHits:     total,
	}
}

// MultiLimiter chains several limiters together. A request is only allowed if
// every limiter in the chain permits it, which lets callers compose per-IP and
// per-tenant limits on the same endpoint.
type MultiLimiter struct {
	limiters []*Limiter
}

func NewMultiLimiter(limiters ...*Limiter) *MultiLimiter {
	return &MultiLimiter{limiters: limiters}
}

func (ml *MultiLimiter) Allow(r *http.Request) bool {
	for _, l := range ml.limiters {
		key := l.keyFn(r)
		allowed, _ := l.Allow(key)
		if !allowed {
			return false
		}
	}
	return true
}

func (ml *MultiLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ml.Allow(r) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SlidingWindowCounter uses a weighted sliding window for smoother limiting
// at window boundaries. It weights the previous window's count by how far
// into the current window the request arrives.
type SlidingWindowCounter struct {
	mu          sync.Mutex
	windows     map[string]*windowPair
	maxRequests int
	windowSize  time.Duration
}

type windowPair struct {
	PrevCount int
	CurrCount int
	CurrStart time.Time
}

func NewSlidingWindowCounter(maxRequests int, windowSize time.Duration) *SlidingWindowCounter {
	return &SlidingWindowCounter{
		windows:     make(map[string]*windowPair),
		maxRequests: maxRequests,
		windowSize:  windowSize,
	}
}

func (sw *SlidingWindowCounter) Allow(key string) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	wp, ok := sw.windows[key]
	if !ok {
		sw.windows[key] = &windowPair{CurrCount: 1, CurrStart: now}
		return true
	}
	if now.Sub(wp.CurrStart) >= sw.windowSize {
		wp.PrevCount = wp.CurrCount
		wp.CurrCount = 0
		wp.CurrStart = now
	}
	weight := 1.0 - now.Sub(wp.CurrStart).Seconds()/sw.windowSize.Seconds()
	if weight < 0 {
		weight = 0
	}
	estimate := float64(wp.PrevCount)*weight + float64(wp.CurrCount)
	if estimate >= float64(sw.maxRequests) {
		return false
	}
	wp.CurrCount++
	return true
}
