package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrMissingToken    = errors.New("authorization token is required")
	ErrInvalidToken    = errors.New("token signature verification failed")
	ErrExpiredToken    = errors.New("token has expired")
	ErrInsufficientACL = errors.New("insufficient permissions for this resource")
)

type contextKey string

const (
	userIDKey   contextKey = "user_id"
	tenantKey   contextKey = "tenant_id"
	scopesKey   contextKey = "scopes"
	sessionTTL             = 15 * time.Minute
	maxTokenLen            = 2048
)

type SessionInfo struct {
	UserID    string
	TenantID  string
	Scopes    []string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

type sessionCache struct {
	mu       sync.RWMutex
	entries  map[string]*SessionInfo
	evictAt  map[string]time.Time
}

func newSessionCache() *sessionCache {
	return &sessionCache{
		entries: make(map[string]*SessionInfo),
		evictAt: make(map[string]time.Time),
	}
}

func (sc *sessionCache) Get(token string) (*SessionInfo, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	info, ok := sc.entries[token]
	if !ok {
		return nil, false
	}
	if time.Now().After(sc.evictAt[token]) {
		return nil, false
	}
	return info, true
}

func (sc *sessionCache) Set(token string, info *SessionInfo) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.entries[token] = info
	sc.evictAt[token] = time.Now().Add(sessionTTL)
}

// Purge removes expired entries. Intended to be called periodically from a
// background goroutine rather than inline on every request.
func (sc *sessionCache) Purge() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	now := time.Now()
	removed := 0
	for tok, exp := range sc.evictAt {
		if now.After(exp) {
			delete(sc.entries, tok)
			delete(sc.evictAt, tok)
			removed++
		}
	}
	return removed
}

type AuthMiddleware struct {
	signingKey []byte
	cache      *sessionCache
	skipPaths  map[string]bool
}

func NewAuthMiddleware(signingKey string, skipPaths []string) *AuthMiddleware {
	sp := make(map[string]bool, len(skipPaths))
	for _, p := range skipPaths {
		sp[p] = true
	}
	return &AuthMiddleware{
		signingKey: []byte(signingKey),
		cache:      newSessionCache(),
		skipPaths:  sp,
	}
}

func (am *AuthMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if am.skipPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		token, err := extractBearerToken(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		session, err := am.validateToken(token)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), userIDKey, session.UserID)
		ctx = context.WithValue(ctx, tenantKey, session.TenantID)
		ctx = context.WithValue(ctx, scopesKey, session.Scopes)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (am *AuthMiddleware) validateToken(raw string) (*SessionInfo, error) {
	if cached, ok := am.cache.Get(raw); ok {
		if time.Now().Before(cached.ExpiresAt) {
			return cached, nil
		}
		return nil, ErrExpiredToken
	}

	parts := strings.SplitN(raw, ".", 3)
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	payload := parts[0] + "." + parts[1]
	sig, err := hex.DecodeString(parts[2])
	if err != nil {
		return nil, ErrInvalidToken
	}

	mac := hmac.New(sha256.New, am.signingKey)
	mac.Write([]byte(payload))
	if !hmac.Equal(mac.Sum(nil), sig) {
		return nil, ErrInvalidToken
	}

	info := &SessionInfo{
		UserID:    parts[0],
		TenantID:  parts[1],
		ExpiresAt: time.Now().Add(sessionTTL),
		IssuedAt:  time.Now(),
	}
	am.cache.Set(raw, info)
	return info, nil
}

func extractBearerToken(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", ErrMissingToken
	}
	if len(header) > maxTokenLen {
		return "", ErrInvalidToken
	}
	// Expect "Bearer <token>"
	if !strings.HasPrefix(header, "Bearer ") {
		return "", ErrInvalidToken
	}
	return strings.TrimPrefix(header, "Bearer "), nil
}

func UserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

func TenantIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(tenantKey).(string); ok {
		return v
	}
	return ""
}

func RequireScope(scope string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scopes, ok := r.Context().Value(scopesKey).([]string)
		if !ok {
			http.Error(w, ErrInsufficientACL.Error(), http.StatusForbidden)
			return
		}
		for _, s := range scopes {
			if s == scope || s == "admin" {
				next.ServeHTTP(w, r)
				return
			}
		}
		http.Error(w, ErrInsufficientACL.Error(), http.StatusForbidden)
	})
}
