package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// TokenStore manages short-lived bearer tokens in memory.
// Tokens are lost on restart — clients must re-authenticate.
type TokenStore struct {
	mu     sync.RWMutex
	tokens map[string]tokenEntry
	ttl    time.Duration
}

type tokenEntry struct {
	username string
	expiry   time.Time
}

// NewTokenStore creates a TokenStore with the given TTL and starts a background
// eviction goroutine that runs until ctx is cancelled.
func NewTokenStore(ctx context.Context, ttl time.Duration) *TokenStore {
	ts := &TokenStore{
		tokens: make(map[string]tokenEntry),
		ttl:    ttl,
	}
	go ts.evictLoop(ctx)
	return ts
}

// Generate mints a new token for username and returns it.
func (ts *TokenStore) Generate(username string) (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(raw)

	ts.mu.Lock()
	ts.tokens[tok] = tokenEntry{username: username, expiry: time.Now().Add(ts.ttl)}
	ts.mu.Unlock()

	return tok, nil
}

// Lookup returns the username associated with token, or ("", false) if the
// token is unknown or expired.
func (ts *TokenStore) Lookup(token string) (string, bool) {
	ts.mu.RLock()
	e, ok := ts.tokens[token]
	ts.mu.RUnlock()

	if !ok || time.Now().After(e.expiry) {
		return "", false
	}
	return e.username, true
}

// Revoke invalidates a token immediately.
func (ts *TokenStore) Revoke(token string) {
	ts.mu.Lock()
	delete(ts.tokens, token)
	ts.mu.Unlock()
}

func (ts *TokenStore) evictLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts.evict()
		}
	}
}

func (ts *TokenStore) evict() {
	now := time.Now()
	ts.mu.Lock()
	for tok, e := range ts.tokens {
		if now.After(e.expiry) {
			delete(ts.tokens, tok)
		}
	}
	ts.mu.Unlock()
}
