package auth

import (
	"sync"
	"time"
)

const (
	// maxFailures is the number of consecutive failures within failureWindow
	// that triggers a lockout.
	maxFailures = 5
	// failureWindow is the rolling window over which failures are counted.
	failureWindow = 5 * time.Minute
	// lockoutDuration is how long a username is blocked after hitting maxFailures.
	lockoutDuration = 15 * time.Minute
)

type authAttempt struct {
	failures    int
	firstFail   time.Time
	lockedUntil time.Time
}

// rateLimiter tracks per-username authentication failures and enforces a
// temporary lockout after repeated failures.  It is intentionally in-memory
// only: a restarted node presents an attacker with a fresh window, which is
// an acceptable trade-off compared to persisting lockout state.
type rateLimiter struct {
	mu       sync.Mutex
	attempts map[string]*authAttempt
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{attempts: make(map[string]*authAttempt)}
}

// IsLocked returns true when the username is currently locked out.
func (rl *rateLimiter) IsLocked(username string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	a, ok := rl.attempts[username]
	if !ok {
		return false
	}
	if time.Now().Before(a.lockedUntil) {
		return true
	}
	// Lockout expired — clear it so the next attempt starts fresh.
	if !a.lockedUntil.IsZero() {
		delete(rl.attempts, username)
	}
	return false
}

// RecordFailure increments the failure counter for username.  If the counter
// reaches maxFailures within the failureWindow the username is locked out.
func (rl *rateLimiter) RecordFailure(username string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	a, ok := rl.attempts[username]
	if !ok {
		rl.attempts[username] = &authAttempt{failures: 1, firstFail: time.Now()}
		return
	}
	// Reset the window if enough time has passed since the first failure.
	if time.Since(a.firstFail) > failureWindow {
		a.failures = 1
		a.firstFail = time.Now()
		a.lockedUntil = time.Time{}
		return
	}
	a.failures++
	if a.failures >= maxFailures {
		a.lockedUntil = time.Now().Add(lockoutDuration)
	}
}

// RecordSuccess clears any failure state for username.
func (rl *rateLimiter) RecordSuccess(username string) {
	rl.mu.Lock()
	delete(rl.attempts, username)
	rl.mu.Unlock()
}
