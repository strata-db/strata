// Package election implements S3-based leader election with a TTL lease.
//
// The protocol is optimistic:
//  1. Read the current lock object.
//  2. If absent or expired, write our record.
//  3. Wait briefly, then read back to verify nobody else won the race.
//
// This is safe for cases where the lock TTL is much larger than the S3 round-
// trip time and clock drift between nodes is bounded. Nodes that lose the race
// will read a different NodeID and back off.
package election

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/makhov/strata/internal/object"
)

// LockKey is the fixed object-storage key for the leader lock.
const LockKey = "leader-lock"

// LockRecord is the content of the leader-lock object.
type LockRecord struct {
	NodeID     string    `json:"node_id"`
	Term       uint64    `json:"term"`
	LeaderAddr string    `json:"leader_addr"` // follower peer-stream address
	ExpiresAt  time.Time `json:"expires_at"`
}

// IsExpired reports whether the lease has passed its expiry.
func (r *LockRecord) IsExpired() bool { return time.Now().After(r.ExpiresAt) }

// Lock manages leader election via a single S3 object.
type Lock struct {
	store         object.Store
	nodeID        string
	advertiseAddr string
	ttl           time.Duration
}

// NewLock creates a Lock.
//   - advertiseAddr is the address followers use to reach this node's peer stream.
//   - ttl is the lease duration; the leader must renew before it expires.
func NewLock(store object.Store, nodeID, advertiseAddr string, ttl time.Duration) *Lock {
	return &Lock{
		store:         store,
		nodeID:        nodeID,
		advertiseAddr: advertiseAddr,
		ttl:           ttl,
	}
}

// TryAcquire attempts to acquire the leader lock.
//
// floorTerm ensures the new term is always strictly greater than any previously
// observed term, preventing term regression after a graceful shutdown.
//
// Returns:
//   - (record, true, nil) if this node acquired the lock.
//   - (existing, false, nil) if another healthy node holds it.
//   - (nil, false, err) on storage error.
func (l *Lock) TryAcquire(ctx context.Context, floorTerm uint64) (*LockRecord, bool, error) {
	existing, err := l.Read(ctx)
	if err != nil {
		return nil, false, err
	}

	// If another node holds a valid (unexpired) lease, we cannot acquire.
	if existing != nil && existing.NodeID != l.nodeID && !existing.IsExpired() {
		return existing, false, nil
	}

	// Determine the new term: strictly greater than both floorTerm and any
	// existing term to prevent term regression.
	newTerm := floorTerm + 1
	if existing != nil && existing.Term >= newTerm {
		newTerm = existing.Term + 1
	}

	rec := &LockRecord{
		NodeID:     l.nodeID,
		Term:       newTerm,
		LeaderAddr: l.advertiseAddr,
		ExpiresAt:  time.Now().Add(l.ttl),
	}
	if err := l.write(ctx, rec); err != nil {
		return nil, false, err
	}

	// Wait briefly then read back — this narrows (but does not eliminate) the
	// race window with another concurrent candidate.
	time.Sleep(100 * time.Millisecond)
	verify, err := l.Read(ctx)
	if err != nil {
		return nil, false, err
	}
	if verify == nil || verify.NodeID != l.nodeID || verify.Term != newTerm {
		return verify, false, nil // someone else won
	}
	return rec, true, nil
}

// Renew extends the lease for an already-held lock. Returns an error if the
// lock has been taken by another node (leader should step down).
func (l *Lock) Renew(ctx context.Context, term uint64) error {
	existing, err := l.Read(ctx)
	if err != nil {
		return err
	}
	if existing == nil || existing.NodeID != l.nodeID || existing.Term != term {
		return fmt.Errorf("election: lock stolen (current: %+v)", existing)
	}
	existing.ExpiresAt = time.Now().Add(l.ttl)
	return l.write(ctx, existing)
}

// Release deletes the lock. Safe to call if the lock is not held.
func (l *Lock) Release(ctx context.Context) error {
	return l.store.Delete(ctx, LockKey)
}

// Read returns the current lock record, or nil if none exists.
func (l *Lock) Read(ctx context.Context) (*LockRecord, error) {
	rc, err := l.store.Get(ctx, LockKey)
	if err == object.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("election: read lock: %w", err)
	}
	defer rc.Close()
	var rec LockRecord
	if err := json.NewDecoder(rc).Decode(&rec); err != nil {
		return nil, fmt.Errorf("election: decode lock: %w", err)
	}
	return &rec, nil
}

func (l *Lock) write(ctx context.Context, rec *LockRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := l.store.Put(ctx, LockKey, bytes.NewReader(b)); err != nil {
		return fmt.Errorf("election: write lock: %w", err)
	}
	return nil
}
