// Package election implements S3-based leader election.
//
// The protocol is optimistic:
//  1. Read the current lock object.
//  2. If absent or owned by us, write our record.
//  3. Wait briefly, then read back to verify nobody else won the race.
//
// There is no TTL on the lock. Liveness is detected via the WAL stream
// (followers attempt a TakeOver after the stream becomes unreachable).
// Leaders do an infrequent read-only watch to detect if they have been
// superseded and step down gracefully.
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
	NodeID     string `json:"node_id"`
	Term       uint64 `json:"term"`
	LeaderAddr string `json:"leader_addr"` // follower peer-stream address
}

// Lock manages leader election via a single S3 object.
type Lock struct {
	store         object.Store
	nodeID        string
	advertiseAddr string
}

// NewLock creates a Lock.
// advertiseAddr is the address followers use to reach this node's peer stream.
func NewLock(store object.Store, nodeID, advertiseAddr string) *Lock {
	return &Lock{
		store:         store,
		nodeID:        nodeID,
		advertiseAddr: advertiseAddr,
	}
}

// TryAcquire attempts to acquire the leader lock at startup.
//
// It writes only if the lock is absent or already owned by this node.
// If another node holds the lock, it returns (existing, false, nil) so the
// caller can become a follower of that node.
//
// floorTerm ensures the new term is always strictly greater than any
// previously observed term, preventing term regression after restart.
func (l *Lock) TryAcquire(ctx context.Context, floorTerm uint64) (*LockRecord, bool, error) {
	existing, err := l.Read(ctx)
	if err != nil {
		return nil, false, err
	}

	// Another node holds the lock — become a follower.
	if existing != nil && existing.NodeID != l.nodeID {
		return existing, false, nil
	}

	newTerm := floorTerm + 1
	if existing != nil && existing.Term >= newTerm {
		newTerm = existing.Term + 1
	}

	return l.writeAndVerify(ctx, newTerm)
}

// TakeOver forcefully attempts to acquire the lock, overwriting any existing
// owner. Called by a follower after it has determined the leader is
// unreachable. Uses the same optimistic read-back to resolve races between
// concurrent candidates.
func (l *Lock) TakeOver(ctx context.Context, floorTerm uint64) (*LockRecord, bool, error) {
	existing, err := l.Read(ctx)
	if err != nil {
		return nil, false, err
	}

	newTerm := floorTerm + 1
	if existing != nil && existing.Term >= newTerm {
		newTerm = existing.Term + 1
	}

	return l.writeAndVerify(ctx, newTerm)
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

// writeAndVerify writes a new lock record for this node with the given term,
// waits briefly, then reads back to confirm this node won.
func (l *Lock) writeAndVerify(ctx context.Context, newTerm uint64) (*LockRecord, bool, error) {
	rec := &LockRecord{
		NodeID:     l.nodeID,
		Term:       newTerm,
		LeaderAddr: l.advertiseAddr,
	}
	if err := l.write(ctx, rec); err != nil {
		return nil, false, err
	}

	// Wait briefly then read back — narrows (but does not eliminate) the
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
