package election

import (
	"context"
	"testing"
	"time"

	"github.com/makhov/strata/internal/object"
)

func newLock(t *testing.T, nodeID, addr string, ttl time.Duration) *Lock {
	t.Helper()
	return NewLock(object.NewMem(), nodeID, addr, ttl)
}

func newLockShared(store *object.Mem, nodeID, addr string, ttl time.Duration) *Lock {
	return NewLock(store, nodeID, addr, ttl)
}

func TestTryAcquireEmpty(t *testing.T) {
	l := newLock(t, "node-1", "localhost:2380", 30*time.Second)
	rec, won, err := l.TryAcquire(context.Background(), 0)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !won {
		t.Error("expected to win election on empty store")
	}
	if rec.NodeID != "node-1" {
		t.Errorf("NodeID: want node-1 got %q", rec.NodeID)
	}
	if rec.Term != 1 {
		t.Errorf("Term: want 1 got %d", rec.Term)
	}
}

func TestTryAcquireFloorTerm(t *testing.T) {
	l := newLock(t, "node-1", "localhost:2380", 30*time.Second)
	rec, won, err := l.TryAcquire(context.Background(), 5)
	if err != nil || !won {
		t.Fatalf("TryAcquire: won=%v err=%v", won, err)
	}
	if rec.Term != 6 {
		t.Errorf("Term: want 6 (floorTerm+1) got %d", rec.Term)
	}
}

func TestTwoNodeElection(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "localhost:2380", 30*time.Second)
	l2 := newLockShared(store, "node-2", "localhost:2381", 30*time.Second)

	_, won1, err := l1.TryAcquire(context.Background(), 0)
	if err != nil || !won1 {
		t.Fatalf("node-1 should win: won=%v err=%v", won1, err)
	}

	// node-2 tries while node-1 holds a valid lease → should lose.
	existing, won2, err := l2.TryAcquire(context.Background(), 0)
	if err != nil {
		t.Fatalf("node-2 TryAcquire: %v", err)
	}
	if won2 {
		t.Error("node-2 should not win while node-1 holds the lock")
	}
	if existing == nil || existing.NodeID != "node-1" {
		t.Errorf("existing lock should belong to node-1, got %+v", existing)
	}
}

func TestExpiredLockAllowsTakeover(t *testing.T) {
	store := object.NewMem()
	// node-1 acquires with a very short TTL.
	l1 := newLockShared(store, "node-1", "localhost:2380", 10*time.Millisecond)
	_, won1, _ := l1.TryAcquire(context.Background(), 0)
	if !won1 {
		t.Fatal("node-1 should win")
	}

	// Wait for lease to expire.
	time.Sleep(50 * time.Millisecond)

	// node-2 should now be able to acquire.
	l2 := newLockShared(store, "node-2", "localhost:2381", 30*time.Second)
	rec, won2, err := l2.TryAcquire(context.Background(), 0)
	if err != nil {
		t.Fatalf("node-2 TryAcquire: %v", err)
	}
	if !won2 {
		t.Errorf("node-2 should win after node-1 lease expires; existing=%+v", rec)
	}
	if won2 && rec.NodeID != "node-2" {
		t.Errorf("expected node-2 to hold the lock, got %q", rec.NodeID)
	}
}

func TestRenew(t *testing.T) {
	l := newLock(t, "node-1", "localhost:2380", 30*time.Second)
	rec, _, _ := l.TryAcquire(context.Background(), 0)

	before := rec.ExpiresAt
	time.Sleep(5 * time.Millisecond)

	if err := l.Renew(context.Background(), rec.Term); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	renewed, _ := l.Read(context.Background())
	if !renewed.ExpiresAt.After(before) {
		t.Error("ExpiresAt should advance after renewal")
	}
}

func TestRenewStolenLock(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "addr1", 10*time.Millisecond)
	l2 := newLockShared(store, "node-2", "addr2", 30*time.Second)

	rec, _, _ := l1.TryAcquire(context.Background(), 0)
	time.Sleep(50 * time.Millisecond) // let l1's lease expire
	l2.TryAcquire(context.Background(), 0)

	// l1 tries to renew but its lock was stolen.
	err := l1.Renew(context.Background(), rec.Term)
	if err == nil {
		t.Error("expected error renewing a stolen lock")
	}
}

func TestRelease(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "addr1", 30*time.Second)
	_, _, _ = l1.TryAcquire(context.Background(), 0)

	if err := l1.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// After release, the lock object should be gone.
	rec, err := l1.Read(context.Background())
	if err != nil {
		t.Fatalf("Read after release: %v", err)
	}
	if rec != nil {
		t.Errorf("expected nil record after release, got %+v", rec)
	}
}

func TestTermMonotonicity(t *testing.T) {
	store := object.NewMem()
	var lastTerm uint64
	for i := 0; i < 5; i++ {
		l := newLockShared(store, "node-1", "addr", 1*time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		rec, won, err := l.TryAcquire(context.Background(), lastTerm)
		if err != nil || !won {
			t.Fatalf("iter %d: TryAcquire won=%v err=%v", i, won, err)
		}
		if rec.Term <= lastTerm {
			t.Errorf("term not monotonic: %d -> %d", lastTerm, rec.Term)
		}
		lastTerm = rec.Term
	}
}
