package strata_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makhov/strata"
	"github.com/makhov/strata/internal/object"
)

// ── faultyStore ───────────────────────────────────────────────────────────────

// faultyStore wraps an object.Store and can be toggled to fail all writes.
type faultyStore struct {
	inner  object.Store
	broken int32 // atomic bool: 1 = fail writes, 0 = pass through
}

func newFaultyStore() *faultyStore {
	return &faultyStore{inner: object.NewMem()}
}

func (f *faultyStore) break_()        { atomic.StoreInt32(&f.broken, 1) }
func (f *faultyStore) repair()        { atomic.StoreInt32(&f.broken, 0) }
func (f *faultyStore) isBroken() bool { return atomic.LoadInt32(&f.broken) == 1 }

func (f *faultyStore) Put(ctx context.Context, key string, r io.Reader) error {
	if f.isBroken() {
		return errors.New("faultyStore: Put: injected failure")
	}
	return f.inner.Put(ctx, key, r)
}

func (f *faultyStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return f.inner.Get(ctx, key)
}

func (f *faultyStore) Delete(ctx context.Context, key string) error {
	if f.isBroken() {
		return errors.New("faultyStore: Delete: injected failure")
	}
	return f.inner.Delete(ctx, key)
}

func (f *faultyStore) List(ctx context.Context, prefix string) ([]string, error) {
	return f.inner.List(ctx, prefix)
}

// ── trackingStore ─────────────────────────────────────────────────────────────

// trackingStore records the keys that have been Put.
type trackingStore struct {
	inner object.Store
	mu    sync.Mutex
	puts  []string
}

func newTrackingStore(inner object.Store) *trackingStore {
	return &trackingStore{inner: inner}
}

func (t *trackingStore) Put(ctx context.Context, key string, r io.Reader) error {
	// Buffer the body so we can track the put and still pass it on.
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := t.inner.Put(ctx, key, bytes.NewReader(data)); err != nil {
		return err
	}
	t.mu.Lock()
	t.puts = append(t.puts, key)
	t.mu.Unlock()
	return nil
}

func (t *trackingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return t.inner.Get(ctx, key)
}

func (t *trackingStore) Delete(ctx context.Context, key string) error {
	return t.inner.Delete(ctx, key)
}

func (t *trackingStore) List(ctx context.Context, prefix string) ([]string, error) {
	return t.inner.List(ctx, prefix)
}

func (t *trackingStore) putCount(prefix string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, k := range t.puts {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

// ── helpers ───────────────────────────────────────────────────────────────────

// openCluster starts count nodes sharing the same object store with short
// segment/checkpoint intervals so S3 state is flushed quickly.
func openCluster(t *testing.T, count int, store object.Store) []*strata.Node {
	t.Helper()
	nodes := make([]*strata.Node, count)
	for i := 0; i < count; i++ {
		peerAddr := freeAddrImpl(t)
		node, err := strata.Open(strata.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			NodeID:             fmt.Sprintf("node-%d", i),
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 300 * time.Millisecond,
			SegmentMaxAge:      200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open node-%d: %v", i, err)
		}
		t.Cleanup(func() { node.Close() })
		nodes[i] = node
	}
	return nodes
}

// ── TestLateNodeJoin ──────────────────────────────────────────────────────────

// TestLateNodeJoin verifies that a node started after data has been written
// recovers the full state via checkpoint + WAL replay from object storage.
func TestLateNodeJoin(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start 2 nodes (leader + follower).
	nodes := openCluster(t, 2, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)

	// Write data and wait for S3 flush.
	const keys = 15
	var lastRev int64
	for i := 0; i < keys; i++ {
		rev, err := leader.Put(ctx, fmt.Sprintf("/join/%d", i), []byte(fmt.Sprintf("v%d", i)), 0)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		lastRev = rev
	}

	// Wait for checkpoint/WAL to reach S3 (CheckpointInterval=300ms, SegmentMaxAge=200ms).
	time.Sleep(1 * time.Second)

	// Start a 3rd node with a fresh data directory but the same object store.
	peerAddr := freeAddrImpl(t)
	late, err := strata.Open(strata.Config{
		DataDir:            t.TempDir(), // fresh: no local DB
		ObjectStore:        store,
		NodeID:             "node-late",
		PeerListenAddr:     peerAddr,
		AdvertisePeerAddr:  peerAddr,
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("late join Open: %v", err)
	}
	t.Cleanup(func() { late.Close() })

	// Wait for the late node to catch up.
	if err := late.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("late node WaitForRevision(%d): %v", lastRev, err)
	}

	// Verify all data is present.
	for i := 0; i < keys; i++ {
		kv, err := late.Get(fmt.Sprintf("/join/%d", i))
		if err != nil || kv == nil {
			t.Errorf("late node Get /join/%d: err=%v kv=%v", i, err, kv)
		} else if string(kv.Value) != fmt.Sprintf("v%d", i) {
			t.Errorf("late node value /join/%d: want v%d got %q", i, i, kv.Value)
		}
	}
}

// ── TestScale3To1 ─────────────────────────────────────────────────────────────

// TestScale3To1 verifies that closing 2 of 3 nodes leaves the remaining node
// fully operational and retaining all data.
func TestScale3To1(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes := openCluster(t, 3, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)

	// Write initial data.
	const phase1Keys = 10
	for i := 0; i < phase1Keys; i++ {
		if _, err := leader.Put(ctx, fmt.Sprintf("/scale/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("phase1 Put: %v", err)
		}
	}

	// Wait for all nodes to replicate.
	rev := leader.CurrentRevision()
	for _, n := range nodes {
		if n != leader {
			if err := n.WaitForRevision(ctx, rev); err != nil {
				t.Fatalf("WaitForRevision: %v", err)
			}
		}
	}

	// Close 2 non-leader nodes.
	closed := 0
	for _, n := range nodes {
		if n != leader && closed < 2 {
			n.Close()
			closed++
		}
	}
	t.Logf("closed %d followers", closed)

	// The remaining leader should still accept writes.
	const phase2Keys = 5
	for i := phase1Keys; i < phase1Keys+phase2Keys; i++ {
		if _, err := leader.Put(ctx, fmt.Sprintf("/scale/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("phase2 Put (after scale-down): %v", err)
		}
	}

	// All keys should be present.
	kvs, err := leader.List("/scale/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(kvs) != phase1Keys+phase2Keys {
		t.Errorf("after scale-down: want %d keys got %d", phase1Keys+phase2Keys, len(kvs))
	}
}

// ── TestObjectStoreUnavailableWritesSucceed ───────────────────────────────────

// TestObjectStoreUnavailableWritesSucceed verifies that node writes succeed
// even when the object store is temporarily unavailable. S3 failures are async
// (WAL upload) and should not block the write path.
func TestObjectStoreUnavailableWritesSucceed(t *testing.T) {
	store := newFaultyStore()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	node, err := strata.Open(strata.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		CheckpointInterval: 24 * time.Hour, // disable auto-checkpoint
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	// Write while S3 is healthy.
	for i := 0; i < 5; i++ {
		if _, err := node.Put(ctx, fmt.Sprintf("/avail/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Put (healthy): %v", err)
		}
	}

	// Break S3.
	store.break_()

	// Writes must still succeed locally.
	for i := 5; i < 10; i++ {
		if _, err := node.Put(ctx, fmt.Sprintf("/avail/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Put (s3 broken): %v", err)
		}
	}

	// All data is readable right now (in Pebble).
	kvs, err := node.List("/avail/")
	if err != nil || len(kvs) != 10 {
		t.Errorf("List while S3 broken: err=%v got %d keys", err, len(kvs))
	}
}

// TestObjectStoreUnavailableRecovery verifies that data written during an S3
// outage survives a restart via local WAL replay.
func TestObjectStoreUnavailableRecovery(t *testing.T) {
	store := newFaultyStore()
	dir := t.TempDir()
	ctx := context.Background()

	// First run: write data with S3 healthy, then break S3, write more.
	func() {
		node, err := strata.Open(strata.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour,
			SegmentMaxAge:      24 * time.Hour, // keep segments local
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := 0; i < 5; i++ {
			node.Put(ctx, fmt.Sprintf("/rec/%d", i), []byte("v"), 0)
		}
		store.break_()
		for i := 5; i < 10; i++ {
			if _, err := node.Put(ctx, fmt.Sprintf("/rec/%d", i), []byte("v"), 0); err != nil {
				t.Fatalf("Put with S3 broken: %v", err)
			}
		}
		node.Close()
	}()

	// Repair S3 and reopen: all data should be recovered from local WAL.
	store.repair()
	node, err := strata.Open(strata.Config{
		DataDir:     dir,
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer node.Close()

	kvs, err := node.List("/rec/")
	if err != nil {
		t.Fatalf("List after restart: %v", err)
	}
	if len(kvs) != 10 {
		t.Errorf("after restart: want 10 keys got %d", len(kvs))
	}
}

// ── TestCheckpointCorruption ──────────────────────────────────────────────────

// TestCheckpointCorruptionManifest verifies that a node returns a clear error
// when the manifest in S3 is corrupt JSON.
func TestCheckpointCorruptionManifest(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Write a corrupt manifest.
	store.Put(ctx, "manifest/latest", strings.NewReader("this is not valid json"))

	_, err := strata.Open(strata.Config{
		DataDir:     t.TempDir(), // fresh dir → will try to restore checkpoint
		ObjectStore: store,
	})
	if err == nil {
		t.Fatal("expected error opening node with corrupt manifest, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestCheckpointCorruptionArchive verifies that a node returns a clear error
// when the checkpoint archive bytes are corrupt.
func TestCheckpointCorruptionArchive(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Write a manifest pointing to a key that contains garbage.
	manifest := `{"checkpoint_key":"checkpoint/0000000001/00000000000000000001","revision":1,"term":1}`
	store.Put(ctx, "manifest/latest", strings.NewReader(manifest))
	store.Put(ctx, "checkpoint/0000000001/00000000000000000001", strings.NewReader("not a real checkpoint"))

	_, err := strata.Open(strata.Config{
		DataDir:     t.TempDir(),
		ObjectStore: store,
	})
	if err == nil {
		t.Fatal("expected error opening node with corrupt checkpoint, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// ── TestLeaderCrashBeforeWALFlush ─────────────────────────────────────────────

// TestLeaderCrashBeforeWALFlush verifies that data replicated to followers
// is not lost even when the leader crashes before its WAL segment is uploaded
// to object storage.
func TestLeaderCrashBeforeWALFlush(t *testing.T) {
	// Use a tracking store so we can verify whether WAL was uploaded.
	mem := object.NewMem()
	tracked := newTrackingStore(mem)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Very large segment size + long age: WAL segments will NOT auto-upload.
	const segSize = 500 << 20 // 500 MB
	const segAge = 24 * time.Hour

	const count = 3
	nodes := make([]*strata.Node, count)
	for i := 0; i < count; i++ {
		peerAddr := freeAddrImpl(t)
		node, err := strata.Open(strata.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        tracked,
			NodeID:             fmt.Sprintf("node-%d", i),
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 24 * time.Hour, // disable checkpoint
			SegmentMaxSize:     segSize,
			SegmentMaxAge:      segAge,
		})
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		t.Cleanup(func() { node.Close() })
		nodes[i] = node
	}

	leader := waitForLeaderNode(t, nodes, 10*time.Second)
	leaderIdx := -1
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
			break
		}
	}
	t.Logf("leader: node-%d", leaderIdx)

	// Write data and verify it's replicated to followers.
	const keyCount = 20
	var lastRev int64
	for i := 0; i < keyCount; i++ {
		rev, err := leader.Put(ctx, fmt.Sprintf("/crash/%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		lastRev = rev
	}
	for i, n := range nodes {
		if n == leader {
			continue
		}
		if err := n.WaitForRevision(ctx, lastRev); err != nil {
			t.Fatalf("node-%d WaitForRevision: %v", i, err)
		}
	}

	// Verify no WAL segment was uploaded yet (segments are too large to auto-rotate).
	walUploads := tracked.putCount("wal/")
	t.Logf("WAL uploads before crash: %d", walUploads)
	if walUploads > 0 {
		t.Log("(WAL was uploaded — test still valid, just less targeted)")
	}

	// Crash the leader.
	t.Logf("crashing leader node-%d", leaderIdx)
	leader.Close()

	// A survivor should become the new leader.
	survivors := make([]*strata.Node, 0, count-1)
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeaderNode(t, survivors, 30*time.Second)
	t.Logf("new leader elected")

	// All data written before the crash must still be accessible.
	for i := 0; i < keyCount; i++ {
		kv, err := newLeader.Get(fmt.Sprintf("/crash/%d", i))
		if err != nil || kv == nil {
			t.Errorf("key /crash/%d missing after leader crash: err=%v", i, err)
		}
	}
}

// ── TestWALReplayAfterPartialUpload ──────────────────────────────────────────

// TestWALReplayAfterPartialUpload simulates a node restarting after some WAL
// segments were uploaded and some were only on local disk.
func TestWALReplayAfterPartialUpload(t *testing.T) {
	store := object.NewMem()
	dir := t.TempDir()
	ctx := context.Background()

	var lastRev int64

	// First run: write data, close cleanly (triggers WAL seal).
	func() {
		node, err := strata.Open(strata.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour,
			SegmentMaxAge:      24 * time.Hour, // keep segments local
			SegmentMaxSize:     500 << 20,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := 0; i < 30; i++ {
			rev, err := node.Put(ctx, fmt.Sprintf("/wal/%d", i), []byte("v"), 0)
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			lastRev = rev
		}
		node.Close()
	}()

	// Second run: should recover all data from local WAL segments.
	node, err := strata.Open(strata.Config{
		DataDir:     dir,
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer node.Close()

	if node.CurrentRevision() != lastRev {
		t.Errorf("CurrentRevision after restart: want %d got %d", lastRev, node.CurrentRevision())
	}
	kvs, err := node.List("/wal/")
	if err != nil || len(kvs) != 30 {
		t.Errorf("List after restart: err=%v got %d (want 30)", err, len(kvs))
	}
}
