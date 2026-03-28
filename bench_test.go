package strata_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/makhov/strata"
)

func openBenchNode(b *testing.B) *strata.Node {
	b.Helper()
	n, err := strata.Open(strata.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { n.Close() })
	return n
}

func BenchmarkPut(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Put(ctx, fmt.Sprintf("/bench/put/%d", i), []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSameKey(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Put(ctx, "/bench/same", []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	if _, err := n.Put(ctx, "/bench/get", []byte("value"), 0); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Get("/bench/get"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreate(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Create(ctx, fmt.Sprintf("/bench/create/%d", i), []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	rev, err := n.Put(ctx, "/bench/update", []byte("v0"), 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newRev, _, _, err := n.Update(ctx, "/bench/update", []byte("v"), rev, 0)
		if err != nil {
			b.Fatal(err)
		}
		rev = newRev
	}
}

func BenchmarkDelete(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("/bench/del/%d", i)
		if _, err := n.Put(ctx, key, []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := n.Delete(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := n.Put(ctx, fmt.Sprintf("/bench/list/%04d", i), []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.List("/bench/list/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutParallel measures Put throughput with concurrent writers.
// Group-commit batches concurrent writes into a single WAL fsync, so this
// benchmark is where the Option-A improvement shows up.
func BenchmarkPutParallel(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			if _, err := n.Put(ctx, fmt.Sprintf("/bench/par/%d", i), []byte("value"), 0); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkPutParallelSingleProc verifies that group-commit batching works even
// without true CPU parallelism. It pins GOMAXPROCS=1 so goroutines are
// cooperatively scheduled, but still spawns 16 concurrent writers. Because each
// writer unlocks n.mu before blocking on the done channel, all 16 can queue
// their requests before the commit loop drains writeC — producing batches of
// ~16 writes per fsync even on one OS thread.
func BenchmarkPutParallelSingleProc(b *testing.B) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)

	n := openBenchNode(b)
	ctx := context.Background()

	const writers = 16
	var (
		counter atomic.Int64
		wg      sync.WaitGroup
		work    = make(chan struct{}, b.N)
	)
	for i := 0; i < b.N; i++ {
		work <- struct{}{}
	}
	close(work)

	b.ResetTimer()
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for range work {
				i := counter.Add(1)
				if _, err := n.Put(ctx, fmt.Sprintf("/bench/singleproc/%d", i), []byte("value"), 0); err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkWatch(b *testing.B) {
	n := openBenchNode(b)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	ch, err := n.Watch(ctx, "/bench/watch/", 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.Put(ctx, fmt.Sprintf("/bench/watch/%d", i), []byte("v"), 0)
		<-ch
	}
}
