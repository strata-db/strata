package etcd_test

import (
	"context"
	"net"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
)

// TestServerAcceptsFrequentClientPings is a regression for the
// "GOAWAY too_many_pings" disconnect that bench/k0s saw against standalone t4.
//
// gRPC-Go's default ServerKeepaliveEnforcementPolicy.MinTime is 5 minutes
// with PermitWithoutStream=false. The etcd v3 client (used by kube-apiserver
// via k0s externalCluster) sends keepalive pings every 30s with
// PermitWithoutStream=true. Under that default the server tags the pings as
// too frequent (two ping strikes during a quiet period → GOAWAY), kills the
// HTTP/2 connection, and any in-flight RPCs fail with Unavailable. Watches
// reconnect transparently — the symptom is that watch_errors stays at 0
// while write/churn fails at high rate.
//
// To trigger strikes the test opens a Watch stream and stays idle long
// enough for several client pings to fire. With the fix (MinTime=5s,
// PermitWithoutStream=true) the server accepts the etcd-style 10s pings;
// without it the server kicks the connection after two strikes and the
// follow-up Put never produces a watch event on the dead stream.
func TestServerAcceptsFrequentClientPings(t *testing.T) {
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	defer func() { _ = node.Close() }()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	// gRPC-Go silently clamps client Time to a 10s minimum, so that's the
	// fastest we can ping. With server MinTime=5s (fix) the server accepts
	// these pings; with the default 5-min MinTime it strikes them out.
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	wc := etcdserverpb.NewWatchClient(conn)
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := wc.Watch(streamCtx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if err := stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:      []byte("/idle/"),
				RangeEnd: []byte("/idle0"),
			},
		},
	}); err != nil {
		t.Fatalf("Watch.Send create: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Watch.Recv created: %v", err)
	}

	// One reader for the lifetime of the stream — multiple concurrent Recvs
	// on a server-stream are unsafe.
	type recvResult struct {
		resp *etcdserverpb.WatchResponse
		err  error
	}
	results := make(chan recvResult, 4)
	go func() {
		for {
			resp, err := stream.Recv()
			results <- recvResult{resp: resp, err: err}
			if err != nil {
				return
			}
		}
	}()

	// Stay idle for 35s — long enough for at least three ~10s pings.
	// Without the fix that's three strikes and a forced GOAWAY before the
	// Put fires. The Recv goroutine surfaces any premature error.
	select {
	case r := <-results:
		t.Fatalf("idle Watch.Recv returned early: err=%v resp=%v (server likely sent GOAWAY)", r.err, r.resp)
	case <-time.After(35 * time.Second):
	}

	// Stream should still be alive — issue a Put on the same conn and
	// confirm the watch delivers it.
	kv := etcdserverpb.NewKVClient(conn)
	putCtx, putCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer putCancel()
	if _, err := kv.Put(putCtx, &etcdserverpb.PutRequest{
		Key:   []byte("/idle/key"),
		Value: []byte("v"),
	}); err != nil {
		t.Fatalf("Put after idle: %v", err)
	}

	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("Watch.Recv after Put: %v", r.err)
		}
		if len(r.resp.Events) == 0 {
			t.Fatalf("Watch.Recv after Put: no events (resp=%+v)", r.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch event not delivered after Put — connection likely dead")
	}
}
