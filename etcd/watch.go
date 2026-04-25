package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/t4db/t4"
)

// Watch implements WatchServer.Watch (bidirectional streaming).
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()

	// All sends are serialized through sendCh to avoid concurrent SendMsg calls.
	sendCh := make(chan *etcdserverpb.WatchResponse, 128)
	go func() {
		for {
			select {
			case resp := <-sendCh:
				_ = stream.Send(resp)
			case <-ctx.Done():
				return
			}
		}
	}()

	type entry struct{ cancel context.CancelFunc }
	var watchesMu sync.Mutex
	watches := map[int64]entry{}
	var nextID int64 = 1
	watchIDs := func() []int64 {
		watchesMu.Lock()
		ids := make([]int64, 0, len(watches))
		for id := range watches {
			ids = append(ids, id)
		}
		watchesMu.Unlock()
		return ids
	}
	removeWatch := func(id int64) (context.CancelFunc, bool) {
		watchesMu.Lock()
		w, ok := watches[id]
		if ok {
			delete(watches, id)
		}
		watchesMu.Unlock()
		if !ok {
			return nil, false
		}
		return w.cancel, true
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}

		switch v := req.RequestUnion.(type) {
		case *etcdserverpb.WatchRequest_CreateRequest:
			cr := v.CreateRequest
			if isInternalKey(string(cr.Key)) {
				select {
				case sendCh <- &etcdserverpb.WatchResponse{
					Header:       s.header(),
					WatchId:      -1,
					Canceled:     true,
					CancelReason: "reserved internal prefix is not watchable",
				}:
				case <-ctx.Done():
					goto done
				}
				continue
			}
			id := nextID
			nextID++

			wctx, cancel := context.WithCancel(ctx)
			watchesMu.Lock()
			watches[id] = entry{cancel}
			watchesMu.Unlock()

			// Confirm the watch was created.
			select {
			case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Created: true}:
			case <-ctx.Done():
				cancel()
				goto done
			}

			go func(watchID int64, startRev int64) {
				scanPrefix, match := watchScan(cr)
				events, err := s.node.Watch(wctx, scanPrefix, fromEtcdRevision(startRev), cr.PrevKv)
				if errors.Is(err, t4.ErrCompacted) {
					// Remove the watch first, but do not cancel wctx before sending
					// the compacted response: that races the select below and can
					// drop the required canceled notification.
					_, _ = removeWatch(watchID)
					select {
					case sendCh <- &etcdserverpb.WatchResponse{
						Header:          s.header(),
						WatchId:         watchID,
						Canceled:        true,
						CancelReason:    "mvcc: required revision has been compacted",
						CompactRevision: toEtcdRevision(s.node.CompactRevision()),
					}:
					case <-ctx.Done():
					}
					return
				}
				if err != nil {
					return
				}

				var progressC <-chan time.Time
				var progressTicker *time.Ticker
				if cr.ProgressNotify {
					progressTicker = time.NewTicker(time.Second)
					progressC = progressTicker.C
					defer progressTicker.Stop()
				}

				// Coalesce close-arriving events into a single WatchResponse.
				// Real etcd batches events on the wire; per-event Send is the
				// dominant cost under high churn. maxBatch caps memory of a
				// single response and matches a typical apiserver watch frame.
				const maxBatch = 256
				batch := make([]*mvccpb.Event, 0, maxBatch)
				flush := func() bool {
					if len(batch) == 0 {
						return true
					}
					resp := &etcdserverpb.WatchResponse{
						Header:  s.header(),
						WatchId: watchID,
						Events:  batch,
					}
					batch = make([]*mvccpb.Event, 0, maxBatch)
					select {
					case sendCh <- resp:
						return true
					case <-wctx.Done():
						return false
					}
				}
				appendEvent := func(e t4.Event) bool {
					if !match(e.KV.Key) {
						return true
					}
					e, ok := userEvent(e)
					if !ok {
						return true
					}
					batch = append(batch, eventToProto(e))
					return true
				}

				for {
					select {
					case e, ok := <-events:
						if !ok {
							flush()
							return
						}
						if !appendEvent(e) {
							return
						}
						// Drain any other ready events without blocking so a
						// burst from scanLog ships in one frame.
					drain:
						for len(batch) < maxBatch {
							select {
							case e2, ok2 := <-events:
								if !ok2 {
									flush()
									return
								}
								if !appendEvent(e2) {
									return
								}
							default:
								break drain
							}
						}
						if !flush() {
							return
						}
					case <-progressC:
						if !flush() {
							return
						}
						select {
						case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: watchID}:
						case <-wctx.Done():
							return
						}
					case <-wctx.Done():
						return
					}
				}
			}(id, cr.StartRevision)

		case *etcdserverpb.WatchRequest_CancelRequest:
			id := v.CancelRequest.WatchId
			if cancel, ok := removeWatch(id); ok {
				cancel()
				select {
				case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id, Canceled: true}:
				case <-ctx.Done():
					goto done
				}
			}
		case *etcdserverpb.WatchRequest_ProgressRequest:
			for _, id := range watchIDs() {
				select {
				case sendCh <- &etcdserverpb.WatchResponse{Header: s.header(), WatchId: id}:
				case <-ctx.Done():
					goto done
				}
			}
		}
	}

done:
	watchesMu.Lock()
	all := make([]entry, 0, len(watches))
	for _, w := range watches {
		all = append(all, w)
	}
	watches = map[int64]entry{}
	watchesMu.Unlock()
	for _, w := range all {
		w.cancel()
	}
	return nil
}

func watchScan(cr *etcdserverpb.WatchCreateRequest) (string, func(string) bool) {
	key := string(cr.Key)
	end := string(cr.RangeEnd)
	if end == "" {
		return key, func(candidate string) bool { return candidate == key }
	}
	match := func(candidate string) bool {
		if end == "\x00" {
			return candidate >= key
		}
		return candidate >= key && candidate < end
	}
	if isPrefixRangeEnd(key, end) {
		return key, match
	}
	return "", match
}

func isPrefixRangeEnd(prefix, end string) bool {
	return prefixRangeEnd(prefix) == end
}

func prefixRangeEnd(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "\x00"
}
