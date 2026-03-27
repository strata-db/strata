# Architecture

## Overview

```
                   ┌──────────────────────────────────────────────────┐
                   │                    Leader node                    │
 client writes ──► │  Node.Put/Create/… → WAL.Append → store.Apply   │
                   │                          │                        │
                   │                     Broadcast                     │
                   └──────────────────────────┬───────────────────────┘
                                              │  gRPC stream (port 2380)
                        ┌─────────────────────┴──────────────────────┐
                        ▼                                             ▼
                ┌───────────────┐                           ┌───────────────┐
                │  Follower B   │                           │  Follower C   │
                │  WAL.Apply    │                           │  WAL.Apply    │
                │  store.Apply  │                           │  store.Apply  │
                └───────────────┘                           └───────────────┘
                reads: local                                reads: local
                writes: forwarded ──────────────────────►  leader via gRPC

                   ┌──────────────┐
                   │  S3 bucket   │
                   │  leader-lock │  ◄── election / liveness
                   │  wal/…       │  ◄── durability / recovery
                   │  checkpoint/ │  ◄── fast startup
                   │  manifest/   │  ◄── single GET to locate latest state
                   └──────────────┘
```

---

## Storage layout

```
<data-dir>/
  db/          Pebble key-value database
  wal/         Local WAL segment files (*.wal, auto-deleted after S3 upload)

S3 bucket/<prefix>/
  manifest/latest                    JSON pointer to the latest checkpoint
  checkpoint/<term>/<revision>       Pebble snapshot archive
  wal/<term>/<first-revision>        Sealed WAL segment
  leader-lock                        JSON leader lease record
```

---

## Write-ahead log (WAL)

Every write goes through the WAL before it touches the database:

1. The leader assigns the next monotonic revision.
2. The entry (`{revision, op, key, value}`) is appended to the active `.wal` segment and fsynced.
3. The entry is broadcast to all connected followers over gRPC streams.
4. The entry is applied to Pebble.
5. The response is returned to the caller.

WAL segment rotation is triggered by size (default 50 MB) or age (default 10 s). When a segment is sealed it is queued for async upload to S3 and then deleted locally once the upload confirms.

### Segment naming

```
wal/<term>/<first-revision>
```

Both fields are zero-padded to fixed widths so that lexicographic order equals chronological order. This allows recovery to replay segments in the correct sequence with a single S3 list call.

---

## Checkpoints

A checkpoint is a point-in-time Pebble snapshot uploaded to S3. It allows new or recovering nodes to skip WAL replay from the beginning of time.

The checkpoint cycle (triggered by `CheckpointInterval`):

1. Seal and upload the current WAL segment.
2. Call `pebble.DB.Checkpoint` to capture a consistent snapshot.
3. Stream the snapshot files into a custom binary archive and upload it to S3.
4. Write `manifest/latest` pointing to the new checkpoint.
5. Delete S3 WAL segments whose last revision is older than the checkpoint.

The archive format is a simple binary stream: a 24-byte header (`magic + term + revision`) followed by file records (`nameLen + name + fileSize + content`). There is no compression or external dependency.

---

## Leader election

Election uses a last-writer-wins S3 object (`leader-lock`) rather than a consensus protocol or TTL polling.

**Acquiring the lock:**
1. Try to read `leader-lock`.
2. If absent (or from a previous term), write a lock record containing `{node_id, term, leader_addr}`.
3. Re-read the lock. If it still contains this node's record, the election is won.

**Liveness detection:**
- Followers detect a dead leader when the WAL gRPC stream fails `FollowerMaxRetries` consecutive times (each attempt has a ~2 s timeout).
- On failure, the follower increments the term and overwrites the lock. It re-reads to confirm it won, then starts serving as the new leader.

**Stepdown:**
- The old leader periodically re-reads the lock (`LeaderWatchInterval`, default 5 min).
- If the lock no longer points to this node, it steps down gracefully.

There is no heartbeat, no TTL, and no ZooKeeper-style session. The only S3 writes for election are at startup and on takeover. A node that is partitioned from S3 will keep serving reads and local writes until it detects the supersession.

---

## Follower replication

Followers connect to the leader's gRPC peer address and open a streaming RPC. The leader pushes WAL entries as they are committed. Each entry contains the revision, operation type, key, and value — enough to apply to the follower's local Pebble instance.

**Catch-up on connect:** when a follower connects, it sends its current revision. The leader replays from that revision using its in-memory ring buffer (`PeerBufferSize` entries, default 10 000). If the follower is too far behind, it must restart from S3.

**Write forwarding:** a client write arriving at a follower is forwarded to the leader via gRPC. The follower returns the leader's response (including the assigned revision) directly to the client.

---

## etcd v3 adapter

The `etcd/` package wraps `*strata.Node` with the etcd v3 gRPC server interfaces (`KVServer`, `WatchServer`, `LeaseServer`, `ClusterServer`, `MaintenanceServer`). The standalone binary registers this adapter and serves on the configured `--listen` address.

Mapping summary:

| etcd operation | Strata call |
|---|---|
| `Range` (single key) | `node.Get` |
| `Range` (prefix) | `node.List` filtered by `RangeEnd` |
| `Put` | `node.Put` |
| `DeleteRange` (single) | `node.Delete` |
| `Txn` (MOD==0 + Put) | `node.Create` |
| `Txn` (MOD==X + Put) | `node.Update` |
| `Txn` (MOD==X + Delete) | `node.DeleteIfRevision` |
| `Watch` | `node.Watch` |
| `Compact` | `node.Compact` |

Lease operations are stubbed (TTL=60, no eviction). Cluster operations return a single synthetic member. These are sufficient for all standard etcd v3 clients.

---

## Concurrency model

- **All writes serialised through a single mutex** (`node.mu`). This keeps the revision counter monotonic and makes WAL append + Pebble apply atomic from the node's perspective.
- **Reads are lock-free** — Pebble handles its own read concurrency.
- **Watchers** are registered in a fan-out broadcaster that sends events after each write. Each watcher runs in its own goroutine.
- **WAL background goroutines** (rotation loop, upload loop) share the WAL mutex with write operations but hold it only briefly during segment rotation.
- **Follower streams** run in dedicated goroutines per connected follower; the leader pushes entries from a channel filled under the write mutex.
