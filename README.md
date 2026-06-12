# Zodiac

Distributed, strongly consistent key-value store built on a custom implementation of the [Raft](https://github.com/MHS-20/Raft) consensus protocol. This project was built mainly to support the [Poseidon](https://github.com/MHS-20/Poseidon)  project.

<div align="center">
<img src="zodiac.png" alt="Logo" width="300"/>
</div>

## Features

- **Linearizable operations** — every read and write goes through the Raft log, committed by a majority, and acknowledged to the client only after full application to the state machine.
- **Cluster-wide revision** — each mutation increments a monotonic revision counter returned in every response, enabling watch resumption and causal ordering.
- **Watch (SSE streaming)** — subscribe to key changes in real time via Server-Sent Events. Events are delivered with at-least-once semantics and can be resumed from a revision.
- **Multi-key transactions** — atomically compare-and-swap across multiple keys with conditional branching (success/failure). All operations execute under a single lock, a single Raft entry, and a single revision increment.
- **Leases with TTL** — grant time-bound leases and attach keys to them. Expired leases automatically delete their keys through Raft, ensuring all nodes converge.
- **Pagination** — bounded `List` responses with cursor-based pagination to avoid unbounded response sizes.
- **Snapshotting** — periodic snapshots (every 100 committed entries) compact the Raft log, bound disk usage, and speed up recovery. Snapshot state includes keys, leases, deduplication table, and revision.
- **Deduplication** — each request carries a `(ClientID, RequestID)` pair. The leader deduplicates retried requests so at-most-once semantics hold even under network failures, leader crashes, and client retries.
- **Cluster membership changes** — nodes can be added or removed at runtime via `POST /join/` and `POST /leave/`. Membership changes are themselves committed through the Raft log as config change entries.
- **Automatic leader discovery** — the client rotates through server addresses on `NotLeader` responses, and tracks an `assumedLeader` to minimise redirects.
- **Dynamic peer discovery** — the client can bootstrap from any seed node via `GET /members/` and refresh the member list when the cluster topology changes.
- **Fault tolerance** — survives leader crashes, follower crashes, network partitions, and delayed responses. The test suite includes extensive fault-injection coverage.

All KV operations are submitted as Raft log entries. The leader proposes the entry, a majority of nodes replicate it, and once committed the entry is applied to the state machine on every node. The response is sent to the caller only after local application.

## Linearizability

Zodiac provides **strict linearizability** for all operations:

1. Every operation is proposed as a Raft log entry and committed by a majority.
2. The response is sent to the client **only after** the entry is applied to the local state machine.
3. Duplicate detection ensures that client retries (due to timeouts, leader crashes, or delayed responses) never apply the same mutation twice.
4. Snapshot serialisation includes the deduplication table, so correctness holds across restarts.

This means once `Put` returns success, every subsequent `Get` (on any node, after any leader change) returns the written value.

## Revision

Each mutation (Put, Append, CAS, Txn, lease expiration) increments a cluster-wide 64-bit `currentRevision`. Read operations (Get, List) snapshot the current revision without incrementing. Every response includes a `Revision` field:

- Clients can use the revision as a logical clock to order events.
- Watch uses the revision for resumption: replay events with `revision > since`.
- The revision is persisted in snapshots and survives restarts.

## Watch (SSE event stream)

Subscribe to real-time key change notifications via Server-Sent Events:

```
GET /watch/?prefix=/pods/&since=42
```

Events are pushed as SSE with `event: put` / `event: delete` and the revision in both the JSON body and the `id:` field.

The server maintains an in-memory ring buffer (capacity 1000). Watchers with `since > 0` first replay buffered events, then stream live events. Slow consumers are silently dropped (non-blocking delivery).

## Transactions

Atomically evaluate conditions on multiple keys and execute a success or failure branch — all under a single lock, a single Raft entry, and a single revision increment.

Supported condition types: `EQ`, `NEQ`, `GTE`, `LTE`, `Exists`, `NotExists`. Branch operations: `Put`, `Delete`, `CAS`, `Append`.

This is the cornerstone for scheduling: atomically check node capacity and bind a pod in one RTT.

## Leases

Leases provide time-bound key ownership with automatic expiry:

| Operation | Effect |
|-----------|--------|
| `POST /lease/grant/` | Create a lease with a TTL (seconds), returns a unique lease ID |
| `POST /lease/keepalive/` | Reset the lease's expiry clock |
| `POST /lease/revoke/` | Immediately delete the lease and all attached keys |
| Expiry loop | Background goroutine checks every second; expired leases are cleaned through Raft |

Keys can be attached to a lease on Put (via the `LeaseID` field). When a lease expires or is revoked, all attached keys are deleted and `EventDelete` watch events are emitted. Lease state (including which nodes own which leases) is persisted in snapshots.

## Pagination

The `POST /list/` endpoint supports cursor-based pagination:

```json
{"prefix": "/pods/", "limit": 100, "keyAfter": "/pods/m"}
```

Response includes a `NextKey` field — pass it as `KeyAfter` in the next request to get the next page. An empty `NextKey` means no more results.

## Snapshotting

After every 100 committed entries, the leader serialises its state (key-value map, leases, deduplication table, revision) into a **snapshot** using Go's `gob` encoding. The snapshot compacts the Raft log and is used to efficiently bring new or lagging followers up to date.

Snapshot tests verify:

- Leader crash + restart with snapshot recovery
- Isolated follower catching up via snapshot installation
- Multiple snapshot rounds (successive snapshots supersede earlier ones)
- Deduplication table survives snapshot cycles

## Membership changes

Nodes join and leave at runtime through `POST /join/` and `POST /leave/` on the leader:

1. The joining node connects its Raft layer to the leader.
2. The leader proposes a `ConfigChange` (add node / remove node) through the Raft log.
3. Once committed, every node updates its peer list and persists it to storage.
4. A re-joining node restores its peer address book from storage and reconnects.

## Quick start

### Prerequisites

- Go 1.26+

### Build

```sh
git clone https://github.com/MHS-20/Zodiac
cd Zodiac
go build ./cmd/zodiacServer/
go build ./cmd/zodiacClient/
```

### Run a 3-node cluster (local)

Create three config files:

```json
// node1.json
{
  "node_id": 1,
  "http_port": 8001,
  "data_dir": "/tmp/zodiac/1",
  "initial_cluster": [
    {"id": 1, "http_addr": "localhost:8001", "raft_addr": "localhost:9001"},
    {"id": 2, "http_addr": "localhost:8002", "raft_addr": "localhost:9002"},
    {"id": 3, "http_addr": "localhost:8003", "raft_addr": "localhost:9003"}
  ]
}
```

```json
// node2.json
{
  "node_id": 2,
  "http_port": 8002,
  "data_dir": "/tmp/zodiac/2",
  "initial_cluster": [
    {"id": 1, "http_addr": "localhost:8001", "raft_addr": "localhost:9001"},
    {"id": 2, "http_addr": "localhost:8002", "raft_addr": "localhost:9002"},
    {"id": 3, "http_addr": "localhost:8003", "raft_addr": "localhost:9003"}
  ]
}
```

```json
// node3.json
{
  "node_id": 3,
  "http_port": 8003,
  "data_dir": "/tmp/zodiac/3",
  "initial_cluster": [
    {"id": 1, "http_addr": "localhost:8001", "raft_addr": "localhost:9001"},
    {"id": 2, "http_addr": "localhost:8002", "raft_addr": "localhost:9002"},
    {"id": 3, "http_addr": "localhost:8003", "raft_addr": "localhost:9003"}
  ]
}
```

In separate terminals:

```sh
RAFT_LISTEN_ADDR=:9001 ./zodiac node1.json
RAFT_LISTEN_ADDR=:9002 ./zodiac node2.json
RAFT_LISTEN_ADDR=:9003 ./zodiac node3.json
```

> `RAFT_LISTEN_ADDR` pins the Raft RPC port. If unset, the raft server binds a random port.

### Run with Docker Compose

```sh
cd deploy
docker compose up
```

This starts a 3-node cluster on ports `8001`–`8003` and a client container that runs a smoke test.

## Configuration

| Field | Type | Description |
|-------|------|-------------|
| `node_id` | int | Unique node identifier (non-negative) |
| `http_port` | int | HTTP API port |
| `data_dir` | string | Directory for Raft log and snapshot storage |
| `initial_cluster` | array | List of all cluster members with their addresses |

Each `initial_cluster` entry:

| Field | Description |
|-------|-------------|
| `id` | Node ID |
| `http_addr` | HTTP address other nodes use to reach this node |
| `raft_addr` | Raft RPC address other nodes use to reach this node |

When adding a new node to an existing cluster, omit it from `initial_cluster` on the joining node — it will discover the leader via the seed nodes in `initial_cluster` and send a `POST /join/` request automatically.

## Usage

### CLI (`zodiac-client`)

```sh
# Put a value
zodiac-client --addr localhost:8001 put name zodiac

# Get a value
zodiac-client --addr localhost:8001 get name
# → zodiac

# Append to a value
zodiac-client --addr localhost:8001 append name " kv"
zodiac-client --addr localhost:8001 get name
# → zodiac kv

# Compare-and-swap
zodiac-client --addr localhost:8001 cas name "zodiac kv" "zodiac kv store"

# List keys with a prefix
zodiac-client --addr localhost:8001 list /nodes/

# Verbose output includes revision
zodiac-client -v --addr localhost:8001 get name
# → zodiac (rev=42)

# Connect to multiple servers for automatic leader discovery
zodiac-client --addr localhost:8001,localhost:8002,localhost:8003 get name

# Discover cluster members from a seed
zodiac-client --discover --addr localhost:8001 get name
```

Exit code is 0 on success. For `get`, exit code 1 means the key was not found; the value is printed to stdout.

Flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `localhost:8000` | Comma-separated server addresses |
| `--discover` | `false` | Discover full cluster from seed addresses |
| `-v` | `false` | Verbose output including revision |
| `--timeout` | `5s` | Request timeout |

### Go SDK (`kvclient`)

```go
import "github.com/MHS-20/Zodiac/kvclient"

// Connect to known addresses
c := kvclient.New([]string{"localhost:8001", "localhost:8002"})

// Or discover members from any seed node
c := kvclient.NewWithDiscovery(ctx, []string{"localhost:8001"})

ctx := context.Background()

// Put / Get / Append / CAS
prev, existed, rev, err := c.Put(ctx, "key", "value")
val, found, rev, err := c.Get(ctx, "key")
prev, existed, rev, err := c.Append(ctx, "key", "suffix")
prev, existed, rev, err := c.CAS(ctx, "key", "expected", "new")

// List (unpaged — returns all matching keys)
pairs, rev, err := c.List(ctx, "/pods/")

// List (paged — cursor-based)
pairs, nextKey, rev, err := c.ListPaged(ctx, "/pods/", 100, "")

// Transactions
type TxnCondition struct {
    Key     string    `json:"key"`
    Compare CompareOp `json:"compare"` // EQ, NEQ, GTE, LTE, Exists, NotExists
    Value   string    `json:"value"`
}
type TxnOp struct {
    Op           TxnOpType `json:"op"` // Put, Delete, CAS, Append
    Key          string    `json:"key"`
    Value        string    `json:"value,omitempty"`
    CompareValue string    `json:"compare_value,omitempty"`
}
succeeded, results, rev, err := c.Txn(ctx, conditions, success, failure)

// Lease operations
leaseID, ttl, err := c.LeaseGrant(ctx, 15)       // 15-second TTL
err = c.LeaseKeepAlive(ctx, leaseID)
err = c.LeaseRevoke(ctx, leaseID)

// Put with a lease attachment
prev, existed, rev, err := c.PutWithLease(ctx, key, value, leaseID)

// Watch — real-time SSE event stream
ch, err := c.Watch(ctx, "/pods/", 0)    // from now
ch, err := c.Watch(ctx, "/pods/", 42)   // from revision 42
for event := range ch {
    fmt.Println(event.Key, event.Value, event.Revision, event.Type)
}

// Refresh the member list after cluster topology changes
n, err := c.RefreshMembers(ctx)
```

### HTTP API

All requests use `Content-Type: application/json`. Unknown JSON fields cause a `400 Bad Request`.

#### `POST /put/`

```json
{"key": "foo", "value": "bar", "clientID": 1, "requestID": 1}
```

Response:

```json
{"RespStatus": 1, "KeyFound": false, "PrevValue": "", "Revision": 1}
```

- `RespStatus`: `1` (OK), `2` (NotLeader), `3` (FailedCommit), `4` (DuplicateRequest)
- `KeyFound`: whether a previous value existed
- `PrevValue`: the previous value if `KeyFound` is true
- `Revision`: the cluster-wide revision after this operation

#### `POST /get/`

```json
{"key": "foo", "clientID": 1, "requestID": 2}
```

Response:

```json
{"RespStatus": 1, "KeyFound": true, "Value": "bar", "Revision": 1}
```

#### `POST /append/`

```json
{"key": "foo", "value": "suffix", "clientID": 1, "requestID": 3}
```

Response: same shape as `PUT` with `Revision`.

#### `POST /cas/`

```json
{"key": "foo", "compareValue": "bar", "value": "baz", "clientID": 1, "requestID": 4}
```

Response: same shape as `PUT` with `Revision`. The write only succeeds if the current value equals `compareValue`.

#### `POST /list/`

```json
{"prefix": "/pods/", "limit": 100, "keyAfter": "/pods/m", "clientID": 1, "requestID": 5}
```

Response:

```json
{"RespStatus": 1, "Pairs": {"/pods/a": "val", "/pods/b": "val"}, "NextKey": "/pods/n", "Revision": 42}
```

- `Pairs`: matching key-value pairs (up to `limit`)
- `NextKey`: cursor for the next page (empty if end of results)
- `Revision`: the current cluster-wide revision

When `Limit` is 0 or omitted, all matching keys are returned (unpaged).

#### `POST /txn/`

```json
{
  "conditions": [
    {"key": "/nodes/3/capacity", "compare": 2, "value": "4"}
  ],
  "success": [
    {"op": 0, "key": "/nodes/3/pods/abc", "value": "running"},
    {"op": 0, "key": "/pods/abc/node", "value": "3"}
  ],
  "failure": [
    {"op": 0, "key": "/sched/retry/pods/abc", "value": "1"}
  ],
  "clientID": 1,
  "requestID": 6
}
```

Response:

```json
{"RespStatus": 1, "Succeeded": true, "Results": [...], "Revision": 43}
```

- `Succeeded`: true if all conditions were met (success branch applied)
- `Results`: array of `{key, prev_value, key_found}` for each branch operation
- Compare ops: `0`=EQ, `1`=NEQ, `2`=GTE, `3`=LTE, `4`=Exists, `5`=NotExists
- Operations: `0`=Put, `1`=Delete, `2`=CAS, `3`=Append

#### `POST /lease/grant/`

```json
{"ttl": 15, "clientID": 1, "requestID": 7}
```

Response:

```json
{"RespStatus": 1, "id": 1, "ttl": 15}
```

#### `POST /lease/keepalive/`

```json
{"id": 1, "clientID": 1, "requestID": 8}
```

Response:

```json
{"RespStatus": 1, "id": 1}
```

#### `POST /lease/revoke/`

```json
{"id": 1, "clientID": 1, "requestID": 9}
```

Response:

```json
{"RespStatus": 1}
```

Revoking a lease immediately deletes all keys attached to it and emits `EventDelete` watch events.

#### `GET /watch/`

```
GET /watch/?prefix=/pods/&since=42
```

Response is a Server-Sent Events stream (`text/event-stream`):

```
event: put
id: 43
data: {"key":"/pods/abc","value":"running","revision":43}

event: delete
id: 44
data: {"key":"/pods/abc","value":"","revision":44}
```

- `prefix`: filter events to keys with this prefix (empty = all keys)
- `since`: replay events with revision > `since` from the ring buffer
- `Last-Event-ID` header is also accepted as an alternative to `since`

#### `POST /join/`

```json
{"id": 4, "raft_addr": "localhost:9004", "http_addr": "localhost:8004"}
```

Sent by a new node to the leader to join the cluster. Must be the leader.

#### `POST /leave/`

```json
{"id": 4}
```

Remove a node from the cluster. Must be the leader.

#### `GET /status/`

```json
{"resp_status": 1, "is_leader": true, "id": 1, "peer_count": 2}
```

Node status and leadership information.

#### `GET /members/`

```json
{"resp_status": 1, "members": [{"id": 1, "http_addr": "localhost:8001"}, {"id": 2, "http_addr": "localhost:8002"}]}
```

Full list of cluster members with their HTTP addresses.

## Tests

```sh
# All tests
go test ./...

# With race detector
go test -race ./...

# Single integration test
go test -run TestTxnAtomicity ./test/

# Unit tests
go test ./kvservice/...
```

The integration test suite (`./test/`) covers leader election, log replication, network partitions, crash recovery, snapshot installation, linearizable semantics, transactions, leases, pagination, watch, and concurrent access patterns.

### Test categories

| Package | What's tested |
|---------|---------------|
| `test/` | Integration tests with 3-node clusters, fault injection (crash, restart, partition, delay), client retry and dedup, snapshot survival |
| `kvservice/` | Unit tests for the in-memory DataStore and command processing |
| `test/txn_test.go` | Transaction atomicity and conditional branching |
| `test/lease_test.go` | Lease grant/keepalive/revoke, auto-expiry, key attachment |
| `test/watch_test.go` | SSE event streaming, prefix filtering, replay on reconnect |
| `test/snapshot_test.go` | Snapshot creation, installation, dedup table preservation |
| `test/list_pagination_test.go` | Cursor-based pagination edge cases |
| `test/system_test.go` | Baseline linearizability and fault tolerance |
