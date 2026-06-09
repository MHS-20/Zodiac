# Zodiac

Distributed, strongly consistent key-value store built on a custom implementation of the [Raft](https://github.com/MHS-20/Raft) consensus protocol. This project was built mainly to support the [Poseidon](https://github.com/MHS-20/Poseidon)  project.

<div align="center">
<img src="zodiac.png" alt="Logo" width="300"/>
</div>

## Features

- **Linearizable operations** — every read and write goes through the Raft log, committed by a majority, and acknowledged to the client only after full application to the state machine.
- **Snapshotting** — periodic snapshots (every 100 committed entries) compact the Raft log, bound disk usage, and speed up recovery. Snapshot state includes the full key space and the deduplication table.
- **Deduplication** — each request carries a `(ClientID, RequestID)` pair. The leader deduplicates retried requests so at-most-once semantics hold even under network failures, leader crashes, and client retries.
- **Cluster membership changes** — nodes can be added or removed at runtime via `POST /join/` and `POST /leave/`. Membership changes are themselves committed through the Raft log as config change entries.
- **Automatic leader discovery** — the client rotates through server addresses on `NotLeader` responses, and tracks an `assumedLeader` to minimise redirects.
- **Dynamic peer discovery** — the client can bootstrap from any seed node via `GET /members/` and refresh the member list when the cluster topology changes.
- **Fault tolerance** — survives leader crashes, follower crashes, network partitions, and delayed responses. The test suite includes extensive fault-injection coverage.

All KV operations are submitted as Raft log entries. The leader proposes the entry, a majority of nodes replicate it, and once committed the entry is applied to the state machine on every node. The response is sent to the caller only after local application.

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
| `--timeout` | `5s` | Request timeout |

### Go SDK (`kvclient`)

```go
import "github.com/MHS-20/Zodiac/kvclient"

// Connect to known addresses
c := kvclient.New([]string{"localhost:8001", "localhost:8002"})

// Or discover members from any seed node
c := kvclient.NewWithDiscovery(ctx, []string{"localhost:8001"})

ctx := context.Background()

// Put
prev, existed, err := c.Put(ctx, "key", "value")

// Get
val, found, err := c.Get(ctx, "key")

// Append
prev, existed, err := c.Append(ctx, "key", "suffix")

// CAS (compare-and-swap)
prev, existed, err := c.CAS(ctx, "key", "expected", "new")

// Refresh the member list after cluster topology changes
n, err := c.RefreshMembers(ctx)
```

The client handles:

- **Leader discovery** — rotates through addresses on `NotLeader` responses
- **Retries** — retries on timeout to a different server
- **Deduplication** — assigns monotonically increasing `RequestID` per client instance

### HTTP API

All requests use `Content-Type: application/json`. Unknown JSON fields cause a `400 Bad Request`.

#### `POST /put/`

```json
{"key": "foo", "value": "bar", "clientID": 1, "requestID": 1}
```

Response:

```json
{"RespStatus": 1, "KeyFound": false, "PrevValue": ""}
```

- `RespStatus`: `1` (OK), `2` (NotLeader), `3` (FailedCommit), `4` (DuplicateRequest)
- `KeyFound`: whether a previous value existed
- `PrevValue`: the previous value if `KeyFound` is true

#### `POST /get/`

```json
{"key": "foo", "clientID": 1, "requestID": 2}
```

Response:

```json
{"RespStatus": 1, "KeyFound": true, "Value": "bar"}
```

#### `POST /append/`

```json
{"key": "foo", "value": "suffix", "clientID": 1, "requestID": 3}
```

Response: same shape as `PUT`.

#### `POST /cas/`

```json
{"key": "foo", "compareValue": "bar", "value": "baz", "clientID": 1, "requestID": 4}
```

Response: same shape as `PUT`. The write only succeeds if the current value equals `compareValue`.

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

## Linearizability

Zodiac provides **strict linearizability** for all operations:

1. Every operation is proposed as a Raft log entry and committed by a majority.
2. The response is sent to the client **only after** the entry is applied to the local state machine.
3. Duplicate detection ensures that client retries (due to timeouts, leader crashes, or delayed responses) never apply the same mutation twice.
4. Snapshot serialisation includes the deduplication table, so correctness holds across restarts.

This means once `Put` returns success, every subsequent `Get` (on any node, after any leader change) returns the written value.

## Snapshotting

After every 100 committed entries, the leader serialises its state (key-value map + deduplication table) into a **snapshot** using Go's `gob` encoding. The snapshot compacts the Raft log and is used to efficiently bring new or lagging followers up to date.

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

## Tests

```sh
# All tests
go test ./...

# With race detector
go test -race ./...

# Single test
go test -run TestSnapshotBasicRestart ./test/

# Unit tests only
go test ./kvservice/...
```

The integration test suite (`./test/`) covers leader election, log replication, network partitions, crash recovery, snapshot installation, linearizable semantics, and concurrent access patterns.
