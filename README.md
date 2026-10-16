# raftkv

A replicated key-value store in Go, built on a from-scratch implementation of the [Raft](https://raft.github.io/raft.pdf) consensus algorithm. The main thing I wanted to get right is the testing. A deterministic simulator runs the whole cluster on a fake network and clock, injects partitions, crashes, message loss, duplication and reordering, and checks the results with a linearizability checker I wrote for it.

```
go test ./...                       # everything, ~15 s
scripts/cluster.sh                  # 3 real nodes on localhost:9001-9003
curl -XPOST localhost:9001/kv -d '{"kind":"put","key":"a","value":"1"}'
```

## What's in it

| Package | What it does |
|---|---|
| `raft` | Leader election, log replication, and commit, written as a deterministic state machine. It does no I/O and starts no goroutines: the caller feeds it ticks, messages and proposals, then drains `Ready()` for messages to send and committed entries. Persistence goes through a `Storage` interface. |
| `raft` (storage) | `FileStorage` is an append-only log plus an atomically replaced `state.json`. Every record carries a CRC32, so a write torn by a crash is detected and cut off on restart. |
| `kv` | The state machine: get, put and append. Every op goes through the log, reads included, so each op takes effect at one point in log order, which makes the store linearizable. Clients tag ops with (client ID, sequence number). A retried op is answered from a session table instead of running twice. Also here: an HTTP server for one member and a client that follows leader redirects and retries. |
| `sim` | Runs N Raft nodes, the KV state machines and simulated clients in one goroutine. All randomness comes from a single seed, so any failure replays exactly. |
| `lincheck` | A Wing & Gong linearizability checker with Lowe's memoization (the same approach as Porcupine and Knossos). It checks each key separately. |

## How it's tested

**The simulator.** Each seed runs 3,000 ticks of faults against a 3- or 5-node cluster with 5 clients:
- messages delayed 1–3 ticks (which reorders them), 5% dropped, 2% duplicated
- random network partitions that heal
- node crashes and restarts from their persisted state, with at most a minority down at once

After every tick it checks two Raft safety properties:
- **election safety:** at most one leader per term, ever
- **state machine safety:** every node applies the same entry at each index

It also checks **log matching** every 50 ticks. At the end it heals everything and requires every client op to finish (liveness). Then it checks that the whole client history is linearizable.

| Run | Client ops checked | Elections | Crashes | Partitions | Violations |
|---|---|---|---|---|---|
| 200 seeds × 3 nodes | 468,903 | 2,312 | 1,713 | 2,454 | 0 |
| 200 seeds × 5 nodes | 500,511 | 1,913 | 1,351 | 2,417 | 0 |

CI runs 1,000 seeds per cluster size on every push. If a seed fails, rerun just that one with `go test ./sim -run 'TestSimulation/nodes=5/seed=17' -v`.

**Does the simulator actually catch bugs?** A checker that never fails proves nothing, so `raft.BugInjection` can switch off individual safety rules. The tests require the simulator to catch each one:

| Bug injected | Caught |
|---|---|
| Grant votes without the "candidate's log is up to date" check (§5.4.1) | Seed 1. A stale leader tries to overwrite an entry a follower already committed, which trips the assertion in `appendEntries`. |
| Commit earlier-term entries by counting replicas: the Figure 8 bug (§5.4.2) | Seed 279. Only with one entry per message, a 3-node cluster and frequent crashes; with default settings it wasn't found in 300 seeds. The new leader's no-op travels in the same message as the old entry, so the dangerous state rarely happens. Some seeds surface it as two nodes applying different entries at the same index. |
| Store runs retried ops again (no dedupe) | Seed 1. The linearizability checker sees an append applied twice. |

The simulator also found a problem I didn't plant. With one entry per message, a follower that spent most of a run crashed could need more than 1,000 ticks to catch up. That's because catch-up sends one batch per round trip, with no pipelining. It's slow but not wrong, so the liveness window is now 3,000 ticks. Pipelining is on the list below.

**Real network.** `kv/server_test.go` starts 3 real servers on loopback TCP, each with its log on disk. The test:
1. Writes data, then kills the leader and checks that the other two elect a new one and keep serving.
2. Restarts the killed node from disk.
3. Kills a different node, so the restarted one has to be part of every quorum.

It then checks that each append happened exactly once across all the retries. This test and the unit tests run under the race detector.

**Unit tests** cover:
- elections and replication
- a minority leader that can't commit and drops its entries after the partition heals
- a node with a stale log that can't win an election
- restart keeping the term, vote and log
- torn and corrupt WAL records being truncated on open

## Performance

One client doing sequential puts on a local 3-node cluster takes about **10.2 ms per put** (Apple M4, loopback). Most of that is two fsyncs in a row, about 3.1 ms each on macOS, which uses `F_FULLFSYNC`: the leader's write, then the follower's. Proposals aren't batched, so throughput scales with concurrent clients only as far as fsync allows.

## What it doesn't do

These are deliberate scope cuts, each one a known extension:
- **No snapshots or log compaction.** The log grows forever, and a restarted node replays it from index 1.
- **No membership changes.** The cluster is fixed at startup.
- **Reads go through the log.** That's simple and linearizable but costs a write's latency. ReadIndex or leases would fix that.
- **No pre-vote and no check-quorum.** A partitioned node that rejoins with a higher term forces an election, and a leader cut off from the majority keeps its role until it hears a higher term. Its clients time out and retry elsewhere.
- **No pipelining or group commit** (see Performance).
- **The session table is never pruned.**

## Layout

```
raft/raft.go        the algorithm (~470 lines)
raft/storage.go     MemoryStorage, FileStorage (WAL with CRCs)
kv/                 Store, HTTP Server, Client
sim/                deterministic cluster simulator
lincheck/           linearizability checker
cmd/raftkv/         the server binary
```

MIT License.
