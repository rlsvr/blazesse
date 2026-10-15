# blazesse

[![Go Reference](https://pkg.go.dev/badge/github.com/rlsvr/blazesse.svg)](https://pkg.go.dev/github.com/rlsvr/blazesse)
[![Go Report Card](https://goreportcard.com/badge/github.com/rlsvr/blazesse)](https://goreportcard.com/report/github.com/rlsvr/blazesse)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**`blazesse`** is an ultra-high-throughput, zero-allocation Server-Sent Events (SSE) toolkit for Go.

Engineered to eliminate slow-consumer memory bloat and withstand 50,000+ concurrent subscribers per node, `blazesse` provides both **direct 1-to-1 connection streaming** (for AI/LLM token generation and long-running job progress) and **sharded 1-to-N broadcasting** (with atomic state conflation and lossless FIFO queues powered by [`github.com/rlsvr/inas`](https://github.com/rlsvr/inas)).

---

## Benchmark Highlights

Run on Intel® Core™ i5-13600K (Linux amd64):

| Benchmark Scenario | Throughput / Fanout Latency | Allocations |
| :--- | :--- | :--- |
| **Fanout to 10,000 Subscribers** | **~105 µs** | **0 B/op, 0 allocs/op** |
| **Fanout to 50,000 Subscribers** | **~650 µs** | **0 allocs/op** |
| **Single-Line Frame Serialization** | **~154 ns** | **2 allocs/op** |
| **Topic Hasher (Inline FNV-1a)** | **< 1 ns** | **0 B/op, 0 allocs/op** |

---

## Why blazesse?

Traditional Go SSE libraries use buffered channels (`chan []byte`). Under real-world production stress (thousands of mobile clients with fluctuating network speeds), those architectures suffer from three fatal flaws:
1. **Slow Consumer OOM**: Stalled TCP sockets fill channel buffers, forcing unbounded heap allocation until the process crashes.
2. **Redundant Serialization**: Serializing JSON payloads inside per-subscriber loops results in $O(N)$ allocations and heavy GC pressure.
3. **Stale Backlogs**: When a client recovers from network lag, it receives an outdated queue of intermediate states instead of the current state.

`blazesse` solves this through four core design principles:
* **Pre-Serialized Immutable Wire Frames (`*Frame`)**: Formatted once at ingest; all 50,000 clients share a single pointer.
* **Lock-Free State Conflation (`DeliveryConflated`)**: Uses an `atomic.Pointer` swap + 1-capacity signal channel. If 50 updates arrive before the socket flushes, the pointer simply overwrites 49 times—the client wakes up once and flushes only the freshest state.
* **Immediate GC Reclamation (`Swap(nil)`)**: Upon read, the client atomically clears its pointer. Once flushed, zero references to the frame remain.
* **Per-Write Deadlines & Jittered Heartbeats**: Powered by Go 1.20+ `http.ResponseController`, dead TCP ZeroWindow connections are evicted within 3 seconds, and keep-alive pings are jittered per connection to prevent timer-heap thundering herds.

---

## Architectural Overview

```
                                [ HTTP Client Request ]
                                           │
                                           ▼
                                 ┌──────────────────┐
                                 │  stream.Stream   │  <-- Physical SSE Connection
                                 │  - Set Headers   │      (Wraps http.ResponseWriter)
                                 │  - WriteDeadline │
                                 │  - Keepalive Ping│
                                 └─────────┬────────┘
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    │                                             │
                    ▼                                             ▼
       [ 1-to-1 Unicast Mode ]                       [ 1-to-N Broadcast Mode ]
     Direct stream.Send(frame)                    Managed by broadcaster.Broadcaster
   - Used for: LLM token generation,            - Used for: 50k clients on topics,
     job progress, user-specific pushes           crypto tickers, sports scores, chat
   - No topics, no fanout, no mailboxes         - Needs Mailbox Delivery Strategies:
   - Synchronous socket backpressure               ┌─────────────────────────────┐
                                                   │   Subscriber Mailbox        │
                                                   │   ├── Conflated (Lossy)     │
                                                   │   └── FIFO (Lossless Queue) │
                                                   └──────────────┬──────────────┘
                                                                  │
                                                        Stream reads & flushes
```

---

## Installation

```bash
go get github.com/rlsvr/blazesse
```

---

## Quickstart

### 1. Direct 1-to-1 Unicast Streaming (AI / LLM Tokens)
Upgrades an HTTP connection directly with zero background state:

```go
package main

import (
	"net/http"
	"time"

	"github.com/rlsvr/blazesse"
)

func handleChat(w http.ResponseWriter, r *http.Request) {
	// Upgrade directly to SSE stream (sets headers, deadlines, and heartbeats)
	stream, err := blazesse.Upgrade(w, r, blazesse.WithWriteTimeout(3*time.Second))
	if err != nil {
		return
	}
	defer stream.Close()

	tokens := []string{"Hello", "world", "from", "blazesse!"}
	for _, token := range tokens {
		_ = stream.Send(blazesse.EventFrame("token", token))
		time.Sleep(50 * time.Millisecond)
	}
}
```

---

### 2. High-Throughput 1-to-N Broadcaster (Conflated Market Tickers)
Broadcasts live market data across 64 cache-line padded topic shards:

```go
package main

import (
	"net/http"
	"time"

	"github.com/rlsvr/blazesse"
)

func main() {
	b := blazesse.NewBroadcaster(
		blazesse.WithShards(64),
		blazesse.WithDeliveryMode(blazesse.DeliveryConflated), // slow clients drop stale ticks
		blazesse.WithWriteTimeout(3*time.Second),
		blazesse.WithHeartbeat(20*time.Second, 10*time.Second),
	)
	defer b.Close()

	// Mount directly as an http.Handler (subscribes via ?topic= or ?topics=)
	http.Handle("/tickers", b)

	// Publish to 50,000 subscribers in ~650µs with 0 allocations
	go func() {
		for {
			payload := `{"symbol":"BTC-USD","price":68450.25}`
			b.Publish("crypto.BTC", blazesse.EventFrame("price", payload))
			time.Sleep(20 * time.Millisecond)
		}
	}()

	_ = http.ListenAndServe(":8080", nil)
}
```

Connect via curl:
```bash
# Subscribe to single topic
curl -N "http://localhost:8080/tickers?topic=crypto.BTC"

# Multiplex multiple topics on one connection
curl -N "http://localhost:8080/tickers?topics=crypto.BTC,crypto.ETH"
```

---

### 3. Lossless FIFO Delivery (Chat & Audit Feeds)
Powered by [`github.com/rlsvr/inas/pkg/queue`](https://github.com/rlsvr/inas):

```go
b := blazesse.NewBroadcaster(
	blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
	blazesse.WithFIFOBufferSize(1024),
	blazesse.WithOverflowPolicy(blazesse.OverflowDropOldest), // or OverflowDropNewest, OverflowDisconnect
)
```

---

### 4. Decoupled Asynchronous Fanout Worker Pool
Offload fanout loops entirely from ingress network threads (`TryPush < 50ns`) using [`github.com/rlsvr/inas/pkg/worker`](https://github.com/rlsvr/inas):

```go
b := blazesse.NewBroadcaster(
	blazesse.WithAsyncWorkers(8, 20000), // 8 background fanout workers, 20k queue cap
)
```

---

## Choosing a Registry Engine: Lock-Free vs. Sharded Mutex

`blazesse` provides two distinct internal subscription storage engines depending on your workload:

```go
// Option A: Lock-Free Copy-On-Write (COW) Snapshot (Default for high-churn view switching)
b := blazesse.NewBroadcaster(
	blazesse.NewEngineLockFree(
		blazesse.WithReconfigInterval(15*time.Millisecond), // batching window
		blazesse.WithReconfigBatchSize(128),               // max batch threshold
		blazesse.WithReconfigWait(false),                  // async (true for synchronous join)
	),
	blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
)

// Option B: CPU Cache-Line Padded Sharded Mutex (Ideal for static connections or write-heavy churn)
b := blazesse.NewBroadcaster(
	blazesse.NewEngineShardedMutex(
		blazesse.WithEngineShards(64), // 64 padded shards
	),
	blazesse.WithDeliveryMode(blazesse.DeliveryConflated),
)
```

### Decision Matrix: Which Engine Should You Choose?

| Workload Characteristic | `EngineLockFree` (`NewEngineLockFree`) | `EngineShardedMutex` (`NewEngineShardedMutex`) |
| :--- | :--- | :--- |
| **Primary Pattern** | **Read-Heavy / High-Churn View-Switching** | **Static Long-Lived Subs / Heavy Ingest** |
| **Broadcast (`Publish`) Cost** | **Zero Locks** (`atomic.Pointer.Load()`, single `MOV`) | `RLock()` across hashed shard |
| **Reader Contention** | **Zero contention** — broadcasts NEVER pause | Can stall behind active writer locks |
| **Subscribe / Switch Latency** | Batched asynchronously (0–15ms configurable) | **Immediate 0ms** in-place map mutation |
| **Memory Footprint** | Granular Copy-On-Write topic maps | Fixed sharded maps |
| **Ideal Real-World Use Case** | **Social feeds, live chat rooms, mobile view switching** where thousands of users switch threads every few seconds without stalling broadcast delivery | **Stock market tickers, crypto order books, IoT telemetry** where clients connect once to fixed topics and remain for hours while millions of messages publish |

> [!TIP]
> **Why does this matter?** In Go, `sync.RWMutex` is writer-preferred. Under rapid view switching (thousands of clients subscribing/unsubscribing concurrently), writer `Lock()` calls starve broadcast `RLock()` readers, causing stuttering and fan-out stalls. `EngineLockFree` decouples `Publish` completely from subscription churn.

---

## Configuration Options

| Option | Description | Default |
| :--- | :--- | :--- |
| `NewEngineLockFree(opts...)` | Lock-free actor-based Copy-On-Write topic snapshot | Active if specified |
| `WithReconfigInterval(d)` | Micro-batch timer window for `EngineLockFree` | `15ms` |
| `WithReconfigBatchSize(n)` | Batch capacity threshold triggering immediate CoW snapshot | `128` |
| `WithReconfigWait(bool)` | Whether `Subscribe`/`Unsubscribe` wait for snapshot commit | `false` |
| `NewEngineShardedMutex(opts...)` | 64 CPU cache-line padded RWMutex shards engine | Default (`EngineShardedMutex`) |
| `WithEngineShards(n int)` | Shard count for `NewEngineShardedMutex` (power of 2) | `64` |
| `WithShards(n int)` | Number of cache-line padded topic shards (alias) | `64` |
| `WithWriteTimeout(d time.Duration)` | Maximum duration allowed for a single write + flush | `3s` |
| `WithHeartbeat(base, jitter time.Duration)` | Keep-alive ping interval with randomized jitter | `20s + 10s jitter` |
| `WithDeliveryMode(mode DeliveryMode)` | Mailbox strategy (`DeliveryConflated` or `DeliveryFIFO`) | `DeliveryConflated` |
| `WithFIFOBufferSize(size int)` | Queue capacity per subscriber in FIFO mode | `512` |
| `WithOverflowPolicy(policy OverflowPolicy)` | Buffer overflow policy (`DropOldest`, `DropNewest`, `Disconnect`) | `OverflowDropOldest` |
| `WithAsyncWorkers(workers, queueCap int)` | Decoupled fanout worker pool via `inas` | `0` (synchronous) |
| `WithTopicExtractor(fn TopicExtractor)` | Custom topic extraction from `*http.Request` | Query `?topic=`, `?topics=` |
| `WithOnConnect(fn func(r) error)` | Pre-connection authentication & authorization hook | `nil` |
| `WithOnDisconnect(fn func(r, topics))` | Connection termination callback | `nil` |
| `WithHeader(key, value string)` | Custom response headers (e.g. CORS) | `none` |
| `WithMetrics(m MetricsCollector)` | Metrics hook (Prometheus/custom counters) | `NoopMetrics` |

---

## Testing & Verification

```bash
# Run all tests with race detector
make test

# Run verbose tests
make test-verbose

# Run benchmarks
make bench

# Run linter
make lint
```

---

## License

[MIT](LICENSE) © [rlsvr](https://github.com/rlsvr)
