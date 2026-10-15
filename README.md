# blazesse

[![Go Reference](https://pkg.go.dev/badge/github.com/rlsvr/blazesse.svg)](https://pkg.go.dev/github.com/rlsvr/blazesse)
[![CI](https://github.com/rlsvr/blazesse/actions/workflows/ci.yml/badge.svg)](https://github.com/rlsvr/blazesse/actions/workflows/ci.yml)
[![golangci-lint](https://img.shields.io/badge/golangci--lint-passing-brightgreen?logo=go)](https://github.com/rlsvr/blazesse/actions/workflows/ci.yml)
[![govulncheck](https://img.shields.io/badge/govulncheck-passing-brightgreen?logo=go)](https://github.com/rlsvr/blazesse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**`blazesse`** is an ultra-high-throughput, zero-allocation Server-Sent Events (SSE) toolkit for Go.

Engineered to eliminate slow-consumer memory bloat and withstand 50,000+ concurrent subscribers per node, `blazesse` provides both **direct 1-to-1 connection streaming** (for AI/LLM token generation and long-running job progress) and **sharded 1-to-N broadcasting** (with atomic state conflation and lossless FIFO queues powered by [`github.com/rlsvr/inas`](https://github.com/rlsvr/inas)).

---

## Benchmark Highlights

Run on 13th Gen Intel® Core™ i5-13600K (Linux amd64):

### Micro-Benchmarks (Raw Ingest & Serialization)

| Benchmark Scenario | Latency | Allocations |
| :--- | :--- | :--- |
| **Topic Hasher (Inline FNV-1a)** | **< 1 ns** | **0 B/op, 0 allocs/op** |
| **Single-Line Frame Serialization** | **~154 ns** | **2 allocs/op** |
| **JSON Frame Serialization (`JSONFrame`)** | **~210 ns** | **1 alloc/op** |
| **Fanout to 10,000 Subscribers (Direct)** | **~105 µs** | **0 B/op, 0 allocs/op** |
| **Fanout to 50,000 Subscribers (Direct)** | **~650 µs** | **0 allocs/op** |

### Macro-Benchmarks: View-Switching Under Churn (1k – 15k Subscribers)

Measured under real-world production stress:
- **1,000 to 15,000 active concurrent subscribers** partitioned across 10 thread topics .
- Continuous high-frequency message broadcasting across all threads.
- Concurrent background **view-switching churn** (continuous loop of client connections joining, reading, and switching topics) causing rapid, continuous subscription mutations.

| Concurrent Subscribers | `EngineLockFree` Latency | `EngineShardedMutex` Latency | Winner / Advantage |
| :---: | :---: | :---: | :---: |
| **1,000** | 104 µs (0.10 ms) | 78 µs (0.08 ms) | Sharded (+25% zero-churn advantage) |
| **3,000** | **201 µs (0.20 ms)** | 340 µs (0.34 ms) | **Lock-Free (+41% faster)** |
| **5,000** | **412 µs (0.41 ms)** | 479 µs (0.48 ms) | **Lock-Free (+14% faster)** |
| **8,000** | **554 µs (0.55 ms)** | 816 µs (0.82 ms) | **Lock-Free (+32% faster)** |
| **10,000** | **520 µs (0.52 ms)** | 818 µs (0.82 ms) | **Lock-Free (+36% faster)** |
| **15,000** | **688 µs (0.69 ms)** | 904 µs (0.90 ms) | **Lock-Free (+24% faster)** |

Under view-switching churn, `EngineLockFree` enables **~1,450 broadcasts/sec** to 15,000 subscribers with zero fan-out pauses.

---

## Why blazesse?

Traditional Go SSE libraries use buffered channels (`chan []byte`). Under real-world production stress (thousands of mobile clients with fluctuating network speeds), those architectures suffer from three fatal flaws:
1. **Slow Consumer OOM**: Stalled TCP sockets fill channel buffers, forcing unbounded heap allocation until the process crashes.
2. **Redundant Serialization**: Serializing JSON payloads inside per-subscriber loops results in $O(N)$ allocations and heavy GC pressure.
3. **Stale Backlogs**: When a client recovers from network lag, it receives an outdated queue of intermediate states instead of the current state.

`blazesse` solves this through five core design principles:
* **Pre-Serialized Immutable Wire Frames (`*Frame`)**: Formatted once at ingest; all 50,000 clients share a single pointer.
* **Lock-Free State Conflation (`DeliveryConflated`)**: Uses an `atomic.Pointer` swap + 1-capacity signal channel. If 50 updates arrive before the socket flushes, the pointer simply overwrites 49 times—the client wakes up once and flushes only the freshest state.
* **Immediate GC Reclamation (`Swap(nil)`)**: Upon read, the client atomically clears its pointer. Once flushed, zero references to the frame remain.
* **Pluggable Registry Engines**: Choose between 64 CPU cache-line padded `RWMutex` shards or an actor-based Lock-Free Copy-On-Write snapshot engine for high-churn view-switching.
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
   - No topics, no fanout, no mailboxes         - Pluggable Registry Engines:
   - Synchronous socket backpressure               ├── Lock-Free CoW Snapshot (High Churn)
                                                   └── 64 Sharded RWMutex (Static/Ingest)
                                                - Mailbox Delivery Strategies:
                                                   ├── Conflated (Lossy-Latest)
                                                   └── FIFO (Lossless Queue via inas)
                                                                  │
                                                        Stream reads & flushes
```

---

## Installation

```bash
go get github.com/rlsvr/blazesse
```

For the Echo web framework adapter:
```bash
go get github.com/rlsvr/blazesse-echo
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

type Ticker struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
}

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

	// Publish to 50,000 subscribers in ~650µs with 0 per-subscriber allocations
	go func() {
		for {
			data := Ticker{Symbol: "BTC-USD", Price: 68450.25}
			frame, _ := blazesse.JSONFrame("price", data)
			b.Publish("crypto.BTC", frame)
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

### 3. Labstack Echo Framework Integration
Integrate cleanly with [Echo](https://github.com/labstack/echo) using [`blazesse-echo`](https://github.com/rlsvr/blazesse-echo):

```go
package main

import (
	"github.com/labstack/echo/v4"
	"github.com/rlsvr/blazesse"
	blazesseEcho "github.com/rlsvr/blazesse-echo"
)

func main() {
	e := echo.New()
	b := blazesse.NewBroadcaster()
	defer b.Close()

	// 1. Mount broadcaster directly as an Echo route
	e.GET("/events", blazesseEcho.Handler(b))

	// 2. Or upgrade 1-to-1 within a standard Echo handler
	e.GET("/llm/stream", func(c echo.Context) error {
		stream, err := blazesseEcho.Upgrade(c)
		if err != nil {
			return err
		}
		defer stream.Close()

		return stream.Send(blazesse.TextFrame("data payload"))
	})

	e.Start(":8080")
}
```

---

### 4. Zero-Allocation Frame Constructors & JSON Helpers
`blazesse` formats SSE wire payloads once before fanout, sharing a single immutable byte buffer among all subscribers:

```go
// 1. Pre-formatted JSON Event frame ("event: user.created\ndata: {...}\n\n")
frame, err := blazesse.JSONFrame("user.created", myUserStruct)

// 2. Pre-formatted JSON Text frame ("data: {...}\n\n")
frame, err := blazesse.JSONTextFrame(myPayloadStruct)

// 3. Custom JSON marshaler (e.g. sonic, easyjson, or segmentio)
frame, err := blazesse.JSONWithFrame("order.placed", order, sonic.Marshal)

// 4. Raw byte payloads or string events
frame := blazesse.BytesFrame("audit", rawProtoOrJsonBytes)
frame := blazesse.EventFrame("ping", "pong")
frame := blazesse.TextFrame("plain text line")

// 5. SSE comments / Keep-alive frames (": heartbeat\n\n")
frame := blazesse.CommentFrame("heartbeat")
```

---

### 5. Lossless FIFO Delivery (Chat & Audit Feeds)
Powered by [`github.com/rlsvr/inas/pkg/queue`](https://github.com/rlsvr/inas):

```go
b := blazesse.NewBroadcaster(
	blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
	blazesse.WithFIFOBufferSize(1024),
	blazesse.WithOverflowPolicy(blazesse.OverflowDropOldest), // or OverflowDropNewest, OverflowDisconnect
)
```

---

### 6. Decoupled Asynchronous Fanout Worker Pool
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
		blazesse.WithReconfigInterval(15*time.Millisecond), // micro-batching window
		blazesse.WithReconfigBatchSize(128),               // max batch threshold
		blazesse.WithReconfigWait(false),                  // async (true for synchronous join)
	),
	blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
)

// Option B: CPU Cache-Line Padded Sharded Mutex (Ideal for static connections or zero-churn ingest)
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
| `NewEngineLockFree(opts...)` | Lock-free actor-based Copy-On-Write topic snapshot engine | Custom option |
| `WithReconfigInterval(d)` | Micro-batch timer window for `EngineLockFree` | `15ms` |
| `WithReconfigBatchSize(n)` | Batch capacity threshold triggering immediate CoW snapshot | `128` |
| `WithReconfigWait(bool)` | Whether `Subscribe`/`Unsubscribe` wait for snapshot commit | `false` |
| `NewEngineShardedMutex(opts...)` | 64 CPU cache-line padded RWMutex shards engine | Default engine |
| `WithEngineShards(n int)` | Shard count for `NewEngineShardedMutex` (power of 2) | `64` |
| `WithEngine(engine)` | Pluggable subscription registry engine | `EngineShardedMutex` |
| `WithShards(n int)` | Number of cache-line padded topic shards (alias) | `64` |
| `WithWriteTimeout(d time.Duration)` | Maximum duration allowed for a single write + flush | `3s` |
| `WithHeartbeat(base, jitter time.Duration)` | Keep-alive ping interval with randomized jitter | `20s + 10s jitter` |
| `WithPingComment(comment string)` | Custom ping payload text (SSE comment line) | `: ping\n\n` |
| `WithDeliveryMode(mode DeliveryMode)` | Mailbox strategy (`DeliveryConflated` or `DeliveryFIFO`) | `DeliveryConflated` |
| `WithFIFOBufferSize(size int)` | Queue capacity per subscriber in FIFO mode | `512` |
| `WithOverflowPolicy(policy OverflowPolicy)` | Buffer overflow policy (`DropOldest`, `DropNewest`, `Disconnect`) | `OverflowDropOldest` |
| `WithAsyncWorkers(workers, queueCap int)` | Decoupled fanout worker pool via `inas` | `0` (synchronous) |
| `WithTopicExtractor(fn TopicExtractor)` | Custom topic extraction from `*http.Request` | Query `?topic=`, `?topics=` |
| `WithOnConnect(fn func(r) error)` | Pre-connection authentication & authorization hook | `nil` |
| `WithOnDisconnect(fn func(r, topics))` | Connection termination callback | `nil` |
| `WithHeader(key, value string)` | Custom response headers (e.g. CORS) | `none` |
| `WithMaxTopicsPerConn(limit int)` | Maximum unique topics per client connection | `32` |
| `WithMaxTopicLen(limit int)` | Maximum character length for topic strings | `128` |
| `WithMaxConnections(limit int)` | Maximum concurrent connections allowed (429 when saturated) | `0` (unlimited) |
| `WithAuthorizeTopics(fn func(r, topics))` | Topic-aware pre-connection authorization hook (403 on error) | `nil` |
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
