# Changelog

All notable changes to `blazesse` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Replay Ring Buffer**: Added `WithReplayBuffer(cap int)` enabling historical event replay via standard `Last-Event-ID` header and `?last_event_id=` query parameter.
- **Resource Limits & Security Caps**: Added `WithMaxTopicsPerConn`, `WithMaxTopicLen`, and `WithMaxConnections` (rejecting saturated connections with HTTP 429 and `Retry-After`).
- **Topic-Aware Authorization**: Added `WithAuthorizeTopics` hook receiving requested topics before upgrading.
- **Zero-Allocation Mailbox Fetch**: Reusable backing slices for `ConflatingSubscriber` (0 allocs/op) and `FIFOSubscriber`.
- **Pre-Flush Writer Validation**: `Upgrade` verifies `http.Flusher` capability before committing HTTP 200 headers.
- **Continuous Fuzzing**: Added native WHATWG SSE frame fuzzing (`FuzzFrameFormat`).
- **CI & Benchmarking**: Added GitHub Actions workflow (`.github/workflows/ci.yml`) and reproducible benchmark script (`scripts/bench.sh`).

### Fixed
- **SSE Field/Event Injection**: Enforced strict sanitization of `\r` and `\n` across data, event names, comments, and IDs.
- **Graceful Shutdown**: Added `Subscriber.Done()` channel and `handlersWg` ensuring `Broadcaster.Close()` immediately terminates in-flight `ServeHTTP` handlers.
- **Lock-Free Engine Reconfig Deadlock**: Fixed actor shutdown path to drain and close all pending `done` channels upon context cancellation.
- **Lock-Free Subscribe Gap**: Stored atomic snapshot before unblocking synchronous `Subscribe()` calls.
- **Async Fanout Ordering**: Partitioned async workers into per-topic shards (`hashTopic(topic) % workers`) to strictly guarantee FIFO order per topic.
- **Overflow Disconnect**: Correctly signals termination via `sub.Done()` to wake draining readers.
- **Header Passthrough**: Ensured `WithHeader` (CORS) and `WithPingComment` are applied to stream upgrades in `ServeHTTP`.

### Changed
- **API Hygiene**: Replaced mutable root package `var` re-exports with strongly-typed `func` wrappers.
- **Decoupled Echo Adapter**: Migrated Echo integration to standalone repository [`blazesse-echo`](https://github.com/rlsvr/blazesse-echo), leaving `blazesse` with zero external dependencies outside `inas`.
- **Goroutine Optimization**: Replaced per-connection stream cancellation watcher goroutine with `context.AfterFunc` (saving 1 goroutine per connection).
