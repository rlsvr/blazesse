// Package blazesse provides high-performance, zero-allocation Server-Sent Events (SSE)
// streaming and broadcasting for Go.
//
// blazesse serves two primary use cases:
//
//  1. 1-to-1 Unicast Streaming: Direct, stateless connection upgrades (blazesse.Upgrade)
//     with per-write deadlines and automatic keep-alive heartbeats. Ideal for AI/LLM
//     token streaming (OpenAI/Gemini/Ollama) and long-running job progress updates.
//
//  2. 1-to-N Broadcaster: Ultra-high-throughput pub/sub engine (blazesse.NewBroadcaster)
//     with cache-line padded topic shards, conflated (lossy-latest) mailboxes, and
//     lossless FIFO queues powered by github.com/rlsvr/hirnok. Built to handle 50,000+
//     concurrent connections with zero allocations per subscriber.
package blazesse

import (
	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

// Core type aliases
type (
	// Frame is an immutable pre-serialized SSE wire payload.
	Frame = frame.Frame

	// Stream wraps an individual HTTP connection for direct 1-to-1 streaming.
	Stream = broadcaster.Stream

	// Broadcaster is a sharded, high-fanout SSE pub/sub engine.
	Broadcaster = broadcaster.Broadcaster

	// Subscriber is a connected client's message mailbox.
	Subscriber = broadcaster.Subscriber

	// DeliveryMode defines the mailbox delivery strategy (Conflated vs FIFO).
	DeliveryMode = broadcaster.DeliveryMode

	// OverflowPolicy determines FIFO queue behavior when full.
	OverflowPolicy = broadcaster.OverflowPolicy

	// Option configures Broadcaster or Stream behavior.
	Option = broadcaster.Option

	// MetricsCollector observes broadcaster metrics.
	MetricsCollector = broadcaster.MetricsCollector

	// CounterMetrics provides atomic metric counters.
	CounterMetrics = broadcaster.CounterMetrics
)

// DeliveryMode constants
const (
	// DeliveryConflated (default) overwrites unread frames with latest state.
	DeliveryConflated = broadcaster.DeliveryConflated

	// DeliveryFIFO provides strict chronological ordering via a bounded queue.
	DeliveryFIFO = broadcaster.DeliveryFIFO
)

// OverflowPolicy constants
const (
	// OverflowDropOldest discards the oldest queued frame to make room.
	OverflowDropOldest = broadcaster.OverflowDropOldest

	// OverflowDropNewest drops incoming frames when buffer is full.
	OverflowDropNewest = broadcaster.OverflowDropNewest

	// OverflowDisconnect terminates the slow subscriber connection.
	OverflowDisconnect = broadcaster.OverflowDisconnect
)

// Frame constructors
var (
	// NewFrame formats a W3C SSE compliant frame with event, id, retry, and multiline data.
	NewFrame = frame.Format

	// TextFrame creates a single-line data frame without an event name.
	TextFrame = frame.Text

	// EventFrame creates a frame with an event name and data payload.
	EventFrame = frame.Event

	// BytesFrame creates a frame with an event name and byte payload.
	BytesFrame = frame.Bytes

	// CommentFrame creates an SSE comment frame (": <comment>\n\n").
	CommentFrame = frame.Comment

	// PingFrame returns the shared keep-alive heartbeat frame (": ping\n\n").
	PingFrame = frame.Ping

	// FromRawFrame wraps pre-formatted wire bytes.
	FromRawFrame = frame.FromRaw

	// JSONFrame formats an SSE frame with an event name and JSON-encoded payload.
	JSONFrame = frame.JSON

	// JSONTextFrame formats an SSE data frame without an event name using JSON encoding.
	JSONTextFrame = frame.JSONText

	// JSONWithFrame formats an SSE frame using a user-provided JSON marshaler function.
	JSONWithFrame = frame.JSONWith
)

// Engine constructors
var (
	// NewBroadcaster initializes a high-throughput sharded SSE broadcaster.
	NewBroadcaster = broadcaster.New

	// Upgrade converts an HTTP request/response into an active 1-to-1 SSE stream.
	Upgrade = broadcaster.Upgrade

	// NewCounterMetrics creates an atomic metrics collector.
	NewCounterMetrics = broadcaster.NewCounterMetrics
)

// Functional configuration options
var (
	WithShards         = broadcaster.WithShards
	WithWriteTimeout   = broadcaster.WithWriteTimeout
	WithHeartbeat      = broadcaster.WithHeartbeat
	WithPingComment    = broadcaster.WithPingComment
	WithDeliveryMode   = broadcaster.WithDeliveryMode
	WithFIFOBufferSize = broadcaster.WithFIFOBufferSize
	WithOverflowPolicy = broadcaster.WithOverflowPolicy
	WithAsyncWorkers   = broadcaster.WithAsyncWorkers
	WithTopicExtractor = broadcaster.WithTopicExtractor
	WithMetrics        = broadcaster.WithMetrics
	WithOnConnect      = broadcaster.WithOnConnect
	WithOnDisconnect   = broadcaster.WithOnDisconnect
	WithHeader         = broadcaster.WithHeader
)
