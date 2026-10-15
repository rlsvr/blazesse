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
//     lossless FIFO queues powered by github.com/rlsvr/inas. Built to handle 50,000+
//     concurrent connections with zero allocations per subscriber.
package blazesse

import (
	"net/http"
	"time"

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

	// RegistryEngine defines the internal topic subscription storage model (EngineShardedMutex vs EngineLockFree).
	RegistryEngine = broadcaster.RegistryEngine

	// DeliveryMode defines the mailbox delivery strategy (Conflated vs FIFO).
	DeliveryMode = broadcaster.DeliveryMode

	// OverflowPolicy determines FIFO queue behavior when full.
	OverflowPolicy = broadcaster.OverflowPolicy

	// Option configures Broadcaster or Stream behavior.
	Option = broadcaster.Option

	// LockFreeEngineOption configures settings specific to EngineLockFree.
	LockFreeEngineOption = broadcaster.LockFreeEngineOption

	// ShardedMutexEngineOption configures settings specific to EngineShardedMutex.
	ShardedMutexEngineOption = broadcaster.ShardedMutexEngineOption

	// MetricsCollector observes broadcaster metrics.
	MetricsCollector = broadcaster.MetricsCollector

	// CounterMetrics provides atomic metric counters.
	CounterMetrics = broadcaster.CounterMetrics

	// TopicExtractor extracts subscribed topics from an incoming HTTP request.
	TopicExtractor = broadcaster.TopicExtractor
)

// RegistryEngine constants
const (
	// EngineShardedMutex (default) partitions topics across 64 CPU-padded RWMutex shards.
	// Best suited for low-to-medium churn subscriptions.
	EngineShardedMutex = broadcaster.EngineShardedMutex

	// EngineLockFree uses an actor-based Copy-On-Write topic snapshot (atomic.Pointer).
	// Reads during Publish are 100% lock-free (MOV instruction).
	// Subscriptions and view switches are batched asynchronously, completely eliminating
	// writer lock contention and fan-out pauses.
	EngineLockFree = broadcaster.EngineLockFree
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

// NewFrame formats a W3C SSE compliant frame with event, id, retry, and multiline data.
func NewFrame(event, id string, retry time.Duration, data []byte) *Frame {
	return frame.Format(event, id, retry, data)
}

// TextFrame creates a single-line data frame without an event name.
func TextFrame(data string) *Frame {
	return frame.Text(data)
}

// EventFrame creates a frame with an event name and data payload.
func EventFrame(event, data string) *Frame {
	return frame.Event(event, data)
}

// BytesFrame creates a frame with an event name and byte payload.
func BytesFrame(event string, data []byte) *Frame {
	return frame.Bytes(event, data)
}

// CommentFrame creates an SSE comment frame (": <comment>\n\n").
func CommentFrame(comment string) *Frame {
	return frame.Comment(comment)
}

// PingFrame returns the shared keep-alive heartbeat frame (": ping\n\n").
func PingFrame() *Frame {
	return frame.Ping()
}

// FromRawFrame wraps pre-formatted wire bytes.
func FromRawFrame(b []byte) *Frame {
	return frame.FromRaw(b)
}

// JSONFrame formats an SSE frame with an event name and JSON-encoded payload.
func JSONFrame(event string, v any) (*Frame, error) {
	return frame.JSON(event, v)
}

// JSONTextFrame formats an SSE data frame without an event name using JSON encoding.
func JSONTextFrame(v any) (*Frame, error) {
	return frame.JSONText(v)
}

// JSONWithFrame formats an SSE frame using a user-provided JSON marshaler function.
func JSONWithFrame(event string, v any, marshal func(any) ([]byte, error)) (*Frame, error) {
	return frame.JSONWith(event, v, marshal)
}

// Engine constructors

// NewBroadcaster initializes a high-throughput sharded SSE broadcaster.
func NewBroadcaster(opts ...Option) *Broadcaster {
	return broadcaster.New(opts...)
}

// Upgrade converts an HTTP request/response into an active 1-to-1 SSE stream.
func Upgrade(w http.ResponseWriter, r *http.Request, opts ...Option) (*Stream, error) {
	return broadcaster.Upgrade(w, r, opts...)
}

// NewCounterMetrics creates an atomic metrics collector.
func NewCounterMetrics() *CounterMetrics {
	return broadcaster.NewCounterMetrics()
}

// Functional configuration options

func NewEngineLockFree(opts ...LockFreeEngineOption) Option {
	return broadcaster.NewEngineLockFree(opts...)
}

func NewEngineShardedMutex(opts ...ShardedMutexEngineOption) Option {
	return broadcaster.NewEngineShardedMutex(opts...)
}

func WithEngine(e RegistryEngine) Option {
	return broadcaster.WithEngine(e)
}

func WithReconfigInterval(d time.Duration) LockFreeEngineOption {
	return broadcaster.WithReconfigInterval(d)
}

func WithReconfigBatchSize(size int) LockFreeEngineOption {
	return broadcaster.WithReconfigBatchSize(size)
}

func WithReconfigWait(wait bool) LockFreeEngineOption {
	return broadcaster.WithReconfigWait(wait)
}

func WithEngineShards(n int) ShardedMutexEngineOption {
	return broadcaster.WithEngineShards(n)
}

func WithShards(n int) Option {
	return broadcaster.WithShards(n)
}

func WithWriteTimeout(d time.Duration) Option {
	return broadcaster.WithWriteTimeout(d)
}

func WithHeartbeat(interval, jitter time.Duration) Option {
	return broadcaster.WithHeartbeat(interval, jitter)
}

func WithPingComment(comment string) Option {
	return broadcaster.WithPingComment(comment)
}

func WithDeliveryMode(mode DeliveryMode) Option {
	return broadcaster.WithDeliveryMode(mode)
}

func WithFIFOBufferSize(size int) Option {
	return broadcaster.WithFIFOBufferSize(size)
}

func WithOverflowPolicy(policy OverflowPolicy) Option {
	return broadcaster.WithOverflowPolicy(policy)
}

func WithAsyncWorkers(workers int, queueCap int) Option {
	return broadcaster.WithAsyncWorkers(workers, queueCap)
}

func WithTopicExtractor(fn TopicExtractor) Option {
	return broadcaster.WithTopicExtractor(fn)
}

func WithMetrics(m MetricsCollector) Option {
	return broadcaster.WithMetrics(m)
}

func WithOnConnect(fn func(r *http.Request) error) Option {
	return broadcaster.WithOnConnect(fn)
}

func WithOnDisconnect(fn func(r *http.Request, topics []string)) Option {
	return broadcaster.WithOnDisconnect(fn)
}

func WithHeader(key, value string) Option {
	return broadcaster.WithHeader(key, value)
}

func WithMaxTopicsPerConn(limit int) Option {
	return broadcaster.WithMaxTopicsPerConn(limit)
}

func WithMaxTopicLen(limit int) Option {
	return broadcaster.WithMaxTopicLen(limit)
}

func WithMaxConnections(limit int) Option {
	return broadcaster.WithMaxConnections(limit)
}

func WithAuthorizeTopics(fn func(r *http.Request, topics []string) error) Option {
	return broadcaster.WithAuthorizeTopics(fn)
}

func WithReplayBuffer(topicCap int) Option {
	return broadcaster.WithReplayBuffer(topicCap)
}
