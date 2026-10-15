package broadcaster

import (
	"net/http"
	"time"
)

// DeliveryMode determines the subscriber mailbox strategy.
type DeliveryMode uint8

const (
	// DeliveryConflated (default) uses lock-free atomic pointer swaps to overwrite
	// unread frames in place. Slow consumers drop intermediate stale states and only
	// wake up for the freshest data. Zero allocations, zero publisher blocking.
	DeliveryConflated DeliveryMode = iota

	// DeliveryFIFO provides strict chronological ordering via a bounded queue.
	// Used for notifications, chat, and event logs where every event must be delivered.
	DeliveryFIFO
)

// OverflowPolicy defines what happens when a FIFO subscriber's queue reaches capacity.
type OverflowPolicy uint8

const (
	// OverflowDropOldest discards the oldest queued message in the buffer to make room.
	OverflowDropOldest OverflowPolicy = iota

	// OverflowDropNewest drops incoming messages until the buffer drains.
	OverflowDropNewest

	// OverflowDisconnect terminates the slow subscriber's connection immediately.
	OverflowDisconnect
)

// TopicExtractor extracts one or more topic strings from an incoming HTTP request.
type TopicExtractor func(r *http.Request) ([]string, error)

// Options holds configuration for Broadcaster and Stream.
type Options struct {
	Shards         int
	WriteTimeout   time.Duration
	PingInterval   time.Duration
	PingJitter     time.Duration
	DeliveryMode   DeliveryMode
	FIFOBufferSize int
	OverflowPolicy OverflowPolicy
	AsyncWorkers   int
	AsyncQueueCap  int
	TopicExtractor TopicExtractor
	Metrics        MetricsCollector
	OnConnect      func(r *http.Request) error
	OnDisconnect   func(r *http.Request, topics []string)
	CustomHeaders  map[string]string
}

// DefaultOptions returns production-ready default settings.
func DefaultOptions() Options {
	return Options{
		Shards:         64,
		WriteTimeout:   3 * time.Second,
		PingInterval:   20 * time.Second,
		PingJitter:     10 * time.Second,
		DeliveryMode:   DeliveryConflated,
		FIFOBufferSize: 512,
		OverflowPolicy: OverflowDropOldest,
		AsyncWorkers:   0, // synchronous fanout by default
		AsyncQueueCap:  10000,
		TopicExtractor: DefaultTopicExtractor,
		Metrics:        NoopMetrics{},
		CustomHeaders:  make(map[string]string),
	}
}

// Option configures Broadcaster behavior.
type Option func(*Options)

// WithShards sets the number of topic shards (must be a power of two, e.g. 32, 64, 128, 256).
func WithShards(n int) Option {
	return func(o *Options) {
		if n > 0 {
			// Round to nearest power of two
			p := 1
			for p < n {
				p <<= 1
			}
			o.Shards = p
		}
	}
}

// WithWriteTimeout sets the maximum duration allowed for a single message write and flush.
// If a client's TCP receive window is stalled longer than this timeout, the client is evicted.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.WriteTimeout = d
		}
	}
}

// WithHeartbeat sets the base keep-alive ping interval and maximum random jitter.
// Jitter randomizes ping ticks per connection to prevent thundering herds on the Go timer heap.
func WithHeartbeat(interval, jitter time.Duration) Option {
	return func(o *Options) {
		if interval > 0 {
			o.PingInterval = interval
		}
		o.PingJitter = jitter
	}
}

// WithDeliveryMode sets the subscriber mailbox strategy (DeliveryConflated or DeliveryFIFO).
func WithDeliveryMode(mode DeliveryMode) Option {
	return func(o *Options) {
		o.DeliveryMode = mode
	}
}

// WithFIFOBufferSize sets the channel/queue capacity for FIFO subscribers.
func WithFIFOBufferSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.FIFOBufferSize = size
		}
	}
}

// WithOverflowPolicy sets how full FIFO subscriber queues handle incoming frames.
func WithOverflowPolicy(policy OverflowPolicy) Option {
	return func(o *Options) {
		o.OverflowPolicy = policy
	}
}

// WithAsyncWorkers enables asynchronous decoupled fanout via background workers.
// The publisher thread pushes to a bounded queue in <50ns without waiting for fanout loops.
func WithAsyncWorkers(workers int, queueCap int) Option {
	return func(o *Options) {
		if workers > 0 {
			o.AsyncWorkers = workers
		}
		if queueCap > 0 {
			o.AsyncQueueCap = queueCap
		}
	}
}

// WithTopicExtractor sets a custom function to extract subscribed topics from the HTTP request.
func WithTopicExtractor(fn TopicExtractor) Option {
	return func(o *Options) {
		if fn != nil {
			o.TopicExtractor = fn
		}
	}
}

// WithMetrics sets a metrics collector for monitoring broadcaster activity.
func WithMetrics(m MetricsCollector) Option {
	return func(o *Options) {
		if m != nil {
			o.Metrics = m
		}
	}
}

// WithOnConnect sets an authentication or authorization hook executed before connection upgrade.
// If the hook returns an error, the connection is rejected with 401 or 403.
func WithOnConnect(fn func(r *http.Request) error) Option {
	return func(o *Options) {
		o.OnConnect = fn
	}
}

// WithOnDisconnect sets a callback executed when a client connection terminates.
func WithOnDisconnect(fn func(r *http.Request, topics []string)) Option {
	return func(o *Options) {
		o.OnDisconnect = fn
	}
}

// WithHeader adds a custom HTTP response header to every SSE response (e.g. CORS).
func WithHeader(key, value string) Option {
	return func(o *Options) {
		if o.CustomHeaders == nil {
			o.CustomHeaders = make(map[string]string)
		}
		o.CustomHeaders[key] = value
	}
}
