package broadcaster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/rlsvr/blazesse/pkg/frame"
	"github.com/rlsvr/inas/pkg/queue"
	"github.com/rlsvr/inas/pkg/worker"
)

var (
	// ErrBroadcasterClosed is returned when publishing to a closed broadcaster.
	ErrBroadcasterClosed = errors.New("blazesse: broadcaster is closed")
)

// shard isolates topic subscriptions and locks into independent buckets.
// Padded to prevent MESI false sharing across adjacent shards in CPU caches.
type shard struct {
	mu     sync.RWMutex
	topics map[string]map[Subscriber]struct{}
	_pad   [64]byte // ensures separation across CPU cache lines
}

// Broadcaster is a high-throughput, sharded SSE pub/sub broadcast engine.
type Broadcaster struct {
	opts       Options
	shards     []shard
	shardMask  uint32
	subCount   atomic.Int64
	closed     atomic.Bool
	asyncQueue *queue.Queue[asyncJob]
	asyncPool  *worker.Pool
	ctx        context.Context
	cancel     context.CancelFunc
}

type asyncJob struct {
	topic string
	frame *frame.Frame
}

// New creates and initializes a Broadcaster with the provided options.
func New(opts ...Option) *Broadcaster {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &Broadcaster{
		opts:      options,
		shards:    make([]shard, options.Shards),
		shardMask: uint32(options.Shards - 1),
		ctx:       ctx,
		cancel:    cancel,
	}

	for i := range b.shards {
		b.shards[i].topics = make(map[string]map[Subscriber]struct{})
	}

	// Initialize decoupled async fanout worker pool if configured
	if options.AsyncWorkers > 0 {
		b.asyncQueue = queue.New[asyncJob](options.AsyncQueueCap)
		b.asyncPool = worker.Run(ctx, options.AsyncWorkers, b.asyncQueue, func(_ context.Context, job asyncJob) {
			b.publishSync(job.topic, job.frame)
		})
	}

	return b
}

// hashTopic computes a zero-allocation 32-bit FNV-1a hash of the topic string.
func hashTopic(s string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	var h uint32 = offset32
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

func (b *Broadcaster) getShard(topic string) *shard {
	return &b.shards[hashTopic(topic)&b.shardMask]
}

// NewSubscriber creates a new mailbox Subscriber according to the configured DeliveryMode.
func (b *Broadcaster) NewSubscriber() Subscriber {
	if b.opts.DeliveryMode == DeliveryFIFO {
		return NewFIFOSubscriber(b.opts.FIFOBufferSize, b.opts.OverflowPolicy, b.opts.Metrics)
	}
	return NewConflatingSubscriber(b.opts.Metrics)
}

// Subscribe subscribes a new client to the given topic.
// Returns the subscriber mailbox and an idempotent unsubscribe teardown function.
func (b *Broadcaster) Subscribe(topic string) (Subscriber, func()) {
	return b.SubscribeMulti([]string{topic})
}

// SubscribeMulti subscribes a new client to multiple topics across shards.
func (b *Broadcaster) SubscribeMulti(topics []string) (Subscriber, func()) {
	sub := b.NewSubscriber()
	if b.closed.Load() {
		return sub, func() {}
	}

	for _, topic := range topics {
		if topic == "" {
			continue
		}
		s := b.getShard(topic)
		s.mu.Lock()
		subs, ok := s.topics[topic]
		if !ok {
			subs = make(map[Subscriber]struct{})
			s.topics[topic] = subs
		}
		subs[sub] = struct{}{}
		s.mu.Unlock()

		b.opts.Metrics.OnClientConnected(topic)
	}
	b.subCount.Add(1)

	var unsubOnce sync.Once
	unsubscribe := func() {
		unsubOnce.Do(func() {
			for _, topic := range topics {
				if topic == "" {
					continue
				}
				s := b.getShard(topic)
				s.mu.Lock()
				if subs, ok := s.topics[topic]; ok {
					delete(subs, sub)
					if len(subs) == 0 {
						delete(s.topics, topic)
					}
				}
				s.mu.Unlock()

				b.opts.Metrics.OnClientDisconnected(topic)
			}
			sub.Close()
			b.subCount.Add(-1)
		})
	}

	return sub, unsubscribe
}

// Publish fans out an immutable Frame across all subscribers of the specified topic.
// Zero per-client allocations; never blocks on slow subscribers.
func (b *Broadcaster) Publish(topic string, f *frame.Frame) {
	if f == nil || f.Len() == 0 || b.closed.Load() {
		return
	}

	if b.asyncQueue != nil {
		// Asynchronous decoupled publish
		_ = b.asyncQueue.TryPush(asyncJob{topic: topic, frame: f})
		return
	}

	b.publishSync(topic, f)
}

func (b *Broadcaster) publishSync(topic string, f *frame.Frame) {
	s := b.getShard(topic)
	s.mu.RLock()
	subs := s.topics[topic]
	for client := range subs {
		client.Push(f)
	}
	s.mu.RUnlock()

	b.opts.Metrics.OnFramePublished(topic, f.Len())
}

// PublishAll broadcasts a Frame to all active subscribers across every topic and shard.
func (b *Broadcaster) PublishAll(f *frame.Frame) {
	if f == nil || f.Len() == 0 || b.closed.Load() {
		return
	}

	for i := range b.shards {
		s := &b.shards[i]
		s.mu.RLock()
		for _, subs := range s.topics {
			for client := range subs {
				client.Push(f)
			}
		}
		s.mu.RUnlock()
	}
}

// ActiveSubscribers returns the count of active subscribers on a specific topic.
func (b *Broadcaster) ActiveSubscribers(topic string) int {
	s := b.getShard(topic)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.topics[topic])
}

// TotalSubscribers returns the total count of connected subscribers across all topics.
func (b *Broadcaster) TotalSubscribers() int {
	return int(b.subCount.Load())
}

// Close gracefully closes the broadcaster and any async background workers.
func (b *Broadcaster) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	b.cancel()

	if b.asyncQueue != nil {
		b.asyncQueue.Close()
	}
	if b.asyncPool != nil {
		_ = b.asyncPool.Wait(context.Background())
	}

	// Close all active subscribers across all shards
	for i := range b.shards {
		s := &b.shards[i]
		s.mu.Lock()
		for _, subs := range s.topics {
			for client := range subs {
				client.Close()
			}
		}
		s.topics = make(map[string]map[Subscriber]struct{})
		s.mu.Unlock()
	}

	return nil
}
