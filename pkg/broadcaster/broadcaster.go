package broadcaster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/rlsvr/inas/pkg/queue"

	"github.com/rlsvr/blazesse/pkg/frame"
)

var (
	// ErrBroadcasterClosed is returned when publishing to a closed broadcaster.
	ErrBroadcasterClosed = errors.New("blazesse: broadcaster is closed")
)

type asyncJob struct {
	topic string
	frame *frame.Frame
}

type asyncShard struct {
	q *queue.Queue[asyncJob]
}

// engine abstracts internal topic subscription storage and lookup strategies.
type engine interface {
	subscribe(topics []string, sub Subscriber) func()
	publish(topic string, f *frame.Frame)
	publishAll(f *frame.Frame)
	activeSubscribers(topic string) int
	close()
}

// Broadcaster is a high-throughput SSE pub/sub broadcast engine.
// It delegates topic routing to a pluggable engine strategy (sharded mutex or lock-free CoW snapshot)
// while providing unified HTTP upgrading, mailbox lifecycle, metrics, and async fanout.
type Broadcaster struct {
	opts        Options
	engine      engine
	subCount    atomic.Int64
	closed      atomic.Bool
	asyncShards []*asyncShard
	asyncWg     sync.WaitGroup
	handlersWg  sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	replay      *replayManager
}

// New creates and initializes a Broadcaster with the provided options.
func New(opts ...Option) *Broadcaster {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &Broadcaster{
		opts:   options,
		ctx:    ctx,
		cancel: cancel,
	}

	if options.ReplayBufferCap > 0 {
		b.replay = newReplayManager(options.ReplayBufferCap)
	}

	if options.Engine == EngineLockFree {
		b.engine = newLockFreeEngine(ctx, options)
	} else {
		b.engine = newShardedMutexEngine(options)
	}

	// Initialize sharded async workers to guarantee per-topic FIFO ordering
	if options.AsyncWorkers > 0 {
		workers := options.AsyncWorkers
		b.asyncShards = make([]*asyncShard, workers)
		shardCap := options.AsyncQueueCap / workers
		if shardCap <= 0 {
			shardCap = 256
		}
		for i := 0; i < workers; i++ {
			shard := &asyncShard{
				q: queue.New[asyncJob](shardCap),
			}
			b.asyncShards[i] = shard
			b.asyncWg.Add(1)
			go func(s *asyncShard) {
				defer b.asyncWg.Done()
				for {
					job, err := s.q.Pop(ctx)
					if err != nil {
						return
					}
					b.publishSync(job.topic, job.frame)
				}
			}(shard)
		}
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
		sub.Close()
		return sub, func() {}
	}

	unsub := b.engine.subscribe(topics, sub)
	b.subCount.Add(1)

	var unsubOnce sync.Once
	return sub, func() {
		unsubOnce.Do(func() {
			unsub()
			b.subCount.Add(-1)
		})
	}
}

// Publish fans out an immutable Frame across all subscribers of the specified topic.
// Zero per-client allocations; never blocks on slow subscribers.
func (b *Broadcaster) Publish(topic string, f *frame.Frame) {
	if f == nil || f.Len() == 0 || b.closed.Load() {
		return
	}

	if b.replay != nil {
		b.replay.Push(topic, f)
	}

	if n := len(b.asyncShards); n > 0 {
		//nolint:gosec // n is bounded by configured worker count
		shardIdx := hashTopic(topic) % uint32(n)
		if !b.asyncShards[shardIdx].q.TryPush(asyncJob{topic: topic, frame: f}) {
			b.opts.Metrics.OnBufferOverflow(topic)
		}
		return
	}

	b.publishSync(topic, f)
}

func (b *Broadcaster) publishSync(topic string, f *frame.Frame) {
	b.engine.publish(topic, f)
}

// PublishAll broadcasts a Frame to all active subscribers across every topic and shard.
func (b *Broadcaster) PublishAll(f *frame.Frame) {
	if f == nil || f.Len() == 0 || b.closed.Load() {
		return
	}
	b.engine.publishAll(f)
}

// ActiveSubscribers returns the count of active subscribers on a specific topic.
func (b *Broadcaster) ActiveSubscribers(topic string) int {
	return b.engine.activeSubscribers(topic)
}

// TotalSubscribers returns the total count of connected subscribers across all topics.
func (b *Broadcaster) TotalSubscribers() int {
	return int(b.subCount.Load())
}

// Close gracefully closes the broadcaster and waits for all active HTTP handlers to exit.
func (b *Broadcaster) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	// Drain and wait for async shards before canceling context
	for _, s := range b.asyncShards {
		s.q.Close()
	}
	b.asyncWg.Wait()
	b.cancel()

	// Close engine and all subscriber mailboxes
	b.engine.close()

	// Wait for all in-flight ServeHTTP handlers to terminate
	b.handlersWg.Wait()

	return nil
}
