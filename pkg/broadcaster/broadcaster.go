package broadcaster

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/rlsvr/blazesse/pkg/frame"
	"github.com/rlsvr/inas/pkg/queue"
	"github.com/rlsvr/inas/pkg/worker"
)

var (
	// ErrBroadcasterClosed is returned when publishing to a closed broadcaster.
	ErrBroadcasterClosed = errors.New("blazesse: broadcaster is closed")
)

type asyncJob struct {
	topic string
	frame *frame.Frame
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
	opts       Options
	engine     engine
	subCount   atomic.Int64
	closed     atomic.Bool
	asyncQueue *queue.Queue[asyncJob]
	asyncPool  *worker.Pool
	ctx        context.Context
	cancel     context.CancelFunc
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

	if options.Engine == EngineLockFree {
		b.engine = newLockFreeEngine(ctx, options)
	} else {
		b.engine = newShardedMutexEngine(options)
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

// SubscribeMulti subscribes a new client to multiple topics across shards or into the lock-free snapshot.
func (b *Broadcaster) SubscribeMulti(topics []string) (Subscriber, func()) {
	sub := b.NewSubscriber()
	if b.closed.Load() {
		return sub, func() {}
	}

	engineUnsub := b.engine.subscribe(topics, sub)
	b.subCount.Add(1)

	var unsubOnce atomic.Bool
	unsubscribe := func() {
		if unsubOnce.CompareAndSwap(false, true) {
			engineUnsub()
			b.subCount.Add(-1)
		}
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

	b.engine.close()
	return nil
}
