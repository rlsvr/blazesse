package broadcaster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

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

type topicSnapshot struct {
	topics map[string]map[Subscriber]struct{}
}

func newEmptySnapshot() *topicSnapshot {
	return &topicSnapshot{
		topics: make(map[string]map[Subscriber]struct{}),
	}
}

type reconfigOp uint8

const (
	opSub reconfigOp = iota
	opUnsub
)

type reconfigReq struct {
	op     reconfigOp
	topics []string
	sub    Subscriber
	done   chan struct{}
}

// Broadcaster is a high-throughput SSE pub/sub broadcast engine supporting both
// cache-line padded sharded mutexes and lock-free copy-on-write snapshotting.
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

	// Lock-free engine state (when Engine == EngineLockFree)
	currentSnap atomic.Pointer[topicSnapshot]
	reconfigCh  chan reconfigReq
	actorDone   chan struct{}
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
		opts:   options,
		ctx:    ctx,
		cancel: cancel,
	}

	if options.Engine == EngineLockFree {
		b.reconfigCh = make(chan reconfigReq, 4096)
		b.actorDone = make(chan struct{})
		b.currentSnap.Store(newEmptySnapshot())
		go b.runReconfigActor()
	} else {
		b.shards = make([]shard, options.Shards)
		b.shardMask = uint32(options.Shards - 1)
		for i := range b.shards {
			b.shards[i].topics = make(map[string]map[Subscriber]struct{})
		}
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

// SubscribeMulti subscribes a new client to multiple topics across shards or into the lock-free snapshot.
func (b *Broadcaster) SubscribeMulti(topics []string) (Subscriber, func()) {
	sub := b.NewSubscriber()
	if b.closed.Load() {
		return sub, func() {}
	}

	if b.opts.Engine == EngineLockFree {
		return b.subscribeMultiLockFree(topics, sub)
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

func (b *Broadcaster) subscribeMultiLockFree(topics []string, sub Subscriber) (Subscriber, func()) {
	cleanTopics := make([]string, 0, len(topics))
	for _, t := range topics {
		if t != "" {
			cleanTopics = append(cleanTopics, t)
		}
	}

	var done chan struct{}
	if b.opts.ReconfigWait {
		done = make(chan struct{})
	}

	select {
	case b.reconfigCh <- reconfigReq{op: opSub, topics: cleanTopics, sub: sub, done: done}:
		if done != nil {
			<-done
		}
	case <-b.ctx.Done():
		return sub, func() {}
	}

	for _, t := range cleanTopics {
		b.opts.Metrics.OnClientConnected(t)
	}
	b.subCount.Add(1)

	var unsubOnce sync.Once
	unsubscribe := func() {
		unsubOnce.Do(func() {
			var unsubDone chan struct{}
			if b.opts.ReconfigWait {
				unsubDone = make(chan struct{})
			}

			select {
			case b.reconfigCh <- reconfigReq{op: opUnsub, topics: cleanTopics, sub: sub, done: unsubDone}:
				if unsubDone != nil {
					<-unsubDone
				}
			case <-b.ctx.Done():
			}

			sub.Close()
			b.subCount.Add(-1)
			for _, t := range cleanTopics {
				b.opts.Metrics.OnClientDisconnected(t)
			}
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
	if b.opts.Engine == EngineLockFree {
		snap := b.currentSnap.Load()
		if snap == nil {
			return
		}
		subs := snap.topics[topic]
		for client := range subs {
			client.Push(f)
		}
		b.opts.Metrics.OnFramePublished(topic, f.Len())
		return
	}

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

	if b.opts.Engine == EngineLockFree {
		snap := b.currentSnap.Load()
		if snap == nil {
			return
		}
		seen := make(map[Subscriber]struct{})
		for _, subs := range snap.topics {
			for client := range subs {
				if _, ok := seen[client]; !ok {
					seen[client] = struct{}{}
					client.Push(f)
				}
			}
		}
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
	if b.opts.Engine == EngineLockFree {
		snap := b.currentSnap.Load()
		if snap == nil {
			return 0
		}
		return len(snap.topics[topic])
	}

	s := b.getShard(topic)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.topics[topic])
}

// TotalSubscribers returns the total count of connected subscribers across all topics.
func (b *Broadcaster) TotalSubscribers() int {
	return int(b.subCount.Load())
}

func (b *Broadcaster) runReconfigActor() {
	defer close(b.actorDone)

	interval := b.opts.ReconfigInterval
	if interval <= 0 {
		interval = 15 * time.Millisecond
	}
	batchCap := b.opts.ReconfigBatchSize
	if batchCap <= 0 {
		batchCap = 128
	}

	batch := make([]reconfigReq, 0, batchCap)
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case req := <-b.reconfigCh:
			batch = append(batch, req)
			if len(batch) >= batchCap {
				b.applyReconfigBatch(batch)
				batch = batch[:0]
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(interval)
			}
		case <-timer.C:
			if len(batch) > 0 {
				b.applyReconfigBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(interval)
		}
	}
}

func (b *Broadcaster) applyReconfigBatch(batch []reconfigReq) {
	current := b.currentSnap.Load()

	// Shallow clone of topic map pointers
	nextTopics := make(map[string]map[Subscriber]struct{}, len(current.topics)+len(batch))
	for t, subs := range current.topics {
		nextTopics[t] = subs
	}

	cloned := make(map[string]bool)
	cloneTopicIfNeeded := func(topic string) {
		if cloned[topic] {
			if nextTopics[topic] == nil {
				nextTopics[topic] = make(map[Subscriber]struct{})
			}
			return
		}
		orig := nextTopics[topic]
		if orig == nil {
			nextTopics[topic] = make(map[Subscriber]struct{})
			cloned[topic] = true
			return
		}
		topicCopy := make(map[Subscriber]struct{}, len(orig)+1)
		for s := range orig {
			topicCopy[s] = struct{}{}
		}
		nextTopics[topic] = topicCopy
		cloned[topic] = true
	}

	for _, req := range batch {
		switch req.op {
		case opSub:
			for _, t := range req.topics {
				cloneTopicIfNeeded(t)
				nextTopics[t][req.sub] = struct{}{}
			}
		case opUnsub:
			for _, t := range req.topics {
				cloneTopicIfNeeded(t)
				delete(nextTopics[t], req.sub)
				if len(nextTopics[t]) == 0 {
					delete(nextTopics, t)
				}
			}
		}

		if req.done != nil {
			close(req.done)
		}
	}

	b.currentSnap.Store(&topicSnapshot{topics: nextTopics})
}

// Close gracefully closes the broadcaster and any async background workers.
func (b *Broadcaster) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	b.cancel()

	if b.actorDone != nil {
		<-b.actorDone
	}

	if b.asyncQueue != nil {
		b.asyncQueue.Close()
	}
	if b.asyncPool != nil {
		_ = b.asyncPool.Wait(context.Background())
	}

	if b.opts.Engine == EngineLockFree {
		snap := b.currentSnap.Load()
		if snap != nil {
			for _, subs := range snap.topics {
				for client := range subs {
					client.Close()
				}
			}
		}
		b.currentSnap.Store(newEmptySnapshot())
		return nil
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
