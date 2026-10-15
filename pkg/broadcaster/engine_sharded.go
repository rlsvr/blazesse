package broadcaster

import (
	"sync"

	"github.com/rlsvr/blazesse/pkg/frame"
)

// shard isolates topic subscriptions and locks into independent buckets.
// Padded to prevent MESI false sharing across adjacent shards in CPU caches.
type shard struct {
	mu     sync.RWMutex
	topics map[string]map[Subscriber]struct{}
	_pad   [64]byte // ensures separation across CPU cache lines
}

// shardedMutexEngine partitions topics across 64 CPU-padded RWMutex shards.
// Provides immediate 0ms subscription registration and in-place map mutation.
// Best suited for low-to-medium churn or write-heavy workloads.
type shardedMutexEngine struct {
	opts      Options
	shards    []shard
	shardMask uint32
}

func newShardedMutexEngine(opts Options) *shardedMutexEngine {
	e := &shardedMutexEngine{
		opts:      opts,
		shards:    make([]shard, opts.Shards),
		shardMask: uint32(opts.Shards - 1),
	}
	for i := range e.shards {
		e.shards[i].topics = make(map[string]map[Subscriber]struct{})
	}
	return e
}

func (e *shardedMutexEngine) getShard(topic string) *shard {
	return &e.shards[hashTopic(topic)&e.shardMask]
}

func (e *shardedMutexEngine) subscribe(topics []string, sub Subscriber) func() {
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		s := e.getShard(topic)
		s.mu.Lock()
		subs, ok := s.topics[topic]
		if !ok {
			subs = make(map[Subscriber]struct{})
			s.topics[topic] = subs
		}
		subs[sub] = struct{}{}
		s.mu.Unlock()

		e.opts.Metrics.OnClientConnected(topic)
	}

	var unsubOnce sync.Once
	return func() {
		unsubOnce.Do(func() {
			for _, topic := range topics {
				if topic == "" {
					continue
				}
				s := e.getShard(topic)
				s.mu.Lock()
				if subs, ok := s.topics[topic]; ok {
					delete(subs, sub)
					if len(subs) == 0 {
						delete(s.topics, topic)
					}
				}
				s.mu.Unlock()

				e.opts.Metrics.OnClientDisconnected(topic)
			}
			sub.Close()
		})
	}
}

func (e *shardedMutexEngine) publish(topic string, f *frame.Frame) {
	s := e.getShard(topic)
	s.mu.RLock()
	subs := s.topics[topic]
	for client := range subs {
		client.Push(f)
	}
	s.mu.RUnlock()

	e.opts.Metrics.OnFramePublished(topic, f.Len())
}

func (e *shardedMutexEngine) publishAll(f *frame.Frame) {
	for i := range e.shards {
		s := &e.shards[i]
		s.mu.RLock()
		for _, subs := range s.topics {
			for client := range subs {
				client.Push(f)
			}
		}
		s.mu.RUnlock()
	}
}

func (e *shardedMutexEngine) activeSubscribers(topic string) int {
	s := e.getShard(topic)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.topics[topic])
}

func (e *shardedMutexEngine) close() {
	for i := range e.shards {
		s := &e.shards[i]
		s.mu.Lock()
		for _, subs := range s.topics {
			for client := range subs {
				client.Close()
			}
		}
		s.topics = make(map[string]map[Subscriber]struct{})
		s.mu.Unlock()
	}
}
