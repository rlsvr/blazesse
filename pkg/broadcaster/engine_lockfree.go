package broadcaster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlsvr/blazesse/pkg/frame"
)

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

// lockFreeEngine uses an actor-based Copy-On-Write topic snapshot (atomic.Pointer).
// Reads during Publish are 100% lock-free (single MOV instruction).
// Reconfiguration mutations (sub/unsub/switch) are batched asynchronously, completely
// eliminating writer lock contention and fan-out pauses.
type lockFreeEngine struct {
	opts        Options
	ctx         context.Context
	currentSnap atomic.Pointer[topicSnapshot]
	reconfigCh  chan reconfigReq
	actorDone   chan struct{}
}

func newLockFreeEngine(ctx context.Context, opts Options) *lockFreeEngine {
	e := &lockFreeEngine{
		opts:       opts,
		ctx:        ctx,
		reconfigCh: make(chan reconfigReq, 4096),
		actorDone:  make(chan struct{}),
	}
	e.currentSnap.Store(newEmptySnapshot())
	go e.runReconfigActor()
	return e
}

func (e *lockFreeEngine) subscribe(topics []string, sub Subscriber) func() {
	cleanTopics := make([]string, 0, len(topics))
	for _, t := range topics {
		if t != "" {
			cleanTopics = append(cleanTopics, t)
		}
	}

	var done chan struct{}
	if e.opts.ReconfigWait {
		done = make(chan struct{})
	}

	select {
	case e.reconfigCh <- reconfigReq{op: opSub, topics: cleanTopics, sub: sub, done: done}:
		if done != nil {
			select {
			case <-done:
			case <-e.ctx.Done():
				sub.Close()
				return func() {}
			}
		}
	case <-e.ctx.Done():
		sub.Close()
		return func() {}
	}

	for _, t := range cleanTopics {
		e.opts.Metrics.OnClientConnected(t)
	}

	var unsubOnce sync.Once
	return func() {
		unsubOnce.Do(func() {
			var unsubDone chan struct{}
			if e.opts.ReconfigWait {
				unsubDone = make(chan struct{})
			}

			select {
			case e.reconfigCh <- reconfigReq{op: opUnsub, topics: cleanTopics, sub: sub, done: unsubDone}:
				if unsubDone != nil {
					select {
					case <-unsubDone:
					case <-e.ctx.Done():
					}
				}
			case <-e.ctx.Done():
			}

			sub.Close()
			for _, t := range cleanTopics {
				e.opts.Metrics.OnClientDisconnected(t)
			}
		})
	}
}

func (e *lockFreeEngine) publish(topic string, f *frame.Frame) {
	snap := e.currentSnap.Load()
	if snap == nil {
		return
	}
	subs := snap.topics[topic]
	for client := range subs {
		client.Push(f)
	}
	e.opts.Metrics.OnFramePublished(topic, f.Len())
}

func (e *lockFreeEngine) publishAll(f *frame.Frame) {
	snap := e.currentSnap.Load()
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
}

func (e *lockFreeEngine) activeSubscribers(topic string) int {
	snap := e.currentSnap.Load()
	if snap == nil {
		return 0
	}
	return len(snap.topics[topic])
}

func (e *lockFreeEngine) runReconfigActor() {
	defer close(e.actorDone)

	interval := e.opts.ReconfigInterval
	if interval <= 0 {
		interval = 15 * time.Millisecond
	}
	batchCap := e.opts.ReconfigBatchSize
	if batchCap <= 0 {
		batchCap = 128
	}

	batch := make([]reconfigReq, 0, batchCap)
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-e.ctx.Done():
			// Cleanly drain in-flight batch and queued requests so no callers deadlock
			for _, req := range batch {
				if req.done != nil {
					close(req.done)
				}
			}
			for {
				select {
				case req := <-e.reconfigCh:
					if req.done != nil {
						close(req.done)
					}
				default:
					return
				}
			}

		case req := <-e.reconfigCh:
			batch = append(batch, req)
			if len(batch) >= batchCap || req.done != nil {
				e.applyReconfigBatch(batch)
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
				e.applyReconfigBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(interval)
		}
	}
}

func (e *lockFreeEngine) applyReconfigBatch(batch []reconfigReq) {
	current := e.currentSnap.Load()

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
			for _, topic := range req.topics {
				cloneTopicIfNeeded(topic)
				nextTopics[topic][req.sub] = struct{}{}
			}
		case opUnsub:
			for _, topic := range req.topics {
				if nextTopics[topic] != nil {
					cloneTopicIfNeeded(topic)
					delete(nextTopics[topic], req.sub)
					if len(nextTopics[topic]) == 0 {
						delete(nextTopics, topic)
					}
				}
			}
		}
	}

	e.currentSnap.Store(&topicSnapshot{topics: nextTopics})

	for _, req := range batch {
		if req.done != nil {
			close(req.done)
		}
	}
}

func (e *lockFreeEngine) close() {
	if e.actorDone != nil {
		<-e.actorDone
	}
	snap := e.currentSnap.Load()
	if snap != nil {
		for _, subs := range snap.topics {
			for client := range subs {
				client.Close()
			}
		}
	}
	e.currentSnap.Store(newEmptySnapshot())
}
