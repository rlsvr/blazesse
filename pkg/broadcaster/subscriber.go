package broadcaster

import (
	"sync/atomic"

	"github.com/rlsvr/blazesse/pkg/frame"
	"github.com/rlsvr/inas/pkg/queue"
)

// Subscriber represents a connected SSE client's message mailbox.
type Subscriber interface {
	// Push enqueues or updates the newest frame in the subscriber's mailbox.
	Push(f *frame.Frame) bool
	// Recv returns a notification channel that signals when one or more frames are ready.
	Recv() <-chan struct{}
	// Fetch atomically retrieves all ready frames and clears the mailbox.
	Fetch() []*frame.Frame
	// Close terminates the mailbox.
	Close()
	// IsClosed reports whether the subscriber is closed.
	IsClosed() bool
}

// ConflatingSubscriber implements a lock-free, zero-allocation mailbox for high-frequency updates.
// Instead of buffering multiple frames, it holds an atomic pointer to the latest frame.
// If the client's socket is slow and 50 updates arrive before the socket flushes,
// the atomic pointer simply overwrites 49 times: the client wakes up once and flushes
// only the freshest state.
type ConflatingSubscriber struct {
	latest  atomic.Pointer[frame.Frame]
	notify  chan struct{} // size 1: coalesces rapid updates into 1 wakeup
	closed  atomic.Bool
	metrics MetricsCollector
}

// NewConflatingSubscriber creates a ConflatingSubscriber.
func NewConflatingSubscriber(metrics MetricsCollector) *ConflatingSubscriber {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	return &ConflatingSubscriber{
		notify:  make(chan struct{}, 1),
		metrics: metrics,
	}
}

func (s *ConflatingSubscriber) Push(f *frame.Frame) bool {
	if s.closed.Load() {
		return false
	}

	// Atomically swap in the newest frame
	old := s.latest.Swap(f)
	if old != nil {
		s.metrics.OnFrameConflated("")
	}

	// Non-blocking wakeup signal
	select {
	case s.notify <- struct{}{}:
	default:
		// Already has a pending wakeup; the atomic Swap above conflated in place.
	}
	return true
}

func (s *ConflatingSubscriber) Recv() <-chan struct{} {
	return s.notify
}

func (s *ConflatingSubscriber) Fetch() []*frame.Frame {
	// Atomically retrieve the frame and set the pointer back to nil.
	// This immediately frees the reference so the GC can reclaim the Frame.
	f := s.latest.Swap(nil)
	if f == nil {
		return nil
	}
	return []*frame.Frame{f}
}

func (s *ConflatingSubscriber) Close() {
	if s.closed.Swap(true) {
		return
	}
	s.latest.Store(nil)
}

func (s *ConflatingSubscriber) IsClosed() bool {
	return s.closed.Load()
}

// FIFOSubscriber implements an ordered, bounded mailbox powered by github.com/rlsvr/hirnok/pkg/queue.
type FIFOSubscriber struct {
	q       *queue.Queue[*frame.Frame]
	notify  chan struct{}
	policy  OverflowPolicy
	closed  atomic.Bool
	metrics MetricsCollector
}

// NewFIFOSubscriber creates a FIFOSubscriber with bounded queue capacity and overflow policy.
func NewFIFOSubscriber(capacity int, policy OverflowPolicy, metrics MetricsCollector) *FIFOSubscriber {
	if capacity <= 0 {
		capacity = 512
	}
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	return &FIFOSubscriber{
		q:       queue.New[*frame.Frame](capacity),
		notify:  make(chan struct{}, 1),
		policy:  policy,
		metrics: metrics,
	}
}

func (s *FIFOSubscriber) Push(f *frame.Frame) bool {
	if s.closed.Load() {
		return false
	}

	if s.q.TryPush(f) {
		select {
		case s.notify <- struct{}{}:
		default:
		}
		return true
	}

	// Queue is full: apply overflow policy
	s.metrics.OnBufferOverflow("")

	switch s.policy {
	case OverflowDropNewest:
		return false

	case OverflowDropOldest:
		// Evict oldest item to make room
		select {
		case <-s.q.Recv():
		default:
		}
		_ = s.q.TryPush(f)
		select {
		case s.notify <- struct{}{}:
		default:
		}
		return true

	case OverflowDisconnect:
		s.Close()
		return false

	default:
		return false
	}
}

func (s *FIFOSubscriber) Recv() <-chan struct{} {
	return s.notify
}

func (s *FIFOSubscriber) Fetch() []*frame.Frame {
	var batch []*frame.Frame
	for {
		select {
		case f := <-s.q.Recv():
			batch = append(batch, f)
		default:
			return batch
		}
	}
}

func (s *FIFOSubscriber) Close() {
	if s.closed.Swap(true) {
		return
	}
	s.q.Close()
}

func (s *FIFOSubscriber) IsClosed() bool {
	return s.closed.Load()
}
