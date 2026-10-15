package broadcaster

import (
	"sync"
	"sync/atomic"

	"github.com/rlsvr/inas/pkg/queue"

	"github.com/rlsvr/blazesse/pkg/frame"
)

// Subscriber represents a connected SSE client's message mailbox.
type Subscriber interface {
	// Push enqueues or updates the newest frame in the subscriber's mailbox.
	Push(f *frame.Frame) bool
	// Recv returns a notification channel that signals when one or more frames are ready.
	Recv() <-chan struct{}
	// Done returns a channel that is closed when the subscriber is closed / disconnected.
	Done() <-chan struct{}
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
	latest    atomic.Pointer[frame.Frame]
	notify    chan struct{} // size 1: coalesces rapid updates into 1 wakeup
	done      chan struct{} // closed when mailbox is closed
	closeOnce sync.Once
	closed    atomic.Bool
	metrics   MetricsCollector
	buf       [1]*frame.Frame // reusable slice backing buffer for zero-alloc Fetch()
}

// NewConflatingSubscriber creates a ConflatingSubscriber.
func NewConflatingSubscriber(metrics MetricsCollector) *ConflatingSubscriber {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	return &ConflatingSubscriber{
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
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

func (s *ConflatingSubscriber) Done() <-chan struct{} {
	return s.done
}

func (s *ConflatingSubscriber) Fetch() []*frame.Frame {
	// Atomically retrieve the frame and set the pointer back to nil.
	// Reuses s.buf[:1] to achieve zero-allocations per delivery.
	f := s.latest.Swap(nil)
	if f == nil {
		return nil
	}
	s.buf[0] = f
	return s.buf[:1]
}

func (s *ConflatingSubscriber) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.latest.Store(nil)
		close(s.done)
		select {
		case s.notify <- struct{}{}:
		default:
		}
	})
}

func (s *ConflatingSubscriber) IsClosed() bool {
	return s.closed.Load()
}

// FIFOSubscriber implements an ordered, bounded mailbox powered by github.com/rlsvr/inas/pkg/queue.
type FIFOSubscriber struct {
	q         *queue.Queue[*frame.Frame]
	notify    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	policy    OverflowPolicy
	closed    atomic.Bool
	metrics   MetricsCollector
	buf       []*frame.Frame
}

// NewFIFOSubscriber creates a FIFOSubscriber with bounded queue capacity and overflow policy.
func NewFIFOSubscriber(capacity int, policy OverflowPolicy, metrics MetricsCollector) *FIFOSubscriber {
	if capacity <= 0 {
		capacity = 512
	}
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	initBufCap := 32
	if capacity < initBufCap {
		initBufCap = capacity
	}
	return &FIFOSubscriber{
		q:       queue.New[*frame.Frame](capacity),
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		policy:  policy,
		metrics: metrics,
		buf:     make([]*frame.Frame, 0, initBufCap),
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
		if s.q.TryPush(f) {
			select {
			case s.notify <- struct{}{}:
			default:
			}
			return true
		}
		return false

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

func (s *FIFOSubscriber) Done() <-chan struct{} {
	return s.done
}

func (s *FIFOSubscriber) Fetch() []*frame.Frame {
	s.buf = s.buf[:0]
	for {
		select {
		case f := <-s.q.Recv():
			s.buf = append(s.buf, f)
		default:
			if len(s.buf) == 0 {
				return nil
			}
			return s.buf
		}
	}
}

func (s *FIFOSubscriber) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.q.Close()
		close(s.done)
		select {
		case s.notify <- struct{}{}:
		default:
		}
	})
}

func (s *FIFOSubscriber) IsClosed() bool {
	return s.closed.Load()
}
