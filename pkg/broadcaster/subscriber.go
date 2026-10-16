package broadcaster

import (
	"sync"
	"sync/atomic"

	"github.com/rlsvr/inas/pkg/queue"

	"github.com/rlsvr/blazesse/pkg/frame"
)

//go:generate mockgen -destination=mock/mock_subscriber.go -package=mock github.com/rlsvr/blazesse/pkg/broadcaster Subscriber

// Subscriber represents a connected SSE client's message mailbox.
type Subscriber interface {
	// Push enqueues or updates the newest frame in the subscriber's mailbox for the specified topic.
	Push(topic string, f *frame.Frame) bool
	// Recv returns a notification channel that signals when one or more frames are ready.
	Recv() <-chan struct{}
	// Done returns a channel that is closed when the subscriber is closed / disconnected.
	Done() <-chan struct{}
	// Fetch atomically retrieves all ready frames and clears the mailbox.
	// The returned slice is valid until the next invocation of Fetch.
	Fetch() []*frame.Frame
	// Close terminates the mailbox.
	Close()
	// IsClosed reports whether the subscriber is closed.
	IsClosed() bool
}

// ConflatingSubscriber implements a lock-free, zero-allocation mailbox for high-frequency updates.
// For single-topic subscriptions, it uses an atomic pointer swap.
// For multi-topic subscriptions, it maintains independent per-topic slots so updates on
// topicB never overwrite unread state from topicA.
type ConflatingSubscriber struct {
	latest      atomic.Pointer[frame.Frame] // single-topic fast path
	singleTopic atomic.Pointer[string]      // initialized once for single-topic mode
	hasMultiple atomic.Bool
	slotsMu     sync.RWMutex
	slots       map[string]*atomic.Pointer[frame.Frame] // per-topic latest state slots
	topicOrder  []string                                // deterministic iteration order for multi-topic
	notify      chan struct{}                           // size 1: coalesces rapid updates into 1 wakeup
	done        chan struct{}                           // closed when mailbox is closed
	closeOnce   sync.Once
	closed      atomic.Bool
	metrics     MetricsCollector
	singleBuf   [1]*frame.Frame // zero-allocation backing array for single topic
	multiBuf    []*frame.Frame  // reusable slice for multi topic
}

// NewConflatingSubscriber creates a ConflatingSubscriber.
func NewConflatingSubscriber(metrics MetricsCollector, topics ...string) *ConflatingSubscriber {
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	s := &ConflatingSubscriber{
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		metrics: metrics,
	}

	if len(topics) > 1 {
		s.hasMultiple.Store(true)
		s.slots = make(map[string]*atomic.Pointer[frame.Frame], len(topics))
		s.topicOrder = make([]string, 0, len(topics))
		for _, t := range topics {
			if _, exists := s.slots[t]; !exists {
				s.slots[t] = &atomic.Pointer[frame.Frame]{}
				s.topicOrder = append(s.topicOrder, t)
			}
		}
		s.multiBuf = make([]*frame.Frame, 0, len(s.topicOrder))
	} else if len(topics) == 1 {
		t := topics[0]
		s.singleTopic.Store(&t)
	}

	return s
}

func (s *ConflatingSubscriber) Push(topic string, f *frame.Frame) bool {
	if s.closed.Load() {
		return false
	}

	if !s.hasMultiple.Load() {
		stPtr := s.singleTopic.Load()
		if stPtr == nil && topic != "" {
			t := topic
			s.singleTopic.CompareAndSwap(nil, &t)
			stPtr = s.singleTopic.Load()
		}
		if stPtr == nil || *stPtr == topic || topic == "" {
			old := s.latest.Swap(f)
			if old != nil {
				s.metrics.OnFrameConflated(topic)
			}
			s.signal()
			return true
		}
		// Multi-topic detected: promote to per-topic slots
		s.promoteToMulti(topic, f)
		s.signal()
		return true
	}

	// Multi-topic path
	s.slotsMu.RLock()
	slot, ok := s.slots[topic]
	s.slotsMu.RUnlock()

	if !ok {
		s.slotsMu.Lock()
		slot = s.getOrCreateSlotLocked(topic)
		s.slotsMu.Unlock()
	}

	old := slot.Swap(f)
	if old != nil {
		s.metrics.OnFrameConflated(topic)
	}

	s.signal()
	return true
}

func (s *ConflatingSubscriber) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *ConflatingSubscriber) getOrCreateSlotLocked(topic string) *atomic.Pointer[frame.Frame] {
	slot, ok := s.slots[topic]
	if !ok {
		slot = &atomic.Pointer[frame.Frame]{}
		s.slots[topic] = slot
		s.topicOrder = append(s.topicOrder, topic)
	}
	return slot
}

func (s *ConflatingSubscriber) promoteToMulti(topic string, f *frame.Frame) {
	s.slotsMu.Lock()
	defer s.slotsMu.Unlock()

	if s.hasMultiple.Load() {
		s.getOrCreateSlotLocked(topic).Store(f)
		return
	}

	s.slots = make(map[string]*atomic.Pointer[frame.Frame], 4)
	s.topicOrder = make([]string, 0, 4)

	prevTopic := "_prev"
	if stPtr := s.singleTopic.Load(); stPtr != nil && *stPtr != "" {
		prevTopic = *stPtr
	}

	prev := s.latest.Swap(nil)
	if prev != nil || prevTopic != "_prev" {
		s.getOrCreateSlotLocked(prevTopic).Store(prev)
	}

	s.getOrCreateSlotLocked(topic).Store(f)
	s.hasMultiple.Store(true)
}

func (s *ConflatingSubscriber) Recv() <-chan struct{} {
	return s.notify
}

func (s *ConflatingSubscriber) Done() <-chan struct{} {
	return s.done
}

// Fetch atomically retrieves all ready frames and clears the mailbox.
// The returned slice is valid until the next call to Fetch.
func (s *ConflatingSubscriber) Fetch() []*frame.Frame {
	if !s.hasMultiple.Load() {
		f := s.latest.Swap(nil)
		if f == nil {
			s.singleBuf[0] = nil
			return nil
		}
		s.singleBuf[0] = f
		return s.singleBuf[:1]
	}

	clear(s.multiBuf)
	s.multiBuf = s.multiBuf[:0]
	s.slotsMu.RLock()
	defer s.slotsMu.RUnlock()

	for _, t := range s.topicOrder {
		if slot, ok := s.slots[t]; ok {
			if f := slot.Swap(nil); f != nil {
				s.multiBuf = append(s.multiBuf, f)
			}
		}
	}

	if len(s.multiBuf) == 0 {
		return nil
	}
	return s.multiBuf
}

func (s *ConflatingSubscriber) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.latest.Store(nil)
		s.slotsMu.Lock()
		for _, slot := range s.slots {
			slot.Store(nil)
		}
		s.slotsMu.Unlock()
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

func (s *FIFOSubscriber) Push(topic string, f *frame.Frame) bool {
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
	s.metrics.OnBufferOverflow(topic)

	switch s.policy {
	case OverflowDropNewest:
		return false

	case OverflowDropOldest:
		// Evict oldest item to make room, with bounded retry under multi-publisher contention
		for attempt := 0; attempt < 3; attempt++ {
			if s.closed.Load() {
				return false
			}
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
	clear(s.buf)
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
