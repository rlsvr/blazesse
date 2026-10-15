package broadcaster

import (
	"sync"

	"github.com/rlsvr/blazesse/pkg/frame"
)

type replayItem struct {
	id    string
	frame *frame.Frame
}

// topicReplayRing is a bounded ring buffer for a single topic.
type topicReplayRing struct {
	mu    sync.RWMutex
	items []replayItem
	cap   int
}

func newTopicReplayRing(capacity int) *topicReplayRing {
	if capacity <= 0 {
		capacity = 128
	}
	return &topicReplayRing{
		items: make([]replayItem, 0, capacity),
		cap:   capacity,
	}
}

func (r *topicReplayRing) Push(f *frame.Frame) {
	if f == nil {
		return
	}
	id := f.ID()
	if id == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.items) >= r.cap {
		copy(r.items, r.items[1:])
		r.items[len(r.items)-1] = replayItem{id: id, frame: f}
	} else {
		r.items = append(r.items, replayItem{id: id, frame: f})
	}
}

func (r *topicReplayRing) ReplayAfter(lastID string) []*frame.Frame {
	if lastID == "" {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	foundIdx := -1
	for i, it := range r.items {
		if it.id == lastID {
			foundIdx = i
			break
		}
	}

	if foundIdx == -1 {
		// If lastID was evicted or not found, return all available buffered frames
		result := make([]*frame.Frame, len(r.items))
		for i, it := range r.items {
			result[i] = it.frame
		}
		return result
	}

	remaining := r.items[foundIdx+1:]
	if len(remaining) == 0 {
		return nil
	}
	result := make([]*frame.Frame, len(remaining))
	for i, it := range remaining {
		result[i] = it.frame
	}
	return result
}

// replayManager manages per-topic replay buffers.
type replayManager struct {
	mu       sync.RWMutex
	topics   map[string]*topicReplayRing
	topicCap int
}

func newReplayManager(topicCap int) *replayManager {
	if topicCap <= 0 {
		return nil
	}
	return &replayManager{
		topics:   make(map[string]*topicReplayRing),
		topicCap: topicCap,
	}
}

func (m *replayManager) Push(topic string, f *frame.Frame) {
	if m == nil || f == nil || f.ID() == "" {
		return
	}

	m.mu.RLock()
	ring, ok := m.topics[topic]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		ring, ok = m.topics[topic]
		if !ok {
			ring = newTopicReplayRing(m.topicCap)
			m.topics[topic] = ring
		}
		m.mu.Unlock()
	}

	ring.Push(f)
}

func (m *replayManager) ReplayAfter(topic, lastID string) []*frame.Frame {
	if m == nil || lastID == "" {
		return nil
	}

	m.mu.RLock()
	ring, ok := m.topics[topic]
	m.mu.RUnlock()

	if !ok {
		return nil
	}
	return ring.ReplayAfter(lastID)
}
