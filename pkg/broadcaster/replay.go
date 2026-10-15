package broadcaster

import (
	"sync"

	"github.com/rlsvr/blazesse/pkg/frame"
)

type replayItem struct {
	id    string
	frame *frame.Frame
}

// topicReplayRing is a bounded O(1) circular ring buffer for a single topic.
type topicReplayRing struct {
	mu     sync.RWMutex
	items  []replayItem
	start  int
	count  int
	cap    int
	policy ReplayMissPolicy
}

func newTopicReplayRing(capacity int, policy ReplayMissPolicy) *topicReplayRing {
	if capacity <= 0 {
		capacity = 128
	}
	return &topicReplayRing{
		items:  make([]replayItem, capacity),
		cap:    capacity,
		policy: policy,
	}
}

// Push appends a frame to the circular ring in O(1) time without slice shifting.
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

	item := replayItem{id: id, frame: f}
	if r.count < r.cap {
		idx := (r.start + r.count) % r.cap
		r.items[idx] = item
		r.count++
	} else {
		// Ring full: overwrite oldest at start and advance start index in O(1)
		r.items[r.start] = item
		r.start = (r.start + 1) % r.cap
	}
}

// ReplayAfter returns all frames in chronological order after lastID.
// If lastID is not found, it applies the configured ReplayMissPolicy.
func (r *topicReplayRing) ReplayAfter(lastID string) []*frame.Frame {
	if lastID == "" {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.count == 0 {
		return nil
	}

	foundIdx := -1
	for i := 0; i < r.count; i++ {
		idx := (r.start + i) % r.cap
		if r.items[idx].id == lastID {
			foundIdx = i
			break
		}
	}

	if foundIdx == -1 {
		if r.policy == ReplayMissNone {
			return nil
		}
		// ReplayMissAll: return all available buffered frames in chronological order
		result := make([]*frame.Frame, r.count)
		for i := 0; i < r.count; i++ {
			result[i] = r.items[(r.start+i)%r.cap].frame
		}
		return result
	}

	remainingCount := r.count - (foundIdx + 1)
	if remainingCount <= 0 {
		return nil
	}

	result := make([]*frame.Frame, remainingCount)
	for i := 0; i < remainingCount; i++ {
		result[i] = r.items[(r.start+foundIdx+1+i)%r.cap].frame
	}
	return result
}

// replayManager manages per-topic replay buffers with bounded topic retention.
type replayManager struct {
	mu         sync.RWMutex
	topics     map[string]*topicReplayRing
	topicOrder []string // FIFO tracker for evicting oldest topic when maxTopics exceeded
	topicCap   int
	missPolicy ReplayMissPolicy
	maxTopics  int
}

func newReplayManager(topicCap int, missPolicy ReplayMissPolicy, maxTopics int) *replayManager {
	if topicCap <= 0 {
		return nil
	}
	if maxTopics <= 0 {
		maxTopics = 10000
	}
	return &replayManager{
		topics:     make(map[string]*topicReplayRing),
		topicCap:   topicCap,
		missPolicy: missPolicy,
		maxTopics:  maxTopics,
	}
}

func (m *replayManager) Push(topic string, f *frame.Frame) {
	if m == nil || f == nil || f.ID() == "" || topic == "" {
		return
	}

	m.mu.RLock()
	ring, ok := m.topics[topic]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		ring, ok = m.topics[topic]
		if !ok {
			// Evict oldest topic ring if maxTopics limit reached
			if len(m.topics) >= m.maxTopics && len(m.topicOrder) > 0 {
				oldest := m.topicOrder[0]
				m.topicOrder = m.topicOrder[1:]
				delete(m.topics, oldest)
			}
			ring = newTopicReplayRing(m.topicCap, m.missPolicy)
			m.topics[topic] = ring
			m.topicOrder = append(m.topicOrder, topic)
		}
		m.mu.Unlock()
	}

	ring.Push(f)
}

func (m *replayManager) ReplayAfter(topic, lastID string) []*frame.Frame {
	if m == nil || lastID == "" || topic == "" {
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
