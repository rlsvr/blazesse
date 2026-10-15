package broadcaster

import (
	"fmt"
	"testing"

	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestReplayRing_CircularOverwrite(t *testing.T) {
	const lastFrame = "id-5"
	ring := newTopicReplayRing(3, ReplayMissAll)

	// Edge cases on empty ring
	if ring.ReplayAfter("") != nil {
		t.Fatalf("expected nil on empty lastID")
	}
	if ring.ReplayAfter("id-0") != nil {
		t.Fatalf("expected nil on empty ring")
	}

	// Push nil or empty ID
	ring.Push(nil)
	ring.Push(frame.Text("no-id"))

	// Push 5 frames: 1, 2, 3, 4, 5 into ring of capacity 3
	for i := 1; i <= 5; i++ {
		ring.Push(frame.Format("ev", fmt.Sprintf("id-%d", i), 0, []byte("data")))
	}

	// Ring should hold 3, 4, 5
	// ReplayAfter id-3 should return 4, 5
	after3 := ring.ReplayAfter("id-3")
	if len(after3) != 2 {
		t.Fatalf("expected 2 frames after id-3, got %d", len(after3))
	}
	if after3[0].ID() != "id-4" || after3[1].ID() != lastFrame {
		t.Fatalf("unexpected frames: %s, %s", after3[0].ID(), after3[1].ID())
	}

	// ReplayAfter id-5 should return nil (it's the latest)
	after5 := ring.ReplayAfter(lastFrame)
	if len(after5) != 0 {
		t.Fatalf("expected 0 frames after latest id-5, got %d", len(after5))
	}

	// ReplayAfter an evicted ID (e.g. id-1):
	// With ReplayMissAll (default), it should return all remaining frames: 3, 4, 5
	afterMissAll := ring.ReplayAfter("id-1")
	if len(afterMissAll) != 3 {
		t.Fatalf("expected 3 frames for evicted ID with ReplayMissAll, got %d", len(afterMissAll))
	}
	if afterMissAll[0].ID() != "id-3" || afterMissAll[1].ID() != "id-4" || afterMissAll[2].ID() != lastFrame {
		t.Fatalf("unexpected frames for ReplayMissAll: %v", afterMissAll)
	}
}

func TestReplayRing_PolicyMissNone(t *testing.T) {
	ring := newTopicReplayRing(3, ReplayMissNone)

	for i := 1; i <= 5; i++ {
		ring.Push(frame.Format("ev", fmt.Sprintf("id-%d", i), 0, []byte("data")))
	}

	// id-1 was evicted. With ReplayMissNone, it should return nil
	afterEvicted := ring.ReplayAfter("id-1")
	if len(afterEvicted) != 0 {
		t.Fatalf("expected nil/empty for evicted ID with ReplayMissNone, got %d", len(afterEvicted))
	}

	// Non-existent ID: returns nil
	afterUnknown := ring.ReplayAfter("id-999")
	if len(afterUnknown) != 0 {
		t.Fatalf("expected nil for unknown ID with ReplayMissNone, got %d", len(afterUnknown))
	}

	// Existing ID: still works
	after4 := ring.ReplayAfter("id-4")
	if len(after4) != 1 || after4[0].ID() != "id-5" {
		t.Fatalf("expected id-5 after id-4, got %v", after4)
	}
}

func TestReplayRing_DefaultCapacity(t *testing.T) {
	ring := newTopicReplayRing(0, ReplayMissAll)
	if ring.cap != 128 {
		t.Fatalf("expected default capacity 128, got %d", ring.cap)
	}
}

func TestReplayManager_MaxTopicsEviction(t *testing.T) {
	mgr := newReplayManager(10, ReplayMissAll, 2) // max 2 topics

	// Edge cases
	mgr.Push("", frame.Format("ev", "id-1", 0, []byte("d1")))
	mgr.Push("topic1", nil)
	mgr.Push("topic1", frame.Text("no-id"))
	if mgr.ReplayAfter("", "id-1") != nil {
		t.Fatalf("expected nil on empty topic")
	}
	if mgr.ReplayAfter("topic1", "") != nil {
		t.Fatalf("expected nil on empty lastID")
	}

	mgr.Push("topic1", frame.Format("ev", "id-1", 0, []byte("d1")))
	mgr.Push("topic2", frame.Format("ev", "id-2", 0, []byte("d2")))
	mgr.Push("topic3", frame.Format("ev", "id-3", 0, []byte("d3"))) // should evict topic1

	// topic1 was evicted
	r1 := mgr.ReplayAfter("topic1", "any")
	if len(r1) != 0 {
		t.Fatalf("expected topic1 to be evicted, got %v", r1)
	}

	// topic2 and topic3 should still exist
	r2 := mgr.ReplayAfter("topic2", "unknown")
	if len(r2) != 1 || r2[0].ID() != "id-2" {
		t.Fatalf("expected topic2 to retain id-2, got %v", r2)
	}

	r3 := mgr.ReplayAfter("topic3", "unknown")
	if len(r3) != 1 || r3[0].ID() != "id-3" {
		t.Fatalf("expected topic3 to retain id-3, got %v", r3)
	}
}

func TestReplayManager_NilOrDisabled(t *testing.T) {
	var mgr *replayManager
	mgr.Push("topic", frame.Format("ev", "id", 0, []byte("d")))
	if mgr.ReplayAfter("topic", "id") != nil {
		t.Fatalf("expected nil for nil manager")
	}

	disabled := newReplayManager(0, ReplayMissAll, 100)
	if disabled != nil {
		t.Fatalf("expected nil manager when topicCap <= 0")
	}
}
