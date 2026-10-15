package broadcaster_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestConflatingSubscriber_OverwritesIntermediateFrames(t *testing.T) {
	metrics := broadcaster.NewCounterMetrics()
	sub := broadcaster.NewConflatingSubscriber(metrics)
	defer sub.Close()

	// Push 1,000 updates without consumer draining
	for i := 1; i <= 1000; i++ {
		sub.Push(frame.Text(fmt.Sprintf("state-%d", i)))
	}

	// Consumer wakes up via Recv signal
	select {
	case <-sub.Recv():
	default:
		t.Fatal("expected wakeup signal on notify channel")
	}

	// Fetch should return ONLY the freshest frame
	batch := sub.Fetch()
	if len(batch) != 1 {
		t.Fatalf("expected 1 conflated frame, got %d", len(batch))
	}

	expected := "data: state-1000\n\n"
	if batch[0].String() != expected {
		t.Fatalf("expected %q, got %q", expected, batch[0].String())
	}

	// Subsequent Fetch must be nil because Swap(nil) cleared the reference
	secondFetch := sub.Fetch()
	if secondFetch != nil {
		t.Fatalf("expected nil after fetch, got %v", secondFetch)
	}

	// Conflated metric should equal 999 (1000 pushes - 1 retained)
	if conflated := metrics.TotalConflated.Load(); conflated != 999 {
		t.Fatalf("expected 999 conflated frames, got %d", conflated)
	}
}

func TestConflatingSubscriber_ConcurrentPush(t *testing.T) {
	sub := broadcaster.NewConflatingSubscriber(nil)
	defer sub.Close()

	const goroutines = 20
	const pushesPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < pushesPerGoroutine; i++ {
				sub.Push(frame.Text(fmt.Sprintf("g%d-item%d", id, i)))
			}
		}(g)
	}

	wg.Wait()

	// Ensure consumer can fetch without race or deadlock
	batch := sub.Fetch()
	if len(batch) != 1 {
		t.Fatalf("expected 1 latest frame, got %d", len(batch))
	}
}
