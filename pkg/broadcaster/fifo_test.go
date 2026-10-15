package broadcaster_test

import (
	"fmt"
	"testing"

	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestFIFOSubscriber_PreservesOrder(t *testing.T) {
	metrics := broadcaster.NewCounterMetrics()
	sub := broadcaster.NewFIFOSubscriber(100, broadcaster.OverflowDropOldest, metrics)
	defer sub.Close()

	for i := 1; i <= 50; i++ {
		sub.Push(frame.Text(fmt.Sprintf("order-%d", i)))
	}

	batch := sub.Fetch()
	if len(batch) != 50 {
		t.Fatalf("expected 50 frames, got %d", len(batch))
	}

	for i, f := range batch {
		expected := fmt.Sprintf("data: order-%d\n\n", i+1)
		if f.String() != expected {
			t.Fatalf("frame %d: expected %q, got %q", i, expected, f.String())
		}
	}
}

func TestFIFOSubscriber_OverflowDropOldest(t *testing.T) {
	metrics := broadcaster.NewCounterMetrics()
	sub := broadcaster.NewFIFOSubscriber(5, broadcaster.OverflowDropOldest, metrics)
	defer sub.Close()

	for i := 1; i <= 10; i++ {
		sub.Push(frame.Text(fmt.Sprintf("item-%d", i)))
	}

	batch := sub.Fetch()
	if len(batch) != 5 {
		t.Fatalf("expected 5 frames in capacity-5 buffer, got %d", len(batch))
	}

	// Should contain the last 5 frames: item-6 through item-10
	for i, f := range batch {
		expected := fmt.Sprintf("data: item-%d\n\n", i+6)
		if f.String() != expected {
			t.Fatalf("frame %d: expected %q, got %q", i, expected, f.String())
		}
	}

	if overflows := metrics.TotalOverflows.Load(); overflows != 5 {
		t.Fatalf("expected 5 overflows recorded, got %d", overflows)
	}
}

func TestFIFOSubscriber_OverflowDropNewest(t *testing.T) {
	metrics := broadcaster.NewCounterMetrics()
	sub := broadcaster.NewFIFOSubscriber(5, broadcaster.OverflowDropNewest, metrics)
	defer sub.Close()

	for i := 1; i <= 10; i++ {
		sub.Push(frame.Text(fmt.Sprintf("item-%d", i)))
	}

	batch := sub.Fetch()
	if len(batch) != 5 {
		t.Fatalf("expected 5 frames, got %d", len(batch))
	}

	// Should contain the first 5 frames: item-1 through item-5
	for i, f := range batch {
		expected := fmt.Sprintf("data: item-%d\n\n", i+1)
		if f.String() != expected {
			t.Fatalf("frame %d: expected %q, got %q", i, expected, f.String())
		}
	}
}

func TestFIFOSubscriber_OverflowDisconnect(t *testing.T) {
	metrics := broadcaster.NewCounterMetrics()
	sub := broadcaster.NewFIFOSubscriber(5, broadcaster.OverflowDisconnect, metrics)
	defer sub.Close()

	for i := 1; i <= 5; i++ {
		if !sub.Push(frame.Text(fmt.Sprintf("item-%d", i))) {
			t.Fatalf("push %d failed unexpectedly", i)
		}
	}

	if sub.IsClosed() {
		t.Fatal("subscriber should still be open")
	}

	// 6th push should trigger disconnect
	delivered := sub.Push(frame.Text("item-6"))
	if delivered {
		t.Fatal("expected 6th push to fail on overflow")
	}

	if !sub.IsClosed() {
		t.Fatal("expected subscriber to be closed after overflow disconnect")
	}

	select {
	case <-sub.Done():
		// Successfully signaled
	default:
		t.Fatal("expected subscriber Done() channel to be closed on overflow disconnect")
	}
}

func TestFIFOSubscriber_OverflowDisconnect_WakesDrainingReader(t *testing.T) {
	s := broadcaster.NewFIFOSubscriber(2, broadcaster.OverflowDisconnect, nil)
	f := frame.Text("x")
	for i := 0; i < 5; i++ {
		s.Push(f)
	}
	if !s.IsClosed() {
		t.Fatal("expected closed")
	}

	<-s.Recv()
	if b := s.Fetch(); len(b) == 0 {
		t.Fatal("expected buffered frames")
	}

	select {
	case <-s.Done():
		// reader woken on disconnect via Done
	case <-s.Recv():
		// reader woken on disconnect via notify
	default:
		t.Fatal("subscriber closed by OverflowDisconnect but neither Recv() nor Done() signaled")
	}
}
