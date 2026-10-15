package broadcaster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestBroadcaster_AsyncWorkers(t *testing.T) {
	// Initialize broadcaster with 4 background fanout workers and FIFO delivery
	b := broadcaster.New(
		broadcaster.WithAsyncWorkers(4, 5000),
		broadcaster.WithDeliveryMode(broadcaster.DeliveryFIFO),
		broadcaster.WithFIFOBufferSize(200),
	)
	defer func() { _ = b.Close() }()

	sub, unsubscribe := b.Subscribe("async.stream")
	defer unsubscribe()

	const totalMessages = 50
	for i := 1; i <= totalMessages; i++ {
		b.Publish("async.stream", frame.Text(fmt.Sprintf("async-%d", i)))
	}

	// Wait for async workers to fan out all messages
	var received int
	deadline := time.After(2 * time.Second)

	for received < totalMessages {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for async messages: received %d/%d", received, totalMessages)
		case <-sub.Recv():
			batch := sub.Fetch()
			received += len(batch)
		}
	}

	if received != totalMessages {
		t.Fatalf("expected %d messages, got %d", totalMessages, received)
	}
}

func TestBroadcaster_AsyncWorkers_StrictFIFOOrdering(t *testing.T) {
	b := broadcaster.New(
		broadcaster.WithDeliveryMode(broadcaster.DeliveryFIFO),
		broadcaster.WithFIFOBufferSize(100000),
		broadcaster.WithAsyncWorkers(8, 100000),
	)
	defer func() { _ = b.Close() }()

	sub, unsub := b.Subscribe("ordered_topic")
	defer unsub()

	const n = 10000
	for i := 0; i < n; i++ {
		b.Publish("ordered_topic", frame.Text(fmt.Sprint(i)))
	}

	var got []int
	deadline := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case <-sub.Recv():
			for _, f := range sub.Fetch() {
				var v int
				_, _ = fmt.Sscanf(f.String(), "data: %d\n\n", &v)
				got = append(got, v)
			}
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d", len(got), n)
		}
	}

	inversions := 0
	prev := -1
	for _, v := range got {
		if v < prev {
			inversions++
		}
		prev = v
	}
	if inversions > 0 {
		t.Fatalf("FIFO + async workers delivered %d out-of-order frames", inversions)
	}
}
