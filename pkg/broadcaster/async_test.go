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
