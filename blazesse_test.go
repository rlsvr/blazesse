package blazesse_test

import (
	"testing"
	"time"

	"github.com/rlsvr/blazesse"
)

func TestFacade_API(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithShards(32),
		blazesse.WithWriteTimeout(2*time.Second),
		blazesse.WithDeliveryMode(blazesse.DeliveryConflated),
	)
	defer func() { _ = b.Close() }()

	sub, unsub := b.Subscribe("test.topic")
	defer unsub()

	f := blazesse.TextFrame("facade-test")
	b.Publish("test.topic", f)

	batch := sub.Fetch()
	if len(batch) != 1 || batch[0].String() != "data: facade-test\n\n" {
		t.Fatalf("unexpected frame via root facade: %v", batch)
	}
}

func TestFacade_FIFO(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
		blazesse.WithFIFOBufferSize(10),
		blazesse.WithOverflowPolicy(blazesse.OverflowDropOldest),
	)
	defer func() { _ = b.Close() }()

	sub, unsub := b.Subscribe("fifo.topic")
	defer unsub()

	b.Publish("fifo.topic", blazesse.EventFrame("event1", "data1"))
	b.Publish("fifo.topic", blazesse.EventFrame("event2", "data2"))

	batch := sub.Fetch()
	if len(batch) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(batch))
	}
	if batch[0].String() != "event: event1\ndata: data1\n\n" {
		t.Fatalf("unexpected frame 0: %q", batch[0].String())
	}
	if batch[1].String() != "event: event2\ndata: data2\n\n" {
		t.Fatalf("unexpected frame 1: %q", batch[1].String())
	}
}
