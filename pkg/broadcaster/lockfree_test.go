package broadcaster_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rlsvr/blazesse"
	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestLockFree_PubSub_FIFO(t *testing.T) {
	b := broadcaster.New(
		broadcaster.NewEngineLockFree(
			broadcaster.WithReconfigInterval(5*time.Millisecond),
			broadcaster.WithReconfigBatchSize(10),
			broadcaster.WithReconfigWait(true),
		),
		broadcaster.WithDeliveryMode(broadcaster.DeliveryFIFO),
	)
	defer b.Close()

	sub, unsub := b.SubscribeMulti([]string{"topicA", "topicB"})
	defer unsub()

	if b.ActiveSubscribers("topicA") != 1 {
		t.Fatalf("expected 1 active subscriber on topicA, got %d", b.ActiveSubscribers("topicA"))
	}
	if b.ActiveSubscribers("topicB") != 1 {
		t.Fatalf("expected 1 active subscriber on topicB, got %d", b.ActiveSubscribers("topicB"))
	}

	f1 := frame.Text("hello A")
	f2 := frame.Text("hello B")
	f3 := frame.Text("hello C")

	b.Publish("topicA", f1)
	b.Publish("topicB", f2)
	b.Publish("topicC", f3) // sub should not receive this

	batch := sub.Fetch()
	if len(batch) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(batch))
	}
	if batch[0].String() != f1.String() {
		t.Errorf("got %s, want %s", batch[0].String(), f1.String())
	}
	if batch[1].String() != f2.String() {
		t.Errorf("got %s, want %s", batch[1].String(), f2.String())
	}

	// Unsubscribe
	unsub()
	time.Sleep(20 * time.Millisecond)

	if b.ActiveSubscribers("topicA") != 0 {
		t.Fatalf("expected 0 active subscribers after cancel, got %d", b.ActiveSubscribers("topicA"))
	}
}

func TestLockFree_PubSub_Conflated(t *testing.T) {
	b := broadcaster.New(
		broadcaster.NewEngineLockFree(
			broadcaster.WithReconfigInterval(5*time.Millisecond),
			broadcaster.WithReconfigWait(true),
		),
		broadcaster.WithDeliveryMode(broadcaster.DeliveryConflated),
	)
	defer b.Close()

	sub, unsub := b.Subscribe("price_feed")
	defer unsub()

	f1 := frame.Text("p1")
	f2 := frame.Text("p2")
	f3 := frame.Text("p3")

	b.Publish("price_feed", f1)
	b.Publish("price_feed", f2)
	b.Publish("price_feed", f3)

	batch := sub.Fetch()
	if len(batch) != 1 {
		t.Fatalf("expected 1 conflated frame, got %d", len(batch))
	}
	if batch[0].String() != f3.String() {
		t.Errorf("got %s, want %s", batch[0].String(), f3.String())
	}
}

func TestLockFree_PublishAll(t *testing.T) {
	b := broadcaster.New(
		broadcaster.NewEngineLockFree(
			broadcaster.WithReconfigInterval(5*time.Millisecond),
			broadcaster.WithReconfigWait(true),
		),
		broadcaster.WithDeliveryMode(broadcaster.DeliveryFIFO),
	)
	defer b.Close()

	sub1, unsub1 := b.Subscribe("topic1")
	defer unsub1()
	sub2, unsub2 := b.Subscribe("topic2")
	defer unsub2()

	b.PublishAll(frame.Text("broadcast"))

	batch1 := sub1.Fetch()
	if len(batch1) != 1 || batch1[0].String() != frame.Text("broadcast").String() {
		t.Fatalf("sub1 unexpected batch: %v", batch1)
	}

	batch2 := sub2.Fetch()
	if len(batch2) != 1 || batch2[0].String() != frame.Text("broadcast").String() {
		t.Fatalf("sub2 unexpected batch: %v", batch2)
	}
}

func TestLockFree_ConcurrentPublishAndViewSwitching(t *testing.T) {
	b := broadcaster.New(
		broadcaster.NewEngineLockFree(
			broadcaster.WithReconfigInterval(5*time.Millisecond),
			broadcaster.WithReconfigBatchSize(32),
			broadcaster.WithReconfigWait(false),
		),
		broadcaster.WithDeliveryMode(broadcaster.DeliveryFIFO),
	)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var pubCount atomic.Int64
	var subReceived atomic.Int64

	// 4 publisher goroutines firing at high speed
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			f := frame.Text("pulse")
			for ctx.Err() == nil {
				topic := fmt.Sprintf("topic_%d", workerID%5)
				b.Publish(topic, f)
				pubCount.Add(1)
			}
		}(i)
	}

	// 20 subscriber goroutines constantly switching views (subscribing, fetching, unsubscribing)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(subID int) {
			defer wg.Done()
			r := 0
			for ctx.Err() == nil {
				topic := fmt.Sprintf("topic_%d", (subID+r)%5)
				sub, unsub := b.Subscribe(topic)

				// Poll for a brief duration
				start := time.Now()
				for time.Since(start) < 5*time.Millisecond && ctx.Err() == nil {
					frames := sub.Fetch()
					subReceived.Add(int64(len(frames)))
					if len(frames) == 0 {
						time.Sleep(200 * time.Microsecond)
					}
				}
				unsub()
				r++
			}
		}(i)
	}

	wg.Wait()

	if pubCount.Load() == 0 {
		t.Fatal("expected pubCount > 0")
	}
	t.Logf("Published: %d, Received: %d during high churn view-switching", pubCount.Load(), subReceived.Load())
}

func TestFacade_EngineLockFree(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.NewEngineLockFree(
			blazesse.WithReconfigInterval(10*time.Millisecond),
			blazesse.WithReconfigWait(true),
		),
	)
	defer b.Close()

	sub, unsub := b.Subscribe("facade_topic")
	defer unsub()

	b.Publish("facade_topic", blazesse.TextFrame("hello facade"))

	batch := sub.Fetch()
	if len(batch) != 1 || batch[0].String() != blazesse.TextFrame("hello facade").String() {
		t.Fatalf("unexpected batch: %v", batch)
	}
}

func TestFacade_EngineShardedMutex(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.NewEngineShardedMutex(
			blazesse.WithEngineShards(32),
		),
	)
	defer b.Close()

	sub, unsub := b.Subscribe("sharded_topic")
	defer unsub()

	b.Publish("sharded_topic", blazesse.TextFrame("hello sharded"))

	batch := sub.Fetch()
	if len(batch) != 1 || batch[0].String() != blazesse.TextFrame("hello sharded").String() {
		t.Fatalf("unexpected batch: %v", batch)
	}
}

