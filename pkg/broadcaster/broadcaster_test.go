package broadcaster_test

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestBroadcaster_PubSub(t *testing.T) {
	b := broadcaster.New(broadcaster.WithShards(64))
	defer func() { _ = b.Close() }()

	subBTC, unsubBTC := b.Subscribe("crypto.BTC")
	defer unsubBTC()

	subETH, unsubETH := b.Subscribe("crypto.ETH")
	defer unsubETH()

	if b.ActiveSubscribers("crypto.BTC") != 1 {
		t.Fatalf("expected 1 BTC subscriber, got %d", b.ActiveSubscribers("crypto.BTC"))
	}
	if b.TotalSubscribers() != 2 {
		t.Fatalf("expected 2 total subscribers, got %d", b.TotalSubscribers())
	}

	// Publish to BTC
	b.Publish("crypto.BTC", frame.Text("btc-68000"))

	// BTC sub should have received frame
	batchBTC := subBTC.Fetch()
	if len(batchBTC) != 1 || batchBTC[0].String() != "data: btc-68000\n\n" {
		t.Fatalf("unexpected BTC frame: %v", batchBTC)
	}

	// ETH sub should be empty
	batchETH := subETH.Fetch()
	if len(batchETH) != 0 {
		t.Fatalf("ETH subscriber received unexpected frame: %v", batchETH)
	}
}

func TestBroadcaster_MultiTopicSubscription(t *testing.T) {
	b := broadcaster.New()
	defer func() { _ = b.Close() }()

	topics := []string{"AAPL", "TSLA", "NVDA"}
	sub, unsubscribe := b.SubscribeMulti(topics)

	for _, sym := range topics {
		if b.ActiveSubscribers(sym) != 1 {
			t.Fatalf("expected 1 subscriber for %s, got %d", sym, b.ActiveSubscribers(sym))
		}
	}

	// Publish to TSLA
	b.Publish("TSLA", frame.Text("tsla-250"))
	batch := sub.Fetch()
	if len(batch) != 1 || batch[0].String() != "data: tsla-250\n\n" {
		t.Fatalf("unexpected TSLA frame: %v", batch)
	}

	// Unsubscribe from all
	unsubscribe()

	for _, sym := range topics {
		if b.ActiveSubscribers(sym) != 0 {
			t.Fatalf("expected 0 subscribers for %s after unsubscribe, got %d", sym, b.ActiveSubscribers(sym))
		}
	}
}

func TestBroadcaster_PublishAll(t *testing.T) {
	b := broadcaster.New()
	defer func() { _ = b.Close() }()

	sub1, unsub1 := b.Subscribe("topic1")
	defer unsub1()
	sub2, unsub2 := b.Subscribe("topic2")
	defer unsub2()

	b.PublishAll(frame.Text("global-alert"))

	batch1 := sub1.Fetch()
	batch2 := sub2.Fetch()

	if len(batch1) != 1 || batch1[0].String() != "data: global-alert\n\n" {
		t.Fatalf("sub1 did not receive global alert")
	}
	if len(batch2) != 1 || batch2[0].String() != "data: global-alert\n\n" {
		t.Fatalf("sub2 did not receive global alert")
	}
}

func TestBroadcaster_ConcurrentPubSubRace(t *testing.T) {
	b := broadcaster.New(broadcaster.WithShards(32))
	defer func() { _ = b.Close() }()

	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Publisher goroutines
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				//nolint:gosec // pseudo-random generator is sufficient for race test
				topic := fmt.Sprintf("topic-%d", rand.IntN(20))
				b.Publish(topic, frame.Text(fmt.Sprintf("val-%d", i)))
			}
		}()
	}

	// Subscriber goroutines (subscribe, fetch, unsubscribe)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations/2; i++ {
				//nolint:gosec // pseudo-random generator is sufficient for race test
				topic := fmt.Sprintf("topic-%d", rand.IntN(20))
				sub, unsub := b.Subscribe(topic)
				_ = sub.Fetch()
				unsub()
			}
		}()
	}

	wg.Wait()
}

func TestBroadcaster_ServeHTTP(t *testing.T) {
	b := broadcaster.New(broadcaster.WithWriteTimeout(2 * time.Second))
	defer func() { _ = b.Close() }()

	server := httptest.NewServer(b)
	defer server.Close()

	// Connect SSE client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"?topic=market-feed", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}

	// Publish an event to the topic
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.Publish("market-feed", frame.Text("trade-123"))
	}()

	reader := bufio.NewReader(resp.Body)
	var line string
	for {
		l, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed reading line: %v", err)
		}
		if strings.HasPrefix(l, "data: trade-123") {
			line = l
			break
		}
	}

	if !strings.Contains(line, "trade-123") {
		t.Fatalf("expected trade-123, got %q", line)
	}
}

func TestConflatingSubscriber_UnannouncedSingleToMultiPromotion(t *testing.T) {
	sub := broadcaster.NewConflatingSubscriber(nil)
	defer sub.Close()

	// First push initializes single-topic mode dynamically for "topicA"
	sub.Push("topicA", frame.Text("a1"))
	// Second push on "topicB" promotes subscriber to multi-topic mode
	sub.Push("topicB", frame.Text("b1"))
	// Third push on "topicA" must overwrite "a1" in-place rather than leaving a stale "_prev" slot
	sub.Push("topicA", frame.Text("a2"))

	batch := sub.Fetch()
	if len(batch) != 2 {
		t.Fatalf("expected exactly 2 frames after conflation, got %d: %v", len(batch), batch)
	}
	if batch[0].String() != "data: a2\n\n" || batch[1].String() != "data: b1\n\n" {
		t.Fatalf("unexpected batch order or contents: %v, %v", batch[0].String(), batch[1].String())
	}
}

func TestBroadcaster_CloseConcurrentWithServeHTTPAndSubscribe(t *testing.T) {
	for iter := 0; iter < 10; iter++ {
		b := broadcaster.New(broadcaster.WithShards(16))

		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sub, unsub := b.SubscribeMulti([]string{"t1", "t2"})
				defer unsub()
				_ = sub.Fetch()
			}()
		}

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rec := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/events?topic=t1", nil)
				b.ServeHTTP(rec, req)
			}()
		}

		time.Sleep(time.Millisecond)
		if err := b.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		wg.Wait()
	}
}

func TestStream_ZeroWriteTimeout(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/stream", nil)

	stream, err := broadcaster.Upgrade(rec, req, broadcaster.WithWriteTimeout(0), broadcaster.WithHeartbeat(0, 0))
	if err != nil {
		t.Fatalf("Upgrade with WriteTimeout(0) failed: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if err := stream.SendText("hello"); err != nil {
		t.Fatalf("SendText failed with WriteTimeout(0): %v", err)
	}
	if err := stream.SendBatch([]*frame.Frame{frame.Text("batch1")}); err != nil {
		t.Fatalf("SendBatch failed with WriteTimeout(0): %v", err)
	}
	if !strings.Contains(rec.Body.String(), "data: hello\n\ndata: batch1\n\n") {
		t.Fatalf("unexpected body: %q", rec.Body.String())
	}
}

func TestBroadcaster_MethodNotAllowed(t *testing.T) {
	b := broadcaster.New()
	defer func() { _ = b.Close() }()

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/events?topic=test", nil)
	b.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Fatalf("expected Allow: GET, HEAD, got %q", allow)
	}
}

func TestReplay_TopicEvictionRing(t *testing.T) {
	b := broadcaster.New(
		broadcaster.WithReplayBuffer(4),
		broadcaster.WithReplayMaxTopics(8),
		broadcaster.WithReplayMissPolicy(broadcaster.ReplayMissNone),
	)
	defer func() { _ = b.Close() }()

	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("topic-%d", i)
		b.Publish(topic, frame.Format("evt", "1", 0, []byte("first")))
		b.Publish(topic, frame.Format("evt", "2", 0, []byte("second")))
	}

	// Oldest topics (0..91) should have been evicted; newest (92..99) should be retained
	server := httptest.NewServer(b)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"?topic=topic-99&last_event_id=1", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed reading replayed line: %v", err)
	}
	if !strings.Contains(line, "id: 2") {
		t.Fatalf("expected replayed id: 2, got %q", line)
	}
}

func TestConflatingSubscriber_PublishAllDoesNotOverwriteTopic(t *testing.T) {
	b := broadcaster.New(broadcaster.WithDeliveryMode(broadcaster.DeliveryConflated))
	defer func() { _ = b.Close() }()

	sub, unsub := b.Subscribe("crypto.BTC")
	defer unsub()

	b.Publish("crypto.BTC", frame.Text("btc-68000"))
	b.PublishAll(frame.Text("system-maintenance"))
	b.Publish("crypto.BTC", frame.Text("btc-68100"))

	batch := sub.Fetch()
	if len(batch) != 2 {
		t.Fatalf("expected 2 frames (latest BTC tick + PublishAll alert), got %d: %v", len(batch), batch)
	}
	if batch[0].String() != "data: btc-68100\n\n" || batch[1].String() != "data: system-maintenance\n\n" {
		t.Fatalf("unexpected batch frames: %v, %v", batch[0].String(), batch[1].String())
	}
}

func TestSubscribeMulti_DeduplicatesAndStripsEmptyTopics(t *testing.T) {
	metrics := broadcaster.NewCounterMetrics()
	b := broadcaster.New(broadcaster.WithMetrics(metrics))
	defer func() { _ = b.Close() }()

	sub, unsub := b.SubscribeMulti([]string{"  btc ", "btc", "", "   "})
	if metrics.ConnectedClients.Load() != 1 {
		t.Fatalf("expected ConnectedClients == 1 for deduplicated topic, got %d", metrics.ConnectedClients.Load())
	}

	b.Publish("btc", frame.Text("tick-1"))
	batch := sub.Fetch()
	if len(batch) != 1 || batch[0].String() != "data: tick-1\n\n" {
		t.Fatalf("unexpected batch: %v", batch)
	}

	unsub()
	if metrics.ConnectedClients.Load() != 0 {
		t.Fatalf("expected ConnectedClients == 0 after unsub, got %d", metrics.ConnectedClients.Load())
	}
}

func TestWithWriteTimeoutZero_OptionApplied(t *testing.T) {
	opts := broadcaster.DefaultOptions()
	broadcaster.WithWriteTimeout(0)(&opts)
	if opts.WriteTimeout != 0 {
		t.Fatalf("expected WriteTimeout == 0, got %v", opts.WriteTimeout)
	}
}
