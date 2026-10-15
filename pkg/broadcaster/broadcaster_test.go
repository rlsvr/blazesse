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
