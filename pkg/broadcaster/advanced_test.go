package broadcaster_test

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rlsvr/blazesse"
	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

type nonFlushingWriter struct {
	http.ResponseWriter
}

// 1. Upgrade fails with ErrNotFlusher before WriteHeader when writer doesn't support flushing.
func TestAdvanced_FlusherCheckBeforeWriteHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	nonFlusher := &nonFlushingWriter{ResponseWriter: rec}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)

	stream, err := broadcaster.Upgrade(nonFlusher, req)
	if !errors.Is(err, broadcaster.ErrNotFlusher) {
		t.Fatalf("expected ErrNotFlusher, got %v", err)
	}
	if stream != nil {
		t.Fatalf("expected nil stream on error")
	}
	if rec.Code != http.StatusOK && rec.Flushed {
		t.Fatalf("headers were committed despite flush failure")
	}
}

// 2. WithHeartbeat(0, 0) disables keep-alive heartbeat loop completely.
func TestAdvanced_HeartbeatDisable(t *testing.T) {
	b := blazesse.NewBroadcaster(blazesse.WithHeartbeat(0, 0))
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?topic=test", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Reader should not receive any pings within timeout
	scanner := bufio.NewScanner(resp.Body)
	received := make(chan string, 1)
	go func() {
		for scanner.Scan() {
			received <- scanner.Text()
		}
	}()

	select {
	case line := <-received:
		if strings.HasPrefix(line, ":") {
			t.Fatalf("unexpected heartbeat ping received when heartbeat is disabled: %s", line)
		}
	case <-time.After(100 * time.Millisecond):
		// Expected: no heartbeat sent
	}
}

// 3. PublishAll dedupes subscribers across multiple topics in shardedMutexEngine.
func TestAdvanced_PublishAllDeduplication(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.NewEngineShardedMutex(),
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
	)
	defer func() { _ = b.Close() }()

	// Single subscriber subscribed to 3 topics
	sub, unsub := b.SubscribeMulti([]string{"topic1", "topic2", "topic3"})
	defer unsub()

	// Broadcast to all
	b.PublishAll(frame.Event("global", "all-data"))

	batch := sub.Fetch()
	if len(batch) != 1 {
		t.Fatalf("expected subscriber to receive PublishAll exactly once, got %d deliveries", len(batch))
	}
}

// 4. ConflatingSubscriber Fetch is zero-allocation.
func TestAdvanced_ConflatingSubscriber_ZeroAllocFetch(t *testing.T) {
	sub := broadcaster.NewConflatingSubscriber(nil)
	f := frame.Text("test")
	sub.Push("test", f)

	allocs := testing.AllocsPerRun(100, func() {
		batch := sub.Fetch()
		if len(batch) == 0 {
			t.Fatal("expected batch")
		}
		sub.Push("test", f)
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocs/op for ConflatingSubscriber.Fetch(), got %.1f", allocs)
	}
}

// 5. Last-Event-ID reconnection replay recovers missed messages.
func TestAdvanced_LastEventIDReplay(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
		blazesse.WithReplayBuffer(100),
	)
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	// Publish messages 1, 2, 3, 4, 5 with explicit event IDs
	b.Publish("chat", frame.Format("msg", "msg-1", 0, []byte("data-1")))
	b.Publish("chat", frame.Format("msg", "msg-2", 0, []byte("data-2")))
	b.Publish("chat", frame.Format("msg", "msg-3", 0, []byte("data-3")))
	b.Publish("chat", frame.Format("msg", "msg-4", 0, []byte("data-4")))
	b.Publish("chat", frame.Format("msg", "msg-5", 0, []byte("data-5")))

	// Client reconnects sending Last-Event-ID: msg-2
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topic=chat", nil)
	req.Header.Set("Last-Event-ID", "msg-2")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Reader should receive messages 3, 4, 5
	scanner := bufio.NewScanner(resp.Body)
	ch := make(chan string, 10)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "id: ") {
				ch <- strings.TrimPrefix(line, "id: ")
			}
		}
	}()

	var replayedIDs []string
	timeout := time.After(2 * time.Second)
	for len(replayedIDs) < 3 {
		select {
		case id := <-ch:
			replayedIDs = append(replayedIDs, id)
		case <-timeout:
			t.Fatalf("timed out waiting for replayed IDs, got: %v", replayedIDs)
		}
	}

	if replayedIDs[0] != "msg-3" || replayedIDs[1] != "msg-4" || replayedIDs[2] != "msg-5" {
		t.Fatalf("unexpected replayed IDs: %v (wanted msg-3, msg-4, msg-5)", replayedIDs)
	}
}
