package test_test

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rlsvr/blazesse"
	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

// 1. WithHeader and custom headers are preserved and sent by Broadcaster.ServeHTTP.
func TestIntegration_ServeHTTP_CustomHeadersPreserved(t *testing.T) {
	b := blazesse.NewBroadcaster(blazesse.WithHeader("Access-Control-Allow-Origin", "https://app.example"))
	defer func() { _ = b.Close() }()
	srv := httptest.NewServer(b)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?topic=x", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got := resp.Header.Get("Access-Control-Allow-Origin")
	if got != "https://app.example" {
		t.Fatalf("expected CORS header 'https://app.example', got %q", got)
	}
}

// 2. Broadcaster.Close() promptly terminates active in-flight ServeHTTP handlers.
func TestIntegration_BroadcasterClose_TerminatesHandlers(t *testing.T) {
	for _, eng := range []struct {
		name string
		opt  blazesse.Option
	}{
		{"sharded", blazesse.NewEngineShardedMutex()},
		{"lockfree", blazesse.NewEngineLockFree(blazesse.WithReconfigWait(true))},
	} {
		t.Run(eng.name, func(t *testing.T) {
			b := blazesse.NewBroadcaster(eng.opt)
			done := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b.ServeHTTP(w, r)
				close(done)
			}))
			defer func() { go srv.Close() }()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?topic=x", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()

			time.Sleep(50 * time.Millisecond)
			_ = b.Close()

			select {
			case <-done:
				t.Log("handler exited cleanly after Close")
			case <-time.After(1 * time.Second):
				t.Fatalf("handler still running 1s after Broadcaster.Close()")
			}
		})
	}
}

// 3. Topic deduplication in default extractor.
func TestIntegration_TopicDeduplication(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/?topics=a,b,a,c&topic=b&topic=d", nil)
	topics, err := broadcaster.DefaultTopicExtractor(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 4 {
		t.Fatalf("expected 4 deduplicated topics, got %d: %v", len(topics), topics)
	}
	for _, exp := range []string{"a", "b", "c", "d"} {
		found := false
		for _, top := range topics {
			if top == exp {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing topic %s in %v", exp, topics)
		}
	}
}

// 4. MaxTopicsPerConn rejects requests exceeding topic limit with 400 Bad Request.
func TestIntegration_MaxTopicsPerConn(t *testing.T) {
	b := blazesse.NewBroadcaster(blazesse.WithMaxTopicsPerConn(3))
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topics=t1,t2,t3,t4", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for exceeding MaxTopicsPerConn, got %d", resp.StatusCode)
	}
}

// 5. MaxTopicLen rejects excessively long topic names with 400 Bad Request.
func TestIntegration_MaxTopicLen(t *testing.T) {
	b := blazesse.NewBroadcaster(blazesse.WithMaxTopicLen(10))
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topic=this-is-longer-than-ten-chars", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for exceeding MaxTopicLen, got %d", resp.StatusCode)
	}
}

// 6. AuthorizeTopics hook validates requested topics and returns 403 Forbidden.
func TestIntegration_AuthorizeTopics(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithAuthorizeTopics(func(r *http.Request, topics []string) error {
			for _, top := range topics {
				if top == "secret" {
					return fmt.Errorf("unauthorized secret topic")
				}
			}
			return nil
		}),
	)
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	// Authorized request
	req1, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topic=public", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for authorized topic, got %d", resp1.StatusCode)
	}

	// Unauthorized request
	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topic=secret", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for unauthorized topic, got %d", resp2.StatusCode)
	}
}

// 7. MaxConnections limit returns 429 Too Many Requests when saturated.
func TestIntegration_MaxConnections(t *testing.T) {
	b := blazesse.NewBroadcaster(blazesse.WithMaxConnections(1))
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, srv.URL+"?topic=t1", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp1.Body.Close() }()

	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for first connection, got %d", resp1.StatusCode)
	}

	// Second connection should be rejected with 429
	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topic=t1", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests when connection limit reached, got %d", resp2.StatusCode)
	}
	if retryAfter := resp2.Header.Get("Retry-After"); retryAfter == "" {
		t.Fatalf("expected Retry-After header on 429 response")
	}
}

// 8. MaxConnections atomic reservation under concurrent burst prevents TOCTOU race.
func TestIntegration_MaxConnections_ConcurrentBurst(t *testing.T) {
	const limit = 5
	const total = 40

	b := blazesse.NewBroadcaster(blazesse.WithMaxConnections(limit))
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	var wg sync.WaitGroup
	var accepted atomic.Int64
	var rejected atomic.Int64
	start := make(chan struct{})
	hold := make(chan struct{})

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?topic=t", nil)
			if err != nil {
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer func() { _ = resp.Body.Close() }()
			switch resp.StatusCode {
			case http.StatusOK:
				accepted.Add(1)
				<-hold
			case http.StatusTooManyRequests:
				rejected.Add(1)
			}
		}()
	}

	close(start)

	// Wait until all 40 requests have connected (5 accepted) or been rejected (35 rejected)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if accepted.Load() == limit && rejected.Load() == total-limit {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := accepted.Load(); got != limit {
		t.Fatalf("expected exactly %d accepted connections under burst, got %d", limit, got)
	}
	if got := rejected.Load(); got != total-limit {
		t.Fatalf("expected exactly %d rejected connections, got %d", total-limit, got)
	}

	// Release held connections
	close(hold)
	wg.Wait()
}

// 9. Replayed events are deduplicated against subscriber's live queue.
func TestIntegration_ServeHTTP_ReplayAndLiveDeduplication(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithReplayBuffer(10),
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
	)
	defer func() { _ = b.Close() }()

	srv := httptest.NewServer(b)
	defer srv.Close()

	// Seed replay buffer with id-1 and id-2
	b.Publish("t", frame.Format("ev", "id-1", 0, []byte("d1")))
	b.Publish("t", frame.Format("ev", "id-2", 0, []byte("d2")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?topic=t", nil)
	req.Header.Set("Last-Event-ID", "id-1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Reader receives events
	scanner := bufio.NewScanner(resp.Body)
	var receivedIDs []string
	msgCh := make(chan string, 10)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "id: ") {
				msgCh <- strings.TrimPrefix(line, "id: ")
			}
		}
	}()

	// Publish id-3 and id-4 live
	time.Sleep(20 * time.Millisecond)
	b.Publish("t", frame.Format("ev", "id-3", 0, []byte("d3")))
	b.Publish("t", frame.Format("ev", "id-4", 0, []byte("d4")))

	timeout := time.After(2 * time.Second)
	for len(receivedIDs) < 3 {
		select {
		case id := <-msgCh:
			receivedIDs = append(receivedIDs, id)
		case <-timeout:
			t.Fatalf("timeout waiting for events, received so far: %v", receivedIDs)
		}
	}

	expected := []string{"id-2", "id-3", "id-4"}
	for i, exp := range expected {
		if receivedIDs[i] != exp {
			t.Fatalf("expected ID %q at index %d, got %q", exp, i, receivedIDs[i])
		}
	}
}
