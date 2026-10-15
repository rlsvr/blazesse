package blazesse_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rlsvr/blazesse"
	"github.com/rlsvr/blazesse/pkg/broadcaster"
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
