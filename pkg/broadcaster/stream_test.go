package broadcaster_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestStream_UpgradeAndSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, err := broadcaster.Upgrade(w, r, broadcaster.WithWriteTimeout(2*time.Second))
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer func() { _ = s.Close() }()

		_ = s.SendText("token-1")
		_ = s.SendEvent("chunk", "token-2")
		_ = s.SendComment("test-comment")
	}))
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}

	reader := bufio.NewReader(resp.Body)
	var lines []string
	for i := 0; i < 9; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		lines = append(lines, strings.TrimRight(line, "\r\n"))
	}

	body := strings.Join(lines, "\n")
	if !strings.Contains(body, "data: token-1") {
		t.Fatalf("missing token-1 in %q", body)
	}
	if !strings.Contains(body, "event: chunk\ndata: token-2") {
		t.Fatalf("missing chunk in %q", body)
	}
	if !strings.Contains(body, ": test-comment") {
		t.Fatalf("missing comment in %q", body)
	}
}

func TestStream_BatchSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, err := broadcaster.Upgrade(w, r)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer func() { _ = s.Close() }()

		frames := []*frame.Frame{
			frame.Text("msg1"),
			frame.Text("msg2"),
			frame.Text("msg3"),
		}
		_ = s.SendBatch(frames)
	}))
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	scanner := bufio.NewScanner(resp.Body)
	var received []string
	for scanner.Scan() {
		text := scanner.Text()
		if strings.HasPrefix(text, "data: ") {
			received = append(received, strings.TrimPrefix(text, "data: "))
		}
	}

	if len(received) != 3 || received[0] != "msg1" || received[1] != "msg2" || received[2] != "msg3" {
		t.Fatalf("unexpected batch response: %v", received)
	}
}
