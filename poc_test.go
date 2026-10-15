package blazesse_test

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rlsvr/blazesse"
	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

// 1. WithHeader / WithPingComment are passed to Broadcaster.ServeHTTP.
func TestPOC_CustomHeadersDroppedByServeHTTP(t *testing.T) {
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

// 2. Broadcaster.Close() terminates in-flight ServeHTTP handlers.
func TestPOC_CloseDoesNotEndHandlers(t *testing.T) {
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

// 3. OverflowDisconnect closes the mailbox and wakes the handler.
func TestPOC_OverflowDisconnectDoesNotDisconnect(t *testing.T) {
	s := broadcaster.NewFIFOSubscriber(2, broadcaster.OverflowDisconnect, nil)
	f := frame.Text("x")
	for i := 0; i < 5; i++ {
		s.Push(f)
	}
	if !s.IsClosed() {
		t.Fatal("expected closed")
	}

	<-s.Recv()
	if b := s.Fetch(); len(b) == 0 {
		t.Fatal("expected buffered frames")
	}

	select {
	case <-s.Recv():
		t.Log("reader woken on disconnect via notify")
	case <-s.Done():
		t.Log("reader woken on disconnect via Done")
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("subscriber closed by OverflowDisconnect but neither Recv() nor Done() signaled")
	}
}

// 4. SSE field injection prevention via CR in data and LF in event name.
func TestPOC_SSEInjection(t *testing.T) {
	userMsg := "hi\revent: admin\rdata: pwned\r\r"
	raw := frame.Event("message", userMsg).String()
	events := parseSSE(raw)
	if len(events) > 1 || (len(events) > 0 && events[0].event != "message") {
		t.Fatalf("CR in data injected extra events/fields: %+v", events)
	}

	raw2 := frame.Event("x\ndata: injected\n\nevent: evil", "ok").String()
	if ev := parseSSE(raw2); len(ev) > 1 {
		t.Fatalf("LF in event name injected extra events: %+v", ev)
	}

	raw3 := frame.Text("line1\r\nline2").String()
	if ev := parseSSE(raw3); len(ev) != 1 {
		t.Fatalf("CRLF text split into %d events: %+v", len(ev), ev)
	}
}

// 5. Async workers preserve topic FIFO order.
func TestPOC_AsyncWorkersReorder(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
		blazesse.WithFIFOBufferSize(100000),
		blazesse.WithAsyncWorkers(8, 100000),
	)
	defer func() { _ = b.Close() }()

	sub, unsub := b.Subscribe("t")
	defer unsub()

	const n = 20000
	for i := 0; i < n; i++ {
		b.Publish("t", frame.Text(fmt.Sprint(i)))
	}

	var got []string
	deadline := time.After(5 * time.Second)
	for len(got) < n {
		select {
		case <-sub.Recv():
			for _, f := range sub.Fetch() {
				got = append(got, f.String())
			}
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d", len(got), n)
		}
	}

	inversions := 0
	prev := -1
	for _, s := range got {
		var v int
		_, _ = fmt.Sscanf(strings.TrimPrefix(strings.TrimSpace(s), "data: "), "%d", &v)
		if v < prev {
			inversions++
		}
		prev = v
	}
	if inversions > 0 {
		t.Fatalf("FIFO + async workers delivered %d out-of-order frames", inversions)
	}
}

// 6. Lock-free engine receives messages published right after Subscribe returns.
func TestPOC_LockFreeSubscribeGap(t *testing.T) {
	b := blazesse.NewBroadcaster(blazesse.NewEngineLockFree(), blazesse.WithDeliveryMode(blazesse.DeliveryFIFO))
	defer func() { _ = b.Close() }()

	sub, unsub := b.Subscribe("room")
	defer unsub()

	b.Publish("room", frame.Text("welcome"))
	select {
	case <-sub.Recv():
		b := sub.Fetch()
		if len(b) == 0 || b[0].String() != "data: welcome\n\n" {
			t.Fatalf("unexpected message payload: %v", b)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("frame published after Subscribe() returned was dropped")
	}
}

// 7. ReconfigWait=true + concurrent Close does not hang Subscribe.
func TestPOC_ReconfigWaitHangOnClose(t *testing.T) {
	for i := 0; i < 50; i++ {
		b := blazesse.NewBroadcaster(blazesse.NewEngineLockFree(blazesse.WithReconfigWait(true), blazesse.WithReconfigInterval(50*time.Millisecond)))
		var wg sync.WaitGroup
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, u := b.Subscribe("x")
			u()
			close(done)
		}()
		time.Sleep(time.Millisecond)
		_ = b.Close()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("iteration %d: Subscribe hung after Close with ReconfigWait=true", i)
		}
		wg.Wait()
	}
}

type sseEvent struct{ event, data string }

func parseSSE(s string) []sseEvent {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	var out []sseEvent
	var cur sseEvent
	var dataLines []string
	has := false
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if has {
				cur.data = strings.Join(dataLines, "\n")
				out = append(out, cur)
			}
			cur, dataLines, has = sseEvent{}, nil, false
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		k, v, _ := strings.Cut(line, ":")
		v = strings.TrimPrefix(v, " ")
		switch k {
		case "event":
			cur.event = v
		case "data":
			dataLines = append(dataLines, v)
			has = true
		}
	}
	if has {
		cur.data = strings.Join(dataLines, "\n")
		out = append(out, cur)
	}
	return out
}
