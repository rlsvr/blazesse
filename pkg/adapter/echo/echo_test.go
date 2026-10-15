package echo_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/rlsvr/blazesse"
	blazesseEcho "github.com/rlsvr/blazesse/pkg/adapter/echo"
)

func TestEcho_Upgrade(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	stream, err := blazesseEcho.Upgrade(c, blazesse.WithHeartbeat(0, 0))
	if err != nil {
		t.Fatalf("unexpected upgrade error: %v", err)
	}
	defer stream.Close()

	if err := stream.SendText("hello echo"); err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "data: hello echo\n\n") {
		t.Fatalf("expected data frame in body, got: %q", body)
	}

	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("unexpected Content-Type: %s", rec.Header().Get("Content-Type"))
	}
}

func TestEcho_Handler(t *testing.T) {
	b := blazesse.NewBroadcaster(
		blazesse.WithHeartbeat(0, 0),
		blazesse.WithDeliveryMode(blazesse.DeliveryConflated),
	)
	defer b.Close()

	e := echo.New()
	e.GET("/tickers", blazesseEcho.Handler(b))

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/tickers?topic=BTC", nil).WithContext(reqCtx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.ServeHTTP(rec, req)
	}()

	// Wait for subscription to be registered
	time.Sleep(50 * time.Millisecond)

	b.Publish("BTC", blazesse.TextFrame("price: 50000"))
	time.Sleep(50 * time.Millisecond)

	cancel()
	<-done

	body := rec.Body.String()
	scanner := bufio.NewScanner(strings.NewReader(body))
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "data: price: 50000" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected 'data: price: 50000' in body, got: %q", body)
	}
}
