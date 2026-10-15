package frame_test

import (
	"testing"
	"time"

	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestFrame_Text(t *testing.T) {
	f := frame.Text("hello world")
	expected := "data: hello world\n\n"
	if f.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f.String())
	}
	if f.Len() != len(expected) {
		t.Fatalf("expected len %d, got %d", len(expected), f.Len())
	}
}

func TestFrame_Event(t *testing.T) {
	f := frame.Event("greeting", "hello world")
	expected := "event: greeting\ndata: hello world\n\n"
	if f.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f.String())
	}
}

func TestFrame_Comment(t *testing.T) {
	f := frame.Comment("heartbeat")
	expected := ": heartbeat\n\n"
	if f.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f.String())
	}
}

func TestFrame_Ping(t *testing.T) {
	f1 := frame.Ping()
	f2 := frame.Ping()
	expected := ": ping\n\n"
	if f1.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f1.String())
	}
	if f1 != f2 {
		t.Fatal("expected frame.Ping() to return shared pre-allocated instance")
	}
}

func TestFrame_MultilineData(t *testing.T) {
	multiline := "line1\nline2\nline3"
	f := frame.Format("update", "msg-123", 5*time.Second, []byte(multiline))
	expected := "id: msg-123\nevent: update\nretry: 5000\ndata: line1\ndata: line2\ndata: line3\n\n"
	if f.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f.String())
	}
}

func TestFrame_EmptyData(t *testing.T) {
	f := frame.Format("tick", "", 0, nil)
	expected := "event: tick\ndata: \n\n"
	if f.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f.String())
	}
}

func TestFrame_FromRaw(t *testing.T) {
	raw := []byte("event: raw\ndata: custom\n\n")
	f := frame.FromRaw(raw)
	if f.String() != string(raw) {
		t.Fatalf("expected %q, got %q", string(raw), f.String())
	}
}

func BenchmarkFrame_Text(b *testing.B) {
	payload := `{"symbol":"BTC-USD","price":68450.25,"volume":1420.5}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = frame.Text(payload)
	}
}

func BenchmarkFrame_Multiline(b *testing.B) {
	payload := []byte("{\n  \"symbol\": \"BTC-USD\",\n  \"price\": 68450.25\n}")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = frame.Format("price_update", "1001", 3000*time.Millisecond, payload)
	}
}
