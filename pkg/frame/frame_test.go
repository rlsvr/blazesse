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

func TestFrame_JSON(t *testing.T) {
	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	p := payload{Name: "tick", Count: 10}

	f, err := frame.JSON("update", p)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	expected := "event: update\ndata: {\"name\":\"tick\",\"count\":10}\n\n"
	if f.String() != expected {
		t.Fatalf("expected %q, got %q", expected, f.String())
	}

	fText, err := frame.JSONText(p)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	expectedText := "data: {\"name\":\"tick\",\"count\":10}\n\n"
	if fText.String() != expectedText {
		t.Fatalf("expected %q, got %q", expectedText, fText.String())
	}

	// Custom marshaler
	fCustom, err := frame.JSONWith("custom", p, func(v any) ([]byte, error) {
		return []byte(`{"custom":true}`), nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	expectedCustom := "event: custom\ndata: {\"custom\":true}\n\n"
	if fCustom.String() != expectedCustom {
		t.Fatalf("expected %q, got %q", expectedCustom, fCustom.String())
	}
}

func TestFrame_InjectionPrevention(t *testing.T) {
	// 1. CR in data cannot forge fields
	userMsg := "hi\revent: admin\rdata: pwned\r\r"
	f := frame.Event("message", userMsg)
	expected := "event: message\ndata: hi\ndata: event: admin\ndata: data: pwned\ndata: \ndata: \n\n"
	if f.String() != expected {
		t.Fatalf("expected:\n%q\ngot:\n%q", expected, f.String())
	}

	// 2. LF / CR in event name is stripped
	f2 := frame.Event("x\ndata: injected\n\nevent: evil", "ok")
	expected2 := "event: xdata: injectedevent: evil\ndata: ok\n\n"
	if f2.String() != expected2 {
		t.Fatalf("expected:\n%q\ngot:\n%q", expected2, f2.String())
	}

	// 3. CRLF text is correctly split into lines without creating extra events
	f3 := frame.Text("line1\r\nline2")
	expected3 := "data: line1\ndata: line2\n\n"
	if f3.String() != expected3 {
		t.Fatalf("expected:\n%q\ngot:\n%q", expected3, f3.String())
	}

	// 4. Multiline comment properly prefixes every line with ": "
	f4 := frame.Comment("comment1\r\nevent: injected\ncomment2")
	expected4 := ": comment1\n: event: injected\n: comment2\n\n"
	if f4.String() != expected4 {
		t.Fatalf("expected:\n%q\ngot:\n%q", expected4, f4.String())
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
