package frame_test

import (
	"bufio"
	"strings"
	"testing"
	"time"

	"github.com/rlsvr/blazesse/pkg/frame"
)

type parsedSSEEvent struct {
	event string
	data  string
}

func parseSSEStream(s string) []parsedSSEEvent {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	var out []parsedSSEEvent
	var cur parsedSSEEvent
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
			cur, dataLines, has = parsedSSEEvent{}, nil, false
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

func FuzzFrameFormat(f *testing.F) {
	f.Add("message", "1", int64(1000), []byte("hello world"))
	f.Add("update\r\nevil", "2\r", int64(0), []byte("multiline\r\ndata\rtest"))
	f.Add("", "", int64(0), []byte(""))
	f.Add("admin", "id: 99", int64(500), []byte("data: injected\r\n\r\nevent: forged"))

	f.Fuzz(func(t *testing.T, event, id string, retryMs int64, data []byte) {
		var retry time.Duration
		if retryMs > 0 && retryMs < 60000 {
			retry = time.Duration(retryMs) * time.Millisecond
		}

		frm := frame.Format(event, id, retry, data)
		raw := frm.String()

		events := parseSSEStream(raw)

		// If data was non-empty, exactly 1 event must be parsed, never more (no event injection)
		if len(data) > 0 {
			if len(events) > 1 {
				t.Fatalf("CR/LF injection detected! Expected at most 1 event, parsed %d: %+v", len(events), events)
			}
			if len(events) == 1 {
				// Event name must not contain CR or LF
				cleanEvent := strings.NewReplacer("\r", "", "\n", "").Replace(event)
				if events[0].event != cleanEvent {
					t.Fatalf("mismatched event name: got %q, want %q", events[0].event, cleanEvent)
				}
			}
		}

		// Also test Text and Comment helpers for fuzz resilience
		textFrm := frame.Text(string(data))
		textEvents := parseSSEStream(textFrm.String())
		if len(data) > 0 && len(textEvents) > 1 {
			t.Fatalf("Text frame injection: expected at most 1 event, got %d", len(textEvents))
		}

		_ = frame.Comment(string(data)).String()
	})
}
