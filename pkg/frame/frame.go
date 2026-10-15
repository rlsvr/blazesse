// Package frame provides immutable, pre-serialized Server-Sent Events (SSE) wire frames.
//
// In high-throughput broadcasting, serializing SSE messages per-client results in O(N)
// allocations and CPU overhead. Frame formats wire bytes once at ingest time so that
// N concurrent subscribers share a single immutable byte slice pointer with zero allocations.
package frame

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Frame represents a pre-serialized, immutable SSE wire payload (e.g. "event: ...\ndata: ...\n\n").
type Frame struct {
	raw []byte
}

// Raw returns the underlying wire-formatted bytes. Callers must treat this slice as read-only.
func (f *Frame) Raw() []byte {
	if f == nil {
		return nil
	}
	return f.raw
}

// Len returns the byte length of the wire frame.
func (f *Frame) Len() int {
	if f == nil {
		return 0
	}
	return len(f.raw)
}

// String returns the wire frame as a string.
func (f *Frame) String() string {
	if f == nil {
		return ""
	}
	return string(f.raw)
}

// FromRaw wraps an already wire-formatted byte slice into a Frame.
func FromRaw(raw []byte) *Frame {
	return &Frame{raw: raw}
}

// Text creates an SSE frame with only a data payload and no event name.
// This is the fastest path for single-line JSON or text payloads.
func Text(data string) *Frame {
	return Format("", "", 0, []byte(data))
}

// Event creates an SSE frame with an event name and data payload.
func Event(event, data string) *Frame {
	return Format(event, "", 0, []byte(data))
}

// Bytes creates an SSE frame with an event name and raw byte slice payload.
func Bytes(event string, data []byte) *Frame {
	return Format(event, "", 0, data)
}

// JSON formats an SSE frame with an event name and JSON-encoded payload.
func JSON(event string, v any) (*Frame, error) {
	return JSONWith(event, v, json.Marshal)
}

// JSONText formats an SSE data frame without an event name using JSON encoding.
func JSONText(v any) (*Frame, error) {
	return JSONWith("", v, json.Marshal)
}

// JSONWith formats a JSON frame using a user-provided marshaler function.
func JSONWith(event string, v any, marshal func(any) ([]byte, error)) (*Frame, error) {
	if marshal == nil {
		marshal = json.Marshal
	}
	data, err := marshal(v)
	if err != nil {
		return nil, err
	}
	return Format(event, "", 0, data), nil
}

// Comment creates an SSE comment frame (": <comment>\n\n"), typically used for keep-alive pings.
// The browser's EventSource ignores lines starting with a colon.
// Newlines in the comment are properly prefixed with ": " to prevent SSE injection.
func Comment(comment string) *Frame {
	var buf bytes.Buffer
	buf.Grow(len(comment) + 8)

	// Normalize CRLF / CR to LF
	comment = strings.ReplaceAll(comment, "\r\n", "\n")
	comment = strings.ReplaceAll(comment, "\r", "\n")

	lines := strings.Split(comment, "\n")
	for _, line := range lines {
		buf.WriteString(": ")
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	return &Frame{raw: buf.Bytes()}
}

var pingFrame = Comment("ping")

// Ping returns a shared, pre-allocated keep-alive heartbeat frame (": ping\n\n").
func Ping() *Frame {
	return pingFrame
}

var crlfReplacer = strings.NewReplacer("\r", "", "\n", "")

// sanitizeField strips \r and \n from field values like event and id to prevent header injection.
func sanitizeField(v string) string {
	if strings.ContainsAny(v, "\r\n") {
		return crlfReplacer.Replace(v)
	}
	return v
}

// Format constructs a W3C SSE specification-compliant wire frame.
//
// Per the WHATWG SSE standard:
// - Lines in data are delimited by CRLF ('\r\n'), LF ('\n'), or CR ('\r'). Each line is prefixed with "data: ".
// - Event names and IDs are sanitized to prevent SSE field/event injection.
func Format(event, id string, retry time.Duration, data []byte) *Frame {
	event = sanitizeField(event)
	id = sanitizeField(id)

	// Pre-estimate buffer size to minimize re-allocations
	estSize := len(data) + 16
	if event != "" {
		estSize += len(event) + 8
	}
	if id != "" {
		estSize += len(id) + 5
	}
	if retry > 0 {
		estSize += 16
	}

	var buf bytes.Buffer
	buf.Grow(estSize)

	if id != "" {
		buf.WriteString("id: ")
		buf.WriteString(id)
		buf.WriteByte('\n')
	}

	if event != "" {
		buf.WriteString("event: ")
		buf.WriteString(event)
		buf.WriteByte('\n')
	}

	if retry > 0 {
		buf.WriteString("retry: ")
		buf.WriteString(strconv.FormatInt(retry.Milliseconds(), 10))
		buf.WriteByte('\n')
	}

	writeData(&buf, data)
	buf.WriteByte('\n')

	return &Frame{raw: buf.Bytes()}
}

func writeData(buf *bytes.Buffer, data []byte) {
	if len(data) == 0 {
		buf.WriteString("data: \n")
		return
	}

	start := 0
	for i := 0; i < len(data); i++ {
		c := data[i]
		if c == '\n' || c == '\r' {
			buf.WriteString("data: ")
			buf.Write(data[start:i])
			buf.WriteByte('\n')

			// Handle \r\n as a single line delimiter
			if c == '\r' && i+1 < len(data) && data[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}

	if start < len(data) {
		buf.WriteString("data: ")
		buf.Write(data[start:])
		buf.WriteByte('\n')
	} else if len(data) > 0 && (data[len(data)-1] == '\n' || data[len(data)-1] == '\r') {
		// Trailing newline in data requires an empty data line per WHATWG spec
		buf.WriteString("data: \n")
	}
}
