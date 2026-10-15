package broadcaster

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlsvr/blazesse/pkg/frame"
)

var (
	// ErrNotFlusher is returned when http.ResponseWriter does not support flushing.
	ErrNotFlusher = errors.New("blazesse: http.ResponseWriter does not support flushing")
	// ErrStreamClosed is returned when attempting to write to a closed stream.
	ErrStreamClosed = errors.New("blazesse: stream is closed")
)

// Stream represents an active, physical 1-to-1 Server-Sent Events HTTP connection.
// It wraps http.ResponseWriter with per-write deadlines, automatic jittered keep-alive
// heartbeats, and safe concurrent writes.
type Stream struct {
	w           http.ResponseWriter
	rc          *http.ResponseController
	ctx         context.Context
	cancel      context.CancelFunc
	stopWatcher atomic.Pointer[func() bool]
	opts        Options
	mu          sync.Mutex // serializes writes between Send calls and heartbeat pings
	closed      atomic.Bool
	done        chan struct{}
	lastAct     atomic.Int64 // timestamp of last write to prevent redundant keep-alives
	pingRaw     []byte
}

// Upgrade initializes an HTTP connection for Server-Sent Events streaming.
// It sets required headers, commits headers with an immediate flush, and spawns
// a background keep-alive heartbeat loop.
func Upgrade(w http.ResponseWriter, r *http.Request, opts ...Option) (*Stream, error) {
	options := DefaultOptions()
	for _, opt := range opts {
		opt(&options)
	}

	rc := http.NewResponseController(w)

	// Set required SSE headers
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // Disable NGINX / Cloudflare proxy buffering

	// Apply any custom headers (e.g. CORS)
	for k, v := range options.CustomHeaders {
		h.Set(k, v)
	}

	// Verify flush capability before committing headers
	if !supportsFlush(w) {
		return nil, ErrNotFlusher
	}

	w.WriteHeader(http.StatusOK)

	// Commit headers immediately with a write deadline
	_ = rc.SetWriteDeadline(time.Now().Add(options.WriteTimeout))
	if err := rc.Flush(); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			return nil, err
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			return nil, ErrNotFlusher
		}
		flusher.Flush()
	}

	var pingBytes []byte
	switch options.PingComment {
	case "ping":
		pingBytes = frame.Ping().Raw()
	case "":
		pingBytes = []byte(":\n\n")
	default:
		pingBytes = frame.Comment(options.PingComment).Raw()
	}

	ctx, cancel := context.WithCancel(r.Context())
	s := &Stream{
		w:       w,
		rc:      rc,
		ctx:     ctx,
		cancel:  cancel,
		opts:    options,
		done:    make(chan struct{}),
		pingRaw: pingBytes,
	}
	s.lastAct.Store(time.Now().UnixNano())

	// Start background jittered heartbeat loop if enabled
	if options.PingInterval > 0 {
		go s.heartbeatLoop()
	}

	// Register context cancellation watcher using context.AfterFunc (0 goroutines spawned)
	stop := context.AfterFunc(r.Context(), func() {
		_ = s.Close()
	})
	s.stopWatcher.Store(&stop)

	return s, nil
}

// Send writes an immutable Frame to the client with a per-write deadline and flushes immediately.
func (s *Stream) Send(f *frame.Frame) error {
	if f == nil || f.Len() == 0 {
		return nil
	}
	if s.closed.Load() {
		return ErrStreamClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return ErrStreamClosed
	}

	_ = s.rc.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
	if _, err := s.w.Write(f.Raw()); err != nil {
		_ = s.closeLocked()
		return err
	}

	if err := s.rc.Flush(); err != nil {
		_ = s.closeLocked()
		return err
	}

	s.lastAct.Store(time.Now().UnixNano())
	return nil
}

// SendText sends a plain data payload without an event name.
func (s *Stream) SendText(data string) error {
	return s.Send(frame.Text(data))
}

// SendEvent sends a named event with a data payload.
func (s *Stream) SendEvent(event, data string) error {
	return s.Send(frame.Event(event, data))
}

// SendComment sends an SSE comment (": <comment>\n\n").
func (s *Stream) SendComment(comment string) error {
	return s.Send(frame.Comment(comment))
}

// SendBatch writes multiple frames and performs a single flush syscall at the end.
func (s *Stream) SendBatch(frames []*frame.Frame) error {
	if len(frames) == 0 {
		return nil
	}
	if s.closed.Load() {
		return ErrStreamClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return ErrStreamClosed
	}

	_ = s.rc.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
	for _, f := range frames {
		if f != nil && f.Len() > 0 {
			//nolint:gosec // raw contains pre-sanitized and validated SSE wire bytes
			if _, err := s.w.Write(f.Raw()); err != nil {
				_ = s.closeLocked()
				return err
			}
		}
	}

	if err := s.rc.Flush(); err != nil {
		_ = s.closeLocked()
		return err
	}

	s.lastAct.Store(time.Now().UnixNano())
	return nil
}

// Flush explicitly flushes buffered data to the client socket.
func (s *Stream) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rc.Flush()
}

// Context returns the stream context, which is canceled when the client disconnects or Close is called.
func (s *Stream) Context() context.Context {
	return s.ctx
}

// Done returns a channel that is closed when the stream is terminated.
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// Close gracefully terminates the stream.
// It is non-blocking and immediately propagates context cancellation and done signals,
// unblocking any in-flight writers without waiting on locks.
func (s *Stream) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	if stopFn := s.stopWatcher.Swap(nil); stopFn != nil && *stopFn != nil {
		(*stopFn)()
	}
	s.cancel()
	close(s.done)
	return nil
}

func (s *Stream) closeLocked() error {
	return s.Close()
}

func (s *Stream) heartbeatLoop() {
	var jitter time.Duration
	if s.opts.PingJitter > 0 {
		//nolint:gosec // weak PRNG is sufficient and faster for heartbeat jitter
		jitter = time.Duration(rand.Int64N(s.opts.PingJitter.Nanoseconds()))
	}
	interval := s.opts.PingInterval + jitter
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	ping := s.pingRaw

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			// If a message was sent recently (within half of PingInterval), skip ping
			last := time.Unix(0, s.lastAct.Load())
			if time.Since(last) < (s.opts.PingInterval / 2) {
				continue
			}

			s.mu.Lock()
			if s.closed.Load() {
				s.mu.Unlock()
				return
			}

			_ = s.rc.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
			_, wErr := s.w.Write(ping)
			fErr := s.rc.Flush()

			if wErr != nil || fErr != nil {
				_ = s.closeLocked()
				s.mu.Unlock()
				return
			}

			s.lastAct.Store(time.Now().UnixNano())
			s.mu.Unlock()
		}
	}
}

func supportsFlush(w http.ResponseWriter) bool {
	if _, ok := w.(http.Flusher); ok {
		return true
	}
	for {
		if u, ok := w.(interface{ Unwrap() http.ResponseWriter }); ok {
			w = u.Unwrap()
			if _, ok := w.(http.Flusher); ok {
				return true
			}
		} else {
			break
		}
	}
	return false
}
