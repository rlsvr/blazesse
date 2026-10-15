package broadcaster

import (
	"errors"
	"net/http"
	"strings"
)

var (
	// ErrNoTopics is returned when an HTTP request specifies no topics to subscribe to.
	ErrNoTopics = errors.New("blazesse: missing topic in request")
)

// DefaultTopicExtractor extracts topics from query parameters (?topic=X and ?topics=X,Y).
// Topics are trimmed and deduplicated, preserving insertion order.
func DefaultTopicExtractor(r *http.Request) ([]string, error) {
	query := r.URL.Query()
	var topics []string
	seen := make(map[string]struct{})

	addTopic := func(t string) {
		t = strings.TrimSpace(t)
		if t != "" {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				topics = append(topics, t)
			}
		}
	}

	// Check ?topic=foo (supports multiple ?topic=foo&topic=bar)
	if list, ok := query["topic"]; ok {
		for _, t := range list {
			addTopic(t)
		}
	}

	// Check ?topics=foo,bar (comma separated)
	if list, ok := query["topics"]; ok {
		for _, item := range list {
			parts := strings.Split(item, ",")
			for _, p := range parts {
				addTopic(p)
			}
		}
	}

	if len(topics) == 0 {
		return nil, ErrNoTopics
	}
	return topics, nil
}

// validateRequest validates client limits, topics, and authorization hooks before upgrading.
func (b *Broadcaster) validateRequest(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	if b.closed.Load() {
		http.Error(w, ErrBroadcasterClosed.Error(), http.StatusServiceUnavailable)
		return nil, false
	}

	// Atomic connection reservation to prevent TOCTOU bursts exceeding MaxConnections
	if b.opts.MaxConnections > 0 {
		if b.activeConns.Add(1) > int64(b.opts.MaxConnections) {
			b.activeConns.Add(-1)
			w.Header().Set("Retry-After", "5")
			http.Error(w, "too many connections", http.StatusTooManyRequests)
			return nil, false
		}
	}

	// Helper to release reservation on validation error
	releaseConn := func() {
		if b.opts.MaxConnections > 0 {
			b.activeConns.Add(-1)
		}
	}

	// Extract topics from request
	topics, err := b.opts.TopicExtractor(r)
	if err != nil {
		releaseConn()
		http.Error(w, "bad request: missing topic", http.StatusBadRequest)
		return nil, false
	}

	// Validate topic count
	if b.opts.MaxTopicsPerConn > 0 && len(topics) > b.opts.MaxTopicsPerConn {
		releaseConn()
		http.Error(w, "too many topics requested", http.StatusBadRequest)
		return nil, false
	}

	// Validate topic length
	if b.opts.MaxTopicLen > 0 {
		for _, t := range topics {
			if len(t) > b.opts.MaxTopicLen {
				releaseConn()
				http.Error(w, "topic exceeds maximum length", http.StatusBadRequest)
				return nil, false
			}
		}
	}

	// Run OnConnect authorization/validation hook
	if b.opts.OnConnect != nil {
		if err := b.opts.OnConnect(r); err != nil {
			releaseConn()
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return nil, false
		}
	}

	// Run topic-aware authorization hook
	if b.opts.AuthorizeTopics != nil {
		if err := b.opts.AuthorizeTopics(r, topics); err != nil {
			releaseConn()
			http.Error(w, "forbidden", http.StatusForbidden)
			return nil, false
		}
	}

	return topics, true
}

func (b *Broadcaster) buildStreamOptions() []Option {
	streamOpts := make([]Option, 0, 3+len(b.opts.CustomHeaders))
	streamOpts = append(streamOpts,
		WithWriteTimeout(b.opts.WriteTimeout),
		WithHeartbeat(b.opts.PingInterval, b.opts.PingJitter),
		WithPingComment(b.opts.PingComment),
	)
	for k, v := range b.opts.CustomHeaders {
		streamOpts = append(streamOpts, WithHeader(k, v))
	}
	return streamOpts
}

func (b *Broadcaster) replayHistory(stream *Stream, topics []string, r *http.Request) (map[string]struct{}, error) {
	lastID := r.Header.Get("Last-Event-ID")
	if lastID == "" {
		lastID = r.URL.Query().Get("last_event_id")
	}
	if lastID == "" || b.replay == nil {
		return nil, nil
	}
	replayedIDs := make(map[string]struct{})
	for _, topic := range topics {
		if missed := b.replay.ReplayAfter(topic, lastID); len(missed) > 0 {
			for _, f := range missed {
				if id := f.ID(); id != "" {
					replayedIDs[id] = struct{}{}
				}
			}
			if err := stream.SendBatch(missed); err != nil {
				return nil, err
			}
		}
	}
	return replayedIDs, nil
}

// ServeHTTP implements http.Handler, enabling Broadcaster to be mounted directly into routers.
func (b *Broadcaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	topics, ok := b.validateRequest(w, r)
	if !ok {
		return
	}
	if b.opts.MaxConnections > 0 {
		defer b.activeConns.Add(-1)
	}

	b.handlersWg.Add(1)
	defer b.handlersWg.Done()

	stream, err := Upgrade(w, r, b.buildStreamOptions()...)
	if err != nil {
		return
	}
	defer func() { _ = stream.Close() }()

	// Subscribe client to topics
	sub, unsubscribe := b.SubscribeMulti(topics)
	defer unsubscribe()

	// Replay missed historical events if Last-Event-ID is present and replay is enabled
	replayedIDs, err := b.replayHistory(stream, topics, r)
	if err != nil {
		return
	}

	defer func() {
		if b.opts.OnDisconnect != nil {
			b.opts.OnDisconnect(r, topics)
		}
	}()

	// Event loop: drain mailbox batches and write to stream
	for {
		select {
		case <-stream.Done():
			return

		case <-sub.Done():
			finalBatch := sub.Fetch()
			if len(replayedIDs) > 0 {
				filtered := finalBatch[:0]
				for _, f := range finalBatch {
					if id := f.ID(); id != "" {
						if _, dup := replayedIDs[id]; dup {
							continue
						}
					}
					filtered = append(filtered, f)
				}
				finalBatch = filtered
			}
			_ = stream.SendBatch(finalBatch)
			return

		case <-sub.Recv():
			batch := sub.Fetch()
			if len(batch) == 0 {
				if sub.IsClosed() {
					return
				}
				continue
			}

			// Deduplicate any overlapping frames that were already transmitted during replayHistory
			if len(replayedIDs) > 0 {
				filtered := batch[:0]
				for _, f := range batch {
					if id := f.ID(); id != "" {
						if _, dup := replayedIDs[id]; dup {
							delete(replayedIDs, id)
							continue
						}
					}
					filtered = append(filtered, f)
				}
				batch = filtered
				if len(batch) == 0 {
					continue
				}
			}

			// Single flush syscall for all batched frames
			if err := stream.SendBatch(batch); err != nil {
				if len(topics) > 0 {
					b.opts.Metrics.OnWriteTimeout(topics[0])
				}
				return
			}
		}
	}
}
