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

	// Check connection limits (if configured)
	if b.opts.MaxConnections > 0 && b.TotalSubscribers() >= b.opts.MaxConnections {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return nil, false
	}

	// Extract topics from request
	topics, err := b.opts.TopicExtractor(r)
	if err != nil {
		http.Error(w, "bad request: missing topic", http.StatusBadRequest)
		return nil, false
	}

	// Validate topic count
	if b.opts.MaxTopicsPerConn > 0 && len(topics) > b.opts.MaxTopicsPerConn {
		http.Error(w, "too many topics requested", http.StatusBadRequest)
		return nil, false
	}

	// Validate topic length
	if b.opts.MaxTopicLen > 0 {
		for _, t := range topics {
			if len(t) > b.opts.MaxTopicLen {
				http.Error(w, "topic exceeds maximum length", http.StatusBadRequest)
				return nil, false
			}
		}
	}

	// Run OnConnect authorization/validation hook
	if b.opts.OnConnect != nil {
		if err := b.opts.OnConnect(r); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return nil, false
		}
	}

	// Run topic-aware authorization hook
	if b.opts.AuthorizeTopics != nil {
		if err := b.opts.AuthorizeTopics(r, topics); err != nil {
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

func (b *Broadcaster) replayHistory(stream *Stream, topics []string, r *http.Request) error {
	lastID := r.Header.Get("Last-Event-ID")
	if lastID == "" {
		lastID = r.URL.Query().Get("last_event_id")
	}
	if lastID == "" || b.replay == nil {
		return nil
	}
	for _, topic := range topics {
		if missed := b.replay.ReplayAfter(topic, lastID); len(missed) > 0 {
			if err := stream.SendBatch(missed); err != nil {
				return err
			}
		}
	}
	return nil
}

// ServeHTTP implements http.Handler, enabling Broadcaster to be mounted directly into routers.
func (b *Broadcaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	topics, ok := b.validateRequest(w, r)
	if !ok {
		return
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
	if err := b.replayHistory(stream, topics, r); err != nil {
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
			_ = stream.SendBatch(sub.Fetch())
			return

		case <-sub.Recv():
			batch := sub.Fetch()
			if len(batch) == 0 {
				if sub.IsClosed() {
					return
				}
				continue
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
