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
func DefaultTopicExtractor(r *http.Request) ([]string, error) {
	query := r.URL.Query()
	var topics []string

	// Check ?topic=foo (supports multiple ?topic=foo&topic=bar)
	if list, ok := query["topic"]; ok {
		for _, t := range list {
			t = strings.TrimSpace(t)
			if t != "" {
				topics = append(topics, t)
			}
		}
	}

	// Check ?topics=foo,bar (comma separated)
	if list, ok := query["topics"]; ok {
		for _, item := range list {
			parts := strings.Split(item, ",")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					topics = append(topics, p)
				}
			}
		}
	}

	if len(topics) == 0 {
		return nil, ErrNoTopics
	}
	return topics, nil
}

// ServeHTTP implements http.Handler, enabling Broadcaster to be mounted directly into routers.
func (b *Broadcaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract topics from request
	topics, err := b.opts.TopicExtractor(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Run OnConnect authorization/validation hook
	if b.opts.OnConnect != nil {
		if err := b.opts.OnConnect(r); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
	}

	// Upgrade HTTP connection to SSE stream
	stream, err := Upgrade(w, r,
		WithWriteTimeout(b.opts.WriteTimeout),
		WithHeartbeat(b.opts.PingInterval, b.opts.PingJitter),
	)
	if err != nil {
		return
	}
	defer func() { _ = stream.Close() }()

	// Subscribe client to topics
	sub, unsubscribe := b.SubscribeMulti(topics)
	defer unsubscribe()

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
				b.opts.Metrics.OnWriteTimeout(topics[0])
				return
			}
		}
	}
}
