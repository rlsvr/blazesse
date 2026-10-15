package broadcaster

import "sync/atomic"

// MetricsCollector defines hooks for observing broadcaster and stream activity.
type MetricsCollector interface {
	OnClientConnected(topic string)
	OnClientDisconnected(topic string)
	OnFramePublished(topic string, bytes int)
	OnFrameConflated(topic string)
	OnWriteTimeout(topic string)
	OnBufferOverflow(topic string)
}

// NoopMetrics is a default no-op implementation of MetricsCollector.
type NoopMetrics struct{}

func (NoopMetrics) OnClientConnected(string)     {}
func (NoopMetrics) OnClientDisconnected(string)  {}
func (NoopMetrics) OnFramePublished(string, int) {}
func (NoopMetrics) OnFrameConflated(string)      {}
func (NoopMetrics) OnWriteTimeout(string)        {}
func (NoopMetrics) OnBufferOverflow(string)      {}

// CounterMetrics provides lock-free atomic counters for basic observability.
type CounterMetrics struct {
	ConnectedClients atomic.Int64
	TotalPublished   atomic.Uint64
	TotalConflated   atomic.Uint64
	TotalTimeouts    atomic.Uint64
	TotalOverflows   atomic.Uint64
	TotalBytesSent   atomic.Uint64
}

// NewCounterMetrics creates an initialized CounterMetrics collector.
func NewCounterMetrics() *CounterMetrics {
	return &CounterMetrics{}
}

func (c *CounterMetrics) OnClientConnected(string) {
	c.ConnectedClients.Add(1)
}

func (c *CounterMetrics) OnClientDisconnected(string) {
	c.ConnectedClients.Add(-1)
}

func (c *CounterMetrics) OnFramePublished(_ string, bytes int) {
	c.TotalPublished.Add(1)
	if bytes > 0 {
		c.TotalBytesSent.Add(uint64(bytes))
	}
}

func (c *CounterMetrics) OnFrameConflated(string) {
	c.TotalConflated.Add(1)
}

func (c *CounterMetrics) OnWriteTimeout(string) {
	c.TotalTimeouts.Add(1)
}

func (c *CounterMetrics) OnBufferOverflow(string) {
	c.TotalOverflows.Add(1)
}
