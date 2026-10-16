package broadcaster

import (
	"context"

	"github.com/rlsvr/inas/pkg/registry"

	"github.com/rlsvr/blazesse/pkg/frame"
)

// lockFreeEngine uses inas/pkg/registry for actor-based Copy-On-Write entry snapshots.
// Reads during Publish are 100% lock-free (sub-nanosecond pre-sliced iteration).
// Reconfiguration mutations (sub/unsub/switch) are batched asynchronously, completely
// eliminating writer lock contention and fan-out pauses.
type lockFreeEngine struct {
	opts Options
	reg  *registry.Registry[string, Subscriber]
}

func newLockFreeEngine(ctx context.Context, opts Options) *lockFreeEngine {
	interval := opts.ReconfigInterval
	if interval <= 0 {
		interval = registry.DefaultReconfigInterval
	}
	batchCap := opts.ReconfigBatchSize
	if batchCap <= 0 {
		batchCap = registry.DefaultReconfigBatchSize
	}

	regOpts := registry.Options{
		ReconfigInterval:  interval,
		ReconfigBatchSize: batchCap,
		ReconfigWait:      opts.ReconfigWait,
		QueueCapacity:     4096,
	}

	hooks := registry.Hooks[string, Subscriber]{
		OnRegister: func(topic string, _ Subscriber) {
			opts.Metrics.OnClientConnected(topic)
		},
		OnDeregister: func(topic string, _ Subscriber) {
			opts.Metrics.OnClientDisconnected(topic)
		},
	}

	return &lockFreeEngine{
		opts: opts,
		reg:  registry.New(ctx, regOpts, hooks),
	}
}

func (e *lockFreeEngine) subscribe(topics []string, sub Subscriber) func() {
	cleanTopics := make([]string, 0, len(topics))
	for _, t := range topics {
		if t != "" {
			cleanTopics = append(cleanTopics, t)
		}
	}

	unsub := e.reg.Register(cleanTopics, sub)
	return func() {
		unsub()
		sub.Close()
	}
}

func (e *lockFreeEngine) publish(topic string, f *frame.Frame) {
	snap := e.reg.Snapshot()
	if snap == nil {
		return
	}
	members := snap.Members(topic)
	for _, client := range members {
		client.Push(topic, f)
	}
	e.opts.Metrics.OnFramePublished(topic, f.Len())
}

func (e *lockFreeEngine) publishAll(f *frame.Frame) {
	snap := e.reg.Snapshot()
	if snap == nil {
		return
	}
	all := snap.AllMembers()
	for _, client := range all {
		client.Push("*", f)
	}
}

func (e *lockFreeEngine) activeSubscribers(topic string) int {
	return e.reg.MemberCount(topic)
}

func (e *lockFreeEngine) close() {
	if e.reg != nil {
		snap := e.reg.Snapshot()
		if snap != nil {
			for _, client := range snap.AllMembers() {
				client.Close()
			}
		}
		_ = e.reg.Close()
	}
}
