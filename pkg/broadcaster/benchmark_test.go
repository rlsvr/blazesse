package broadcaster_test

import (
	"fmt"
	"testing"

	"github.com/rlsvr/blazesse/pkg/broadcaster"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func BenchmarkFanout_10kSubscribers(b *testing.B) {
	broad := broadcaster.New(broadcaster.WithShards(64))
	defer func() { _ = broad.Close() }()

	const numSubs = 10000
	const topic = "market.crypto.BTC"

	for i := 0; i < numSubs; i++ {
		_, _ = broad.Subscribe(topic)
	}

	f := frame.Text(`{"symbol":"BTC-USD","price":68450.25,"volume":1420.5}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		broad.Publish(topic, f)
	}
}

func BenchmarkFanout_50kSubscribers(b *testing.B) {
	broad := broadcaster.New(broadcaster.WithShards(128))
	defer func() { _ = broad.Close() }()

	const numSubs = 50000
	const topic = "market.tickers.all"

	for i := 0; i < numSubs; i++ {
		_, _ = broad.Subscribe(topic)
	}

	f := frame.Text(`{"symbol":"ETH-USD","price":3450.10,"volume":8200.0}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		broad.Publish(topic, f)
	}
}

func BenchmarkPublish_MultiTopicShards(b *testing.B) {
	broad := broadcaster.New(broadcaster.WithShards(64))
	defer func() { _ = broad.Close() }()

	const topicsCount = 100
	const subsPerTopic = 100

	for t := 0; t < topicsCount; t++ {
		topic := fmt.Sprintf("ticker.%d", t)
		for s := 0; s < subsPerTopic; s++ {
			_, _ = broad.Subscribe(topic)
		}
	}

	f := frame.Text(`{"update":true}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("ticker.%d", i%topicsCount)
		broad.Publish(topic, f)
	}
}
