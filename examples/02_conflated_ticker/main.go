package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/rlsvr/blazesse"
)

func main() {
	metrics := blazesse.NewCounterMetrics()

	// High-throughput sharded broadcaster with conflated delivery
	b := blazesse.NewBroadcaster(
		blazesse.WithShards(64),
		blazesse.WithDeliveryMode(blazesse.DeliveryConflated),
		blazesse.WithWriteTimeout(3*time.Second),
		blazesse.WithHeartbeat(15*time.Second, 5*time.Second),
		blazesse.WithMetrics(metrics),
		blazesse.WithHeader("Access-Control-Allow-Origin", "*"),
	)
	defer func() { _ = b.Close() }()

	// Mount broadcaster directly into net/http
	http.Handle("/tickers", b)

	// Simulate high-frequency market feed (100 updates/sec across BTC, ETH, SOL)
	go simulateMarketFeed(b)

	// Periodically log metrics
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("[METRICS] Clients: %d | Published: %d | Conflated (saved bandwidth): %d",
				metrics.ConnectedClients.Load(),
				metrics.TotalPublished.Load(),
				metrics.TotalConflated.Load(),
			)
		}
	}()

	fmt.Println("🚀 Conflated Market Ticker Broadcaster running on http://localhost:8081/tickers")
	fmt.Println("👉 Subscribe single topic: curl -N 'http://localhost:8081/tickers?topic=crypto.BTC'")
	fmt.Println("👉 Subscribe multi-topics: curl -N 'http://localhost:8081/tickers?topics=crypto.BTC,crypto.ETH'")
	srv := &http.Server{
		Addr:              ":8081",
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func simulateMarketFeed(b *blazesse.Broadcaster) {
	symbols := []string{"crypto.BTC", "crypto.ETH", "crypto.SOL"}
	prices := map[string]float64{
		"crypto.BTC": 68450.0,
		"crypto.ETH": 3520.0,
		"crypto.SOL": 182.5,
	}

	ticker := time.NewTicker(10 * time.Millisecond) // 100 ticks/second
	defer ticker.Stop()

	seq := 0
	for range ticker.C {
		seq++
		//nolint:gosec // weak PRNG is sufficient for simulated market feed
		sym := symbols[rand.IntN(len(symbols))]
		//nolint:gosec // weak PRNG is sufficient for simulated market feed
		delta := (rand.Float64() - 0.49) * 5.0
		prices[sym] += delta

		payload := fmt.Sprintf(`{"seq": %d, "symbol": %q, "price": %.2f, "ts": %d}`,
			seq, sym, prices[sym], time.Now().UnixMilli())

		// Broadcast frame: 0 allocations per client
		b.Publish(sym, blazesse.EventFrame("tick", payload))
	}
}
