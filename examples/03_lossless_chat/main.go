package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/rlsvr/blazesse"
)

func main() {
	// Lossless FIFO broadcaster powered by github.com/rlsvr/inas
	b := blazesse.NewBroadcaster(
		blazesse.WithDeliveryMode(blazesse.DeliveryFIFO),
		blazesse.WithFIFOBufferSize(512),
		blazesse.WithOverflowPolicy(blazesse.OverflowDropOldest),
		blazesse.WithWriteTimeout(3*time.Second),
		blazesse.WithHeader("Access-Control-Allow-Origin", "*"),
	)
	defer func() { _ = b.Close() }()

	// SSE stream endpoint
	http.Handle("/chat", b)

	// Send message endpoint: curl -X POST 'http://localhost:8082/send?topic=general&user=alice&msg=hello'
	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		topic := r.URL.Query().Get("topic")
		user := r.URL.Query().Get("user")
		msg := r.URL.Query().Get("msg")

		if topic == "" || user == "" || msg == "" {
			http.Error(w, "missing topic, user, or msg query parameter", http.StatusBadRequest)
			return
		}

		payload := fmt.Sprintf(`{"user": %q, "msg": %q, "time": %q}`, user, msg, time.Now().Format(time.RFC3339))
		b.Publish(topic, blazesse.EventFrame("message", payload))

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "message dispatched")
	})

	fmt.Println("🚀 Lossless FIFO Chat Broadcaster running on http://localhost:8082")
	fmt.Println("👉 Connect SSE:  curl -N 'http://localhost:8082/chat?topic=general'")
	fmt.Println("👉 Send Message: curl -X POST 'http://localhost:8082/send?topic=general&user=alice&msg=Hello+World'")
	srv := &http.Server{
		Addr:              ":8082",
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
