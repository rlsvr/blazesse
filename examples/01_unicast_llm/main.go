package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rlsvr/blazesse"
)

func main() {
	http.HandleFunc("/chat", handleLLMChat)

	fmt.Println("🚀 Unicast LLM Streaming Example listening on http://localhost:8080/chat")
	fmt.Println("👉 Test with: curl -N http://localhost:8080/chat")
	srv := &http.Server{
		Addr:              ":8080",
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func handleLLMChat(w http.ResponseWriter, r *http.Request) {
	// Direct 1-to-1 connection upgrade - no topic hubs or background registries
	stream, err := blazesse.Upgrade(w, r,
		blazesse.WithWriteTimeout(3*time.Second),
		blazesse.WithHeader("Access-Control-Allow-Origin", "*"),
	)
	if err != nil {
		http.Error(w, "stream upgrade failed", http.StatusInternalServerError)
		return
	}
	defer func() { _ = stream.Close() }()

	prompt := r.URL.Query().Get("prompt")
	if prompt == "" {
		prompt = "Why is blazesse so fast?"
	}
	//nolint:gosec // demonstration logging in example
	log.Printf("streaming prompt of length: %d", len(prompt))

	simulatedResponse := "Blazesse achieves near-zero latency by serializing SSE frames once at ingest, " +
		"using cache-line padded topic shards to eliminate MESI cache contention, and employing lock-free " +
		"atomic pointer swaps for conflated state delivery."

	tokens := strings.Fields(simulatedResponse)

	// Stream token by token with realistic LLM latency
	for i, token := range tokens {
		select {
		case <-stream.Done():
			// Client disconnected early (e.g. closed browser tab)
			log.Println("client cancelled prompt")
			return
		default:
		}

		payload := fmt.Sprintf(`{"index": %d, "token": %q}`, i, token+" ")
		if err := stream.Send(blazesse.EventFrame("token", payload)); err != nil {
			log.Printf("write error: %v", err)
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	_ = stream.Send(blazesse.EventFrame("done", `{"status": "complete"}`))
}
