package exchange

import (
	"testing"
	"time"

	common "github.com/evdnx/goexchange/common"
)

// Live integration test for IR WebSocket. Skipped in short mode.
func TestIRWebSocket_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live IR websocket test in short mode")
	}

	logger := common.DefaultLogger()
	ws := NewIRWebSocketClient("wss://websockets.independentreserve.com", logger)

	received := make(chan []byte, 4)

	handler := func(b []byte) {
		received <- b
	}

	channels := []string{"orderbook-xbt", "ticker-xbt"}
	if err := ws.Subscribe(channels, handler); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	defer ws.Close()

	// Wait for at least two messages
	count := 0
	timeout := time.After(20 * time.Second)
	for count < 2 {
		select {
		case <-received:
			count++
		case <-timeout:
			t.Fatal("timeout waiting for live websocket messages")
		}
	}
}
