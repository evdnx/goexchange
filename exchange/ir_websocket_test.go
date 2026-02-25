package exchange

import (
	"fmt"
	"os"
	"testing"
	"time"

	common "github.com/evdnx/goexchange/common"
	"github.com/evdnx/golog"
)

func TestIRWebSocketClient_OrderbookTicker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live websocket test in short mode")
	}

	logger := defaultTestLogger()
	ws := NewIRWebSocketClient("wss://websockets.independentreserve.com", logger)

	received := make(chan string, 2)

	handler := func(msg []byte) {
		received <- string(msg)
	}

	// Subscribe to XBT orderbook and ticker
	channels := []string{"orderbook-xbt", "ticker-xbt"}
	if err := ws.Subscribe(channels, handler); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	time.Sleep(2 * time.Second) // Wait for connection and events

	// Wait for at least one message from each channel
	count := 0
	for count < 2 {
		select {
		case msg := <-received:
			fmt.Fprintf(os.Stdout, "Received: %s\n", msg)
			count++
		case <-time.After(10 * time.Second):
			ws.Close()
			t.Fatal("timeout waiting for websocket events")
		}
	}

	ws.Close()
}

func defaultTestLogger() *golog.Logger {
	return common.DefaultLogger()
}
