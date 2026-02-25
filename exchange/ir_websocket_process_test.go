package exchange

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	common "github.com/evdnx/goexchange/common"
)

func TestProcessRawMessage_OrderEventDispatch(t *testing.T) {
	logger := common.DefaultLogger()
	ws := NewIRWebSocketClient("wss://example", logger)

	ch := "orderbook-xbt"
	var wg sync.WaitGroup
	wg.Add(1)

	ws.orderHandlers[ch] = func(e OrderEvent) {
		if e.OrderGuid != "abcd-1234" {
			t.Errorf("unexpected OrderGuid: %s", e.OrderGuid)
		}
		wg.Done()
	}

	env := IRRawMessage{
		Event:   "NewOrder",
		Channel: ch,
		Time:    time.Now().UnixMilli(),
		Data:    nil,
	}

	// build order event data
	oe := OrderEvent{
		OrderType: "LimitBid",
		OrderGuid: "abcd-1234",
		Volume:    1.5,
	}
	b, _ := json.Marshal(oe)
	env.Data = b

	raw, _ := json.Marshal(env)

	if err := ws.processRawMessage(raw); err != nil {
		t.Fatalf("processRawMessage error: %v", err)
	}

	// wait for handler
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for order handler")
	}
}

func TestProcessRawMessage_TradeEventDispatch(t *testing.T) {
	logger := common.DefaultLogger()
	ws := NewIRWebSocketClient("wss://example", logger)

	ch := "ticker-xbt"
	var wg sync.WaitGroup
	wg.Add(1)

	ws.tradeHandlers[ch] = func(e TradeEvent) {
		if e.TradeGuid != "trade-1" {
			t.Errorf("unexpected TradeGuid: %s", e.TradeGuid)
		}
		wg.Done()
	}

	env := IRRawMessage{
		Event:   "Trade",
		Channel: ch,
		Time:    time.Now().UnixMilli(),
		Data:    nil,
	}

	te := TradeEvent{
		TradeGuid: "trade-1",
		Volume:    2.0,
	}
	b, _ := json.Marshal(te)
	env.Data = b

	raw, _ := json.Marshal(env)

	if err := ws.processRawMessage(raw); err != nil {
		t.Fatalf("processRawMessage error: %v", err)
	}

	// wait for handler
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for trade handler")
	}
}
