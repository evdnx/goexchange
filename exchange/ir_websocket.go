package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/evdnx/goexchange/common"
	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/golog"
	"nhooyr.io/websocket"
)

// IRWebSocketClient manages a websocket connection to Independent Reserve
// and handles subscriptions and message dispatch.
type IRWebSocketClient struct {
	conn   *websocket.Conn
	url    string
	logger *golog.Logger
	mu     sync.RWMutex
	subs   map[string]func([]byte)
	// typed handlers
	orderHandlers map[string]func(OrderEvent)
	tradeHandlers map[string]func(TradeEvent)
	// track subscribed channels for resubscribe on reconnect
	subscribed map[string]bool
	connected  bool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewIRWebSocketClient creates a new IRWebSocketClient.
func NewIRWebSocketClient(baseURL string, logger *golog.Logger) *IRWebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &IRWebSocketClient{
		url:           baseURL,
		logger:        logger,
		subs:          make(map[string]func([]byte)),
		orderHandlers: make(map[string]func(OrderEvent)),
		tradeHandlers: make(map[string]func(TradeEvent)),
		subscribed:    make(map[string]bool),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Connect establishes the websocket connection.
func (c *IRWebSocketClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connected {
		return nil
	}
	conn, _, err := websocket.Dial(c.ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	c.conn = conn
	c.connected = true
	go c.readLoop()
	return nil
}

// Subscribe subscribes to one or more channels (e.g., orderbook-xbt, ticker-xbt)
func (c *IRWebSocketClient) Subscribe(channels []string, handler func([]byte)) error {
	if err := c.Connect(); err != nil {
		return err
	}
	msg := map[string]interface{}{
		"Event": "Subscribe",
		"Data":  channels,
	}
	b, _ := json.Marshal(msg)
	c.mu.Lock()
	for _, ch := range channels {
		c.subs[ch] = handler
		c.subscribed[ch] = true
	}
	c.mu.Unlock()
	return c.conn.Write(c.ctx, websocket.MessageText, b)
}

// Unsubscribe unsubscribes from one or more channels.
func (c *IRWebSocketClient) Unsubscribe(channels []string) error {
	msg := map[string]interface{}{
		"Event": "Unsubscribe",
		"Data":  channels,
	}
	b, _ := json.Marshal(msg)
	c.mu.Lock()
	for _, ch := range channels {
		delete(c.subs, ch)
		delete(c.subscribed, ch)
	}
	c.mu.Unlock()
	return c.conn.Write(c.ctx, websocket.MessageText, b)
}

// readLoop reads messages and dispatches to handlers.
func (c *IRWebSocketClient) readLoop() {
	for {
		_, data, err := c.conn.Read(c.ctx)
		if err != nil {
			c.logger.Warnf("IR WS read error: %v", err)
			// attempt reconnect asynchronously
			c.Close()
			c.attemptReconnect()
			return
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Warnf("IR WS invalid JSON: %v", err)
			continue
		}
		if err := c.processRawMessage(data); err != nil {
			c.logger.Warnf("IR WS process message error: %v", err)
		}
	}
}

// processRawMessage parses a raw websocket message and dispatches to handlers.
// This is separated for testability.
func (c *IRWebSocketClient) processRawMessage(data []byte) error {
	var env IRRawMessage
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("envelope unmarshal error: %w", err)
	}

	switch env.Event {
	case "Heartbeat":
		return nil
	case "NewOrder", "OrderChanged", "OrderCanceled":
		var oe OrderEvent
		if err := json.Unmarshal(env.Data, &oe); err != nil {
			return fmt.Errorf("order event unmarshal: %w", err)
		}
		ch := env.Channel
		c.mu.RLock()
		if h, ok := c.orderHandlers[ch]; ok {
			go h(oe)
		}
		if raw, ok := c.subs[ch]; ok {
			go raw(data)
		}
		c.mu.RUnlock()
		return nil
	case "Trade":
		var te TradeEvent
		if err := json.Unmarshal(env.Data, &te); err != nil {
			return fmt.Errorf("trade event unmarshal: %w", err)
		}
		ch := env.Channel
		c.mu.RLock()
		if h, ok := c.tradeHandlers[ch]; ok {
			go h(te)
		}
		if raw, ok := c.subs[ch]; ok {
			go raw(data)
		}
		c.mu.RUnlock()
		return nil
	default:
		ch := env.Channel
		c.mu.RLock()
		if raw, ok := c.subs[ch]; ok {
			go raw(data)
		}
		c.mu.RUnlock()
		return nil
	}
}

// attemptReconnect tries to reconnect and resubscribe channels with backoff
func (c *IRWebSocketClient) attemptReconnect() {
	go func() {
		backoff := time.Second
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			c.logger.Infof("attempting IR WS reconnect")
			err := c.Connect()
			if err == nil {
				// resubscribe channels
				c.mu.RLock()
				channels := make([]string, 0, len(c.subscribed))
				for ch := range c.subscribed {
					channels = append(channels, ch)
				}
				c.mu.RUnlock()
				if len(channels) > 0 {
					msg := map[string]interface{}{"Event": "Subscribe", "Data": channels}
					b, _ := json.Marshal(msg)
					_ = c.conn.Write(c.ctx, websocket.MessageText, b)
				}
				return
			}
			c.logger.Warnf("IR WS reconnect failed: %v; retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}()
}

// IRRawMessage is the common websocket envelope
type IRRawMessage struct {
	Event   string          `json:"Event"`
	Channel string          `json:"Channel"`
	Nonce   int64           `json:"Nonce,omitempty"`
	Time    int64           `json:"Time,omitempty"`
	Data    json.RawMessage `json:"Data,omitempty"`
}

// OrderEvent represents orderbook events: NewOrder, OrderChanged, OrderCanceled
type OrderEvent struct {
	OrderType        string             `json:"OrderType"`
	OrderGuid        string             `json:"OrderGuid"`
	ClientId         string             `json:"ClientId,omitempty"`
	Price            map[string]float64 `json:"Price,omitempty"`
	Volume           float64            `json:"Volume,omitempty"`
	CreatedTimestamp string             `json:"CreatedTimestampUtc,omitempty"`
	FeePercent       float64            `json:"FeePercent,omitempty"`
}

// TradeEvent represents a trade published on the ticker channel
type TradeEvent struct {
	TradeGuid     string             `json:"TradeGuid"`
	TradeDate     string             `json:"TradeDate"`
	Volume        float64            `json:"Volume"`
	Price         map[string]float64 `json:"Price,omitempty"`
	BidGuid       string             `json:"BidGuid,omitempty"`
	OfferGuid     string             `json:"OfferGuid,omitempty"`
	BidClientId   string             `json:"BidClientId,omitempty"`
	OfferClientId string             `json:"OfferClientId,omitempty"`
	Side          string             `json:"Side,omitempty"`
}

// ToCommonOrder converts an IR OrderEvent into a simplified common.Order.
// The priceCurrency parameter selects which price to use from the Price map (e.g., "AUD").
func (oe OrderEvent) ToCommonOrder(symbol, exchange, priceCurrency string) common.Order {
	return oe.ToCommonOrderWithEvent(symbol, exchange, priceCurrency, "")
}

// ToCommonOrderWithEvent converts an IR OrderEvent into a simplified common.Order.
// The priceCurrency parameter selects which price to use from the Price map (e.g., "AUD").
// The event parameter (e.g., "NewOrder", "OrderChanged", "OrderCanceled") is used to map status.
func (oe OrderEvent) ToCommonOrderWithEvent(symbol, exchange, priceCurrency, event string) common.Order {
	side := common.OrderSideBuy
	if strings.Contains(strings.ToLower(oe.OrderType), "offer") {
		side = common.OrderSideSell
	}

	otype := common.OrderTypeLimit
	if strings.HasPrefix(strings.ToLower(oe.OrderType), "market") {
		otype = common.OrderTypeMarket
	}

	price := 0.0
	if oe.Price != nil {
		if v, ok := oe.Price[strings.ToLower(priceCurrency)]; ok {
			price = v
		} else if v, ok := oe.Price[strings.ToUpper(priceCurrency)]; ok {
			price = v
		} else {
			// pick first available price
			for _, v := range oe.Price {
				price = v
				break
			}
		}
	}

	createdAt := time.Now()
	if oe.CreatedTimestamp != "" {
		if ts, err := time.Parse(time.RFC3339, oe.CreatedTimestamp); err == nil {
			createdAt = ts
		}
	}

	// Map event to status
	status := common.OrderStatusNew
	switch strings.ToLower(event) {
	case "neworder":
		status = common.OrderStatusNew
	case "orderchanged":
		if oe.Volume == 0 {
			status = common.OrderStatusFilled
		} else {
			status = common.OrderStatusPartiallyFilled
		}
	case "ordercanceled":
		status = common.OrderStatusCancelled
	default:
		if oe.Volume == 0 {
			status = common.OrderStatusFilled
		}
	}

	return common.Order{
		ID:              oe.OrderGuid,
		ClientOrderID:   oe.ClientId,
		Symbol:          symbol,
		Side:            side,
		Type:            otype,
		Status:          status,
		Price:           price,
		Amount:          oe.Volume,
		FilledAmount:    0,
		RemainingAmount: oe.Volume,
		Fee:             oe.FeePercent,
		CreatedAt:       createdAt,
		UpdatedAt:       createdAt,
		Quantity:        oe.Volume,
		Timestamp:       createdAt,
	}
}

// ToCommonTrade converts an IR TradeEvent into a models.Trade. priceCurrency selects which price to use.
func (te TradeEvent) ToCommonTrade(symbol, exchange, priceCurrency string) models.Trade {
	price := 0.0
	if te.Price != nil {
		if v, ok := te.Price[strings.ToLower(priceCurrency)]; ok {
			price = v
		} else if v, ok := te.Price[strings.ToUpper(priceCurrency)]; ok {
			price = v
		} else {
			for _, v := range te.Price {
				price = v
				break
			}
		}
	}

	execTime := time.Now()
	if te.TradeDate != "" {
		if t, err := time.Parse(time.RFC3339, te.TradeDate); err == nil {
			execTime = t
		}
	}

	tradeType := strings.ToLower(te.Side)
	if tradeType == "" {
		tradeType = ""
	}

	return models.Trade{
		ID:            te.TradeGuid,
		Symbol:        symbol,
		Exchange:      exchange,
		Type:          tradeType,
		Quantity:      te.Volume,
		Price:         price,
		Total:         te.Volume * price,
		ExecutionTime: execTime,
	}
}

// SubscribeOrderBook subscribes to an orderbook channel and registers a typed handler.
func (c *IRWebSocketClient) SubscribeOrderBook(cryptoPrimary string, handler func(OrderEvent)) error {
	ch := fmt.Sprintf("orderbook-%s", strings.ToLower(cryptoPrimary))
	c.mu.Lock()
	c.orderHandlers[ch] = handler
	c.subscribed[ch] = true
	c.mu.Unlock()
	return c.Subscribe([]string{ch}, func(b []byte) {})
}

// SubscribeTickerTrades subscribes to a ticker-{crypto} channel and registers a trade handler.
func (c *IRWebSocketClient) SubscribeTickerTrades(cryptoPrimary string, handler func(TradeEvent)) error {
	ch := fmt.Sprintf("ticker-%s", strings.ToLower(cryptoPrimary))
	c.mu.Lock()
	c.tradeHandlers[ch] = handler
	c.subscribed[ch] = true
	c.mu.Unlock()
	return c.Subscribe([]string{ch}, func(b []byte) {})
}

// Close closes the websocket connection.
func (c *IRWebSocketClient) Close() {
	c.cancel()
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close(websocket.StatusNormalClosure, "closing")
	}
	c.connected = false
	c.mu.Unlock()
}
