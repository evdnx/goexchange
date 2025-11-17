package exchange

import (
	"errors"
	"fmt"
	"time"

	"github.com/evdnx/goexchange/models"
)

// ExchangeWrapper wraps existing exchange clients to implement the Exchange interface
// required by the FallbackManager and UnifiedClient
type ExchangeWrapper struct {
	name   string
	client interface{} // The actual exchange client (BinanceClient, CoinbaseClient, etc.)
}

// NewExchangeWrapper creates a new exchange wrapper
func NewExchangeWrapper(name string, client interface{}) *ExchangeWrapper {
	return &ExchangeWrapper{
		name:   name,
		client: client,
	}
}

// GetName returns the configured exchange name
func (w *ExchangeWrapper) GetName() string {
	return w.name
}

// IsHealthy checks if the exchange is healthy
func (w *ExchangeWrapper) IsHealthy() bool {
	// Try to use the IsHealthy method if it exists
	if client, ok := w.client.(interface{ IsHealthy() bool }); ok {
		return client.IsHealthy()
	}

	// Otherwise, assume it's healthy
	return true
}

// FetchMarketData fetches market data from the exchange
func (w *ExchangeWrapper) FetchMarketData(symbol string) (models.MarketData, error) {
	if client, ok := w.client.(interface {
		FetchMarketData(string) (models.MarketData, error)
	}); ok {
		return client.FetchMarketData(symbol)
	}
	return models.MarketData{}, ErrMethodNotImplemented
}

// GetOrderBook gets the order book from the exchange
func (w *ExchangeWrapper) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	if client, ok := w.client.(interface {
		GetOrderBook(string, int) (*models.OrderBook, error)
	}); ok {
		return client.GetOrderBook(symbol, depth)
	}
	return nil, ErrMethodNotImplemented
}

// GetBalance gets the balance from the exchange
func (w *ExchangeWrapper) GetBalance(asset string) (*Balance, error) {
	if client, ok := w.client.(interface {
		GetBalance(string) (*Balance, error)
	}); ok {
		return client.GetBalance(asset)
	}
	return nil, ErrMethodNotImplemented
}

// PlaceOrder places an order on the exchange
func (w *ExchangeWrapper) PlaceOrder(order interface{}) (string, error) {
	// Try strongly typed order signatures first
	switch ord := order.(type) {
	case Order:
		if client, ok := w.client.(interface {
			PlaceOrder(Order) (string, error)
		}); ok {
			return client.PlaceOrder(ord)
		}
	case *Order:
		if client, ok := w.client.(interface {
			PlaceOrder(Order) (string, error)
		}); ok && ord != nil {
			return client.PlaceOrder(*ord)
		}
	}

	if client, ok := w.client.(interface {
		PlaceOrder(interface{}) (string, error)
	}); ok {
		return client.PlaceOrder(order)
	}

	return "", ErrMethodNotImplemented
}

// CancelOrder cancels an order on the exchange
func (w *ExchangeWrapper) CancelOrder(symbol string, orderID string) error {
	if client, ok := w.client.(interface{ CancelOrder(string, string) error }); ok {
		return client.CancelOrder(symbol, orderID)
	}
	return ErrMethodNotImplemented
}

// GetOrderStatus gets the status of an order on the exchange
func (w *ExchangeWrapper) GetOrderStatus(orderID string) (OrderStatus, error) {
	if client, ok := w.client.(interface {
		GetOrderStatus(string) (OrderStatus, error)
	}); ok {
		return client.GetOrderStatus(orderID)
	}
	return OrderStatusNew, ErrMethodNotImplemented
}

// GetTicker gets the ticker from the exchange
func (w *ExchangeWrapper) GetTicker(symbol string) (*models.Ticker, error) {
	if client, ok := w.client.(interface {
		GetTicker(string) (*models.Ticker, error)
	}); ok {
		return client.GetTicker(symbol)
	}
	return nil, ErrMethodNotImplemented
}

// GetCandles gets candles from the exchange
func (w *ExchangeWrapper) GetCandles(symbol string, timeframe string, since time.Time, limit int) ([]models.Candle, error) {
	if client, ok := w.client.(interface {
		GetCandles(string, string, time.Time, int) ([]models.Candle, error)
	}); ok {
		return client.GetCandles(symbol, timeframe, since, limit)
	}
	return nil, ErrMethodNotImplemented
}

// GetTrades gets trades from the exchange
func (w *ExchangeWrapper) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	if client, ok := w.client.(interface {
		GetTrades(string, time.Time, int) ([]models.Trade, error)
	}); ok {
		return client.GetTrades(symbol, since, limit)
	}
	return nil, ErrMethodNotImplemented
}

// GetBalances gets all balances from the exchange
func (w *ExchangeWrapper) GetBalances() (map[string]*Balance, error) {
	if client, ok := w.client.(interface {
		GetBalances() (map[string]*Balance, error)
	}); ok {
		return client.GetBalances()
	}
	return nil, ErrMethodNotImplemented
}

// GetOrders gets orders from the exchange
func (w *ExchangeWrapper) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	if client, ok := w.client.(interface {
		GetOrders(string, time.Time, int) ([]Order, error)
	}); ok {
		return client.GetOrders(symbol, since, limit)
	}
	return nil, ErrMethodNotImplemented
}

// GetOrder returns a single order by ID if the underlying client exposes it
func (w *ExchangeWrapper) GetOrder(symbol string, orderID string) (*Order, error) {
	if client, ok := w.client.(interface {
		GetOrder(string, string) (*Order, error)
	}); ok {
		return client.GetOrder(symbol, orderID)
	}

	if client, ok := w.client.(interface {
		GetOrders(string, time.Time, int) ([]Order, error)
	}); ok {
		orders, err := client.GetOrders(symbol, time.Time{}, 250)
		if err != nil {
			return nil, err
		}
		for _, order := range orders {
			if order.ID == orderID {
				copy := order
				return &copy, nil
			}
		}
		return nil, fmt.Errorf("order %s not found", orderID)
	}

	return nil, ErrMethodNotImplemented
}

// CreateOrder delegates to the wrapped client if supported
func (w *ExchangeWrapper) CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error) {
	if client, ok := w.client.(interface {
		CreateOrder(string, OrderSide, OrderType, float64, float64) (*Order, error)
	}); ok {
		return client.CreateOrder(symbol, side, orderType, amount, price)
	}

	// Attempt to synthesize using PlaceOrder
	order := Order{
		Symbol: symbol,
		Side:   side,
		Type:   orderType,
		Amount: amount,
		Price:  price,
	}
	if id, err := w.PlaceOrder(order); err == nil {
		order.ID = id
		order.Status = OrderStatusNew
		order.Timestamp = time.Now()
		return &order, nil
	}
	return nil, ErrMethodNotImplemented
}

// GetTradingPairs exposes trading pairs if implemented
func (w *ExchangeWrapper) GetTradingPairs() ([]TradingPair, error) {
	if client, ok := w.client.(interface{ GetTradingPairs() ([]TradingPair, error) }); ok {
		return client.GetTradingPairs()
	}
	return nil, ErrMethodNotImplemented
}

// CancelAllOrders cancels all orders on the exchange
func (w *ExchangeWrapper) CancelAllOrders(symbol string) error {
	if client, ok := w.client.(interface{ CancelAllOrders(string) error }); ok {
		return client.CancelAllOrders(symbol)
	}
	return ErrMethodNotImplemented
}

// ErrMethodNotImplemented is returned when a method is not implemented by the exchange
var ErrMethodNotImplemented = errors.New("method not implemented")
