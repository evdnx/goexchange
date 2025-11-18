package common

import (
	"errors"
	"time"

	"github.com/evdnx/goexchange/models"
)

// ExchangeClient defines the interface for an exchange client
type ExchangeClient interface {
	GetName() string
	GetTradingPairs() ([]TradingPair, error)
	GetTicker(symbol string) (*models.Ticker, error)
	GetOrderBook(symbol string, depth int) (*models.OrderBook, error)
	GetCandles(symbol string, interval string, since time.Time, limit int) ([]models.Candle, error)
	GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error)
	GetBalance(currency string) (*Balance, error)
	GetBalances() (map[string]*Balance, error)
	CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error)
	GetOrder(symbol string, orderID string) (*Order, error)
	GetOrders(symbol string, since time.Time, limit int) ([]Order, error)
	CancelOrder(symbol string, orderID string) error
	CancelAllOrders(symbol string) error
	IsHealthy() bool
}

// BaseClient is a base implementation of the ExchangeClient interface
type BaseClient struct {
	name      string
	apiKey    string
	apiSecret string
	testnet   bool
	healthy   bool
}

// NewBaseClient creates a new base client
func NewBaseClient(name, apiKey, apiSecret string, testnet bool) *BaseClient {
	return &BaseClient{
		name:      name,
		apiKey:    apiKey,
		apiSecret: apiSecret,
		testnet:   testnet,
		healthy:   true,
	}
}

// GetName returns the name of the exchange
func (c *BaseClient) GetName() string { return c.name }

// APIKey returns the configured API key.
func (c *BaseClient) APIKey() string { return c.apiKey }

// APISecret returns the configured API secret.
func (c *BaseClient) APISecret() string { return c.apiSecret }

// IsTestnet reports whether the client is targeting a testnet environment.
func (c *BaseClient) IsTestnet() bool { return c.testnet }

// IsHealthy returns true if the exchange is healthy
func (c *BaseClient) IsHealthy() bool { return c.healthy }

// SetHealth sets the health status of the exchange
func (c *BaseClient) SetHealth(healthy bool) { c.healthy = healthy }

// ErrNotImplemented is returned when a method is not implemented
var ErrNotImplemented = errors.New("method not implemented")

// Default ExchangeClient interface implementations that return ErrNotImplemented.
func (c *BaseClient) GetTradingPairs() ([]TradingPair, error)  { return nil, ErrNotImplemented }
func (c *BaseClient) GetTicker(string) (*models.Ticker, error) { return nil, ErrNotImplemented }
func (c *BaseClient) GetOrderBook(string, int) (*models.OrderBook, error) {
	return nil, ErrNotImplemented
}
func (c *BaseClient) GetCandles(string, string, time.Time, int) ([]models.Candle, error) {
	return nil, ErrNotImplemented
}
func (c *BaseClient) GetTrades(string, time.Time, int) ([]models.Trade, error) {
	return nil, ErrNotImplemented
}
func (c *BaseClient) GetBalance(string) (*Balance, error)       { return nil, ErrNotImplemented }
func (c *BaseClient) GetBalances() (map[string]*Balance, error) { return nil, ErrNotImplemented }
func (c *BaseClient) CreateOrder(string, OrderSide, OrderType, float64, float64) (*Order, error) {
	return nil, ErrNotImplemented
}
func (c *BaseClient) GetOrder(string, string) (*Order, error) { return nil, ErrNotImplemented }
func (c *BaseClient) GetOrders(string, time.Time, int) ([]Order, error) {
	return nil, ErrNotImplemented
}
func (c *BaseClient) CancelOrder(string, string) error { return ErrNotImplemented }
func (c *BaseClient) CancelAllOrders(string) error     { return ErrNotImplemented }
