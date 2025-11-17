package exchange

import (
	"time"

	"github.com/evdnx/goexchange/cache"
	"github.com/evdnx/goexchange/models"
)

// CachedExchange wraps an Exchange with caching capabilities
type CachedExchange struct {
	exchange    Exchange
	marketCache *cache.MarketDataCache
}

// NewCachedExchange creates a new cached exchange
func NewCachedExchange(exchange Exchange) *CachedExchange {
	return &CachedExchange{
		exchange:    exchange,
		marketCache: cache.NewMarketDataCache(),
	}
}

// NewCachedExchangeWithConfig creates a new cached exchange with the given cache configuration
func NewCachedExchangeWithConfig(exchange Exchange, config cache.Config) *CachedExchange {
	return &CachedExchange{
		exchange:    exchange,
		marketCache: cache.NewMarketDataCacheWithConfig(config),
	}
}

// FetchMarketData fetches market data for a symbol, using cache when available
func (c *CachedExchange) FetchMarketData(symbol string) (models.MarketData, error) {
	// Try to get from cache first
	timestamp := time.Now().Truncate(time.Minute) // Round to the nearest minute

	if data, found := c.marketCache.GetMarketData(symbol, "1m", timestamp); found {
		return data, nil
	}

	// If not in cache, fetch from the exchange
	data, err := c.exchange.FetchMarketData(symbol)
	if err != nil {
		return models.MarketData{}, err
	}

	// Store in cache
	c.marketCache.SetMarketData(data, 0) // Use default TTL

	return data, nil
}

func (c *CachedExchange) PlaceOrder(order interface{}) (string, error) {
	return c.exchange.PlaceOrder(order)
}

func (c *CachedExchange) CancelOrder(symbol string, orderID string) error {
	return c.exchange.CancelOrder(symbol, orderID)
}

func (c *CachedExchange) CancelAllOrders(symbol string) error {
	return c.exchange.CancelAllOrders(symbol)
}

func (c *CachedExchange) GetOrderStatus(orderID string) (OrderStatus, error) {
	return c.exchange.GetOrderStatus(orderID)
}

func (c *CachedExchange) GetTradingPairs() ([]TradingPair, error) {
	return c.exchange.GetTradingPairs()
}

func (c *CachedExchange) GetTicker(symbol string) (*models.Ticker, error) {
	return c.exchange.GetTicker(symbol)
}

func (c *CachedExchange) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	return c.exchange.GetOrderBook(symbol, depth)
}

func (c *CachedExchange) GetCandles(symbol string, timeframe string, since time.Time, limit int) ([]models.Candle, error) {
	return c.exchange.GetCandles(symbol, timeframe, since, limit)
}

func (c *CachedExchange) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	return c.exchange.GetTrades(symbol, since, limit)
}

func (c *CachedExchange) GetBalance(asset string) (*Balance, error) {
	return c.exchange.GetBalance(asset)
}

func (c *CachedExchange) GetBalances() (map[string]*Balance, error) {
	return c.exchange.GetBalances()
}

func (c *CachedExchange) CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error) {
	return c.exchange.CreateOrder(symbol, side, orderType, amount, price)
}

func (c *CachedExchange) GetOrder(symbol string, orderID string) (*Order, error) {
	return c.exchange.GetOrder(symbol, orderID)
}

func (c *CachedExchange) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	return c.exchange.GetOrders(symbol, since, limit)
}

func (c *CachedExchange) GetName() string {
	return c.exchange.GetName()
}

func (c *CachedExchange) IsHealthy() bool {
	return c.exchange.IsHealthy()
}

// Stop stops the cache's background processes
func (c *CachedExchange) Stop() {
	c.marketCache.Stop()
}
