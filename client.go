package goexchange

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/evdnx/goexchange/cache"
	"github.com/evdnx/goexchange/internal/logutil"
	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/golog"
	"github.com/evdnx/gowscl"
)

// OrderSide, OrderType, OrderStatus and related constants are defined in exchange/order.go.

// ExchangeCapability represents a capability that an exchange may support
type ExchangeCapability string

const (
	// CapabilitySpotTrading represents spot trading capability
	CapabilitySpotTrading ExchangeCapability = "spot_trading"
	// CapabilityMarginTrading represents margin trading capability
	CapabilityMarginTrading ExchangeCapability = "margin_trading"
	// CapabilityFuturesTrading represents futures trading capability
	CapabilityFuturesTrading ExchangeCapability = "futures_trading"
	// CapabilityOptionsTrading represents options trading capability
	CapabilityOptionsTrading ExchangeCapability = "options_trading"
	// CapabilityLending represents lending capability
	CapabilityLending ExchangeCapability = "lending"
	// CapabilityStaking represents staking capability
	CapabilityStaking ExchangeCapability = "staking"
	// CapabilityWebSocket represents WebSocket support
	CapabilityWebSocket ExchangeCapability = "websocket"
	// CapabilityHistoricalData represents historical data capability
	CapabilityHistoricalData ExchangeCapability = "historical_data"
)

// AccountInfo represents account information
type AccountInfo struct {
	AccountType     string
	CanTrade        bool
	CanWithdraw     bool
	CanDeposit      bool
	Balances        map[string]float64
	UpdateTime      time.Time
	MakerCommission float64
	TakerCommission float64
}

// Client extends the basic ExchangeClient interface with additional capabilities
type Client interface {
	ExchangeClient

	// HasCapability checks if the exchange has a specific capability
	HasCapability(capability ExchangeCapability) bool

	// GetWebSocketClient returns the WebSocket client for the exchange
	GetWebSocketClient() *gowscl.Client

	// FetchHistoricalData fetches historical market data for a symbol
	FetchHistoricalData(symbol string, interval string, start, end time.Time) ([]models.MarketData, error)

	// FetchTickers fetches tickers for multiple symbols
	FetchTickers(symbols []string) (map[string]models.MarketData, error)

	// PlaceBulkOrders places multiple orders at once
	PlaceBulkOrders(orders []Order) ([]string, error)

	// CancelAllOrders cancels all open orders
	CancelAllOrders(symbol string) error

	// GetAccountInfo gets account information
	GetAccountInfo() (AccountInfo, error)

	// GetAllBalances gets all asset balances
	GetAllBalances() (map[string]float64, error)

	// Transfer transfers assets between accounts
	Transfer(asset string, amount float64, fromAccount, toAccount string) error

	// WithContext returns a new client with the given context
	WithContext(ctx context.Context) Client
}

// DefaultClient provides common functionality for exchange clients
type DefaultClient struct {
	client       ExchangeClient
	capabilities []ExchangeCapability
	wsClient     *gowscl.Client
	ctx          context.Context
}

// NewClient creates a new client wrapper with capability metadata
func NewClient(client ExchangeClient, capabilities []ExchangeCapability, wsClient *gowscl.Client) *DefaultClient {
	return &DefaultClient{
		client:       client,
		capabilities: capabilities,
		wsClient:     wsClient,
		ctx:          context.Background(),
	}
}

// GetName delegates to the embedded client
func (c *DefaultClient) GetName() string {
	return c.client.GetName()
}

// GetTicker delegates to the embedded client
func (c *DefaultClient) GetTicker(symbol string) (*models.Ticker, error) {
	return c.client.GetTicker(symbol)
}

// CreateOrder delegates to the embedded client
func (c *DefaultClient) CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error) {
	return c.client.CreateOrder(symbol, side, orderType, amount, price)
}

// CancelOrder delegates to the embedded client
func (c *DefaultClient) CancelOrder(symbol string, orderID string) error {
	return c.client.CancelOrder(symbol, orderID)
}

// GetOrder delegates to the embedded client
func (c *DefaultClient) GetOrder(symbol string, orderID string) (*Order, error) {
	return c.client.GetOrder(symbol, orderID)
}

// GetOrders delegates to the embedded client
func (c *DefaultClient) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	return c.client.GetOrders(symbol, since, limit)
}

// GetBalance delegates to the embedded client
func (c *DefaultClient) GetBalance(asset string) (*Balance, error) {
	return c.client.GetBalance(asset)
}

// GetBalances delegates to the embedded client
func (c *DefaultClient) GetBalances() (map[string]*Balance, error) {
	return c.client.GetBalances()
}

// GetOrderBook delegates to the embedded client
func (c *DefaultClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	return c.client.GetOrderBook(symbol, depth)
}

// GetCandles delegates to the embedded client
func (c *DefaultClient) GetCandles(symbol string, interval string, since time.Time, limit int) ([]models.Candle, error) {
	return c.client.GetCandles(symbol, interval, since, limit)
}

// GetTrades delegates to the embedded client
func (c *DefaultClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	return c.client.GetTrades(symbol, since, limit)
}

// CancelAllOrders delegates to the embedded client
func (c *DefaultClient) CancelAllOrders(symbol string) error {
	return c.client.CancelAllOrders(symbol)
}

// IsHealthy delegates to the embedded client
func (c *DefaultClient) IsHealthy() bool {
	return c.client.IsHealthy()
}

// HasCapability checks if the exchange has a specific capability
func (c *DefaultClient) HasCapability(capability ExchangeCapability) bool {
	for _, cap := range c.capabilities {
		if cap == capability {
			return true
		}
	}
	return false
}

// GetWebSocketClient returns the WebSocket client for the exchange
func (c *DefaultClient) GetWebSocketClient() *gowscl.Client {
	return c.wsClient
}

// GetTradingPairs delegates to the embedded client
func (c *DefaultClient) GetTradingPairs() ([]TradingPair, error) {
	return c.client.GetTradingPairs()
}

// WithContext returns a new client with the given context
func (c *DefaultClient) WithContext(ctx context.Context) Client {
	newClient := *c
	newClient.ctx = ctx
	return &newClient
}

// Default implementations for extended capabilities

// FetchHistoricalData fetches historical market data for a symbol
func (c *DefaultClient) FetchHistoricalData(symbol string, interval string, start, end time.Time) ([]models.MarketData, error) {
	return nil, nil
}

// FetchTickers fetches tickers for multiple symbols
func (c *DefaultClient) FetchTickers(symbols []string) (map[string]models.MarketData, error) {
	return nil, nil
}

// PlaceBulkOrders places multiple orders at once
func (c *DefaultClient) PlaceBulkOrders(orders []Order) ([]string, error) {
	return nil, nil
}

// GetAccountInfo gets account information
func (c *DefaultClient) GetAccountInfo() (AccountInfo, error) {
	return AccountInfo{}, nil
}

// GetAllBalances gets all asset balances
func (c *DefaultClient) GetAllBalances() (map[string]float64, error) {
	return nil, nil
}

// Transfer transfers assets between accounts
func (c *DefaultClient) Transfer(asset string, amount float64, fromAccount, toAccount string) error {
	return nil
}

// ResilientClientConfig contains configuration for the resilient client
type ResilientClientConfig struct {
	MaxRetries         int
	RetryDelay         time.Duration
	MaxRetryDelay      time.Duration
	RetryBackoffFactor float64
	OperationTimeout   time.Duration
}

// DefaultResilientClientConfig returns the default resilient client configuration
func DefaultResilientClientConfig() ResilientClientConfig {
	return ResilientClientConfig{
		MaxRetries:         3,
		RetryDelay:         100 * time.Millisecond,
		MaxRetryDelay:      10 * time.Second,
		RetryBackoffFactor: 2.0,
		OperationTimeout:   30 * time.Second,
	}
}

// MarketSnapshot describes a simplified, normalized view of market data
type MarketSnapshot struct {
	Symbol    string
	Bid       float64
	Ask       float64
	Price     float64
	Volume    float64
	Timestamp time.Time
	Exchange  string
}

// UnifiedClientOption configures a UnifiedClient
type UnifiedClientOption func(*UnifiedClient)

// UnifiedClient combines caching, resiliency, and a unified data surface.
type UnifiedClient struct {
	fallbackManager *FallbackManager
	config          ResilientClientConfig
	logger          *golog.Logger

	cacheEnabled bool
	cacheConfig  cache.Config
	cacheStore   *cache.Cache
}

const unifiedClientComponent = "unified_client"

// NewUnifiedClient creates a new unified client instance
func NewUnifiedClient(fallbackManager *FallbackManager, opts ...UnifiedClientOption) *UnifiedClient {
	if fallbackManager == nil {
		panic("fallback manager is required for UnifiedClient")
	}

	client := &UnifiedClient{
		fallbackManager: fallbackManager,
		config:          DefaultResilientClientConfig(),
		logger:          logutil.Default(),
		cacheEnabled:    true,
		cacheConfig:     cache.DefaultConfig(),
	}
	client.cacheStore = cache.New(client.cacheConfig)

	for _, opt := range opts {
		opt(client)
	}

	return client
}

// WithResilientClientConfig overrides the default resiliency configuration
func WithResilientClientConfig(cfg ResilientClientConfig) UnifiedClientOption {
	return func(c *UnifiedClient) {
		c.config = cfg
	}
}

// WithCacheConfig overrides cache behavior
func WithCacheConfig(cfg cache.Config) UnifiedClientOption {
	return func(c *UnifiedClient) {
		c.cacheConfig = cfg
		c.cacheEnabled = cfg.Enabled
		c.rebuildCache()
	}
}

// WithoutClientCache disables caching entirely
func WithoutClientCache() UnifiedClientOption {
	return func(c *UnifiedClient) {
		c.cacheEnabled = false
		if c.cacheStore != nil {
			c.cacheStore.Stop()
			c.cacheStore = nil
		}
	}
}

// Stop releases background resources held by the client
func (c *UnifiedClient) Stop() {
	if c.cacheStore != nil {
		c.cacheStore.Stop()
	}
}

func (c *UnifiedClient) rebuildCache() {
	if c.cacheStore != nil {
		c.cacheStore.Stop()
	}
	if c.cacheEnabled {
		c.cacheStore = cache.New(c.cacheConfig)
	} else {
		c.cacheStore = nil
	}
}

func (c *UnifiedClient) cacheKey(parts ...interface{}) string {
	return fmt.Sprint(parts...)
}

func (c *UnifiedClient) getCached(key string) (interface{}, bool) {
	if !c.cacheEnabled || c.cacheStore == nil {
		return nil, false
	}
	return c.cacheStore.Get(key)
}

func (c *UnifiedClient) setCache(key string, value interface{}, ttl time.Duration) {
	if !c.cacheEnabled || c.cacheStore == nil {
		return
	}
	if ttl <= 0 {
		ttl = c.cacheConfig.DefaultTTL
	}
	c.cacheStore.Set(key, value, ttl)
}

func (c *UnifiedClient) calculateBackoff(attempt int) time.Duration {
	delay := c.config.RetryDelay * time.Duration(float64(attempt)*c.config.RetryBackoffFactor)
	if delay > c.config.MaxRetryDelay {
		delay = c.config.MaxRetryDelay
	}
	return delay
}

func (c *UnifiedClient) sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *UnifiedClient) withOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || c.config.OperationTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.config.OperationTimeout)
}

func executeWithRetry[T any](c *UnifiedClient, ctx context.Context, operation string, fn func(Exchange) (T, error)) (T, string, error) {
	var zero T
	ctx, cancel := c.withOperationContext(ctx)
	defer cancel()

	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := c.calculateBackoff(attempt)
			c.logger.Debug(
				fmt.Sprintf("retrying %s attempt %d after %v", operation, attempt, delay),
				golog.String("component", unifiedClientComponent),
			)
			if err := c.sleepWithContext(ctx, delay); err != nil {
				return zero, "", fmt.Errorf("operation %s timed out during backoff: %w", operation, err)
			}
		}

		result, name, err := ExecuteWithFallback(c.fallbackManager, operation, fn)
		if err == nil {
			if attempt > 0 {
				c.logger.Info(
					fmt.Sprintf("operation %s recovered on %s after %d retries", operation, name, attempt),
					golog.String("component", unifiedClientComponent),
				)
			}
			return result, name, nil
		}

		lastErr = err
		c.logger.Warn(
			fmt.Sprintf("operation %s failed on attempt %d: %v", operation, attempt+1, err),
			golog.String("component", unifiedClientComponent),
		)
	}

	return zero, "", fmt.Errorf("operation %s failed after %d attempts: %w", operation, c.config.MaxRetries+1, lastErr)
}

func (c *UnifiedClient) cachedTicker(symbol string) (*models.Ticker, bool) {
	if value, ok := c.getCached(c.cacheKey("ticker:", symbol)); ok {
		if ticker, ok := value.(*models.Ticker); ok {
			return ticker, true
		}
	}
	return nil, false
}

func (c *UnifiedClient) cacheTicker(symbol string, ticker *models.Ticker) {
	if ticker == nil {
		return
	}
	copyTicker := *ticker
	c.setCache(c.cacheKey("ticker:", symbol), &copyTicker, c.cacheConfig.MarketDataTTL)
}

func (c *UnifiedClient) cachedOrderBook(symbol string, depth int) (*models.OrderBook, bool) {
	if value, ok := c.getCached(c.cacheKey("orderbook:", symbol, ":", depth)); ok {
		if ob, ok := value.(*models.OrderBook); ok {
			return ob, true
		}
	}
	return nil, false
}

func (c *UnifiedClient) cacheOrderBook(symbol string, depth int, ob *models.OrderBook) {
	if ob == nil {
		return
	}
	copyBook := *ob
	c.setCache(c.cacheKey("orderbook:", symbol, ":", depth), &copyBook, c.cacheConfig.MarketDataTTL)
}

// --- ExchangeClient interface implementation ------------------------------------

// GetName returns a composite name describing the primary exchange managed by this client
func (c *UnifiedClient) GetName() string {
	if c == nil || c.fallbackManager == nil {
		return "unified-client"
	}
	if name := c.fallbackManager.PrimaryExchange(); name != "" {
		return name
	}
	return "unified-client"
}

// GetTradingPairs returns supported trading pairs from the healthiest exchange
func (c *UnifiedClient) GetTradingPairs() ([]TradingPair, error) {
	result, _, err := executeWithRetry(c, context.Background(), "GetTradingPairs", func(exchange Exchange) ([]TradingPair, error) {
		return exchange.GetTradingPairs()
	})
	return result, err
}

// GetTicker returns ticker data, using cache when available
func (c *UnifiedClient) GetTicker(symbol string) (*models.Ticker, error) {
	res, _, err := c.GetTickerWithContext(context.Background(), symbol)
	return res, err
}

// GetTickerWithContext returns ticker data along with the exchange name
func (c *UnifiedClient) GetTickerWithContext(ctx context.Context, symbol string) (*models.Ticker, string, error) {
	if ticker, ok := c.cachedTicker(symbol); ok {
		return ticker, c.GetName(), nil
	}

	result, name, err := executeWithRetry(c, ctx, "GetTicker", func(exchange Exchange) (*models.Ticker, error) {
		return exchange.GetTicker(symbol)
	})
	if err != nil {
		return nil, name, err
	}

	c.cacheTicker(symbol, result)
	return result, name, nil
}

// GetOrderBook returns the current order book for a symbol
func (c *UnifiedClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	res, _, err := c.GetOrderBookWithContext(context.Background(), symbol, depth)
	return res, err
}

// GetOrderBookWithContext returns an order book and the exchange that served it
func (c *UnifiedClient) GetOrderBookWithContext(ctx context.Context, symbol string, depth int) (*models.OrderBook, string, error) {
	if ob, ok := c.cachedOrderBook(symbol, depth); ok {
		return ob, c.GetName(), nil
	}

	result, name, err := executeWithRetry(c, ctx, "GetOrderBook", func(exchange Exchange) (*models.OrderBook, error) {
		return exchange.GetOrderBook(symbol, depth)
	})
	if err != nil {
		return nil, name, err
	}

	c.cacheOrderBook(symbol, depth, result)
	return result, name, nil
}

// GetCandles returns candle data
func (c *UnifiedClient) GetCandles(symbol string, interval string, since time.Time, limit int) ([]models.Candle, error) {
	res, _, err := c.GetCandlesWithContext(context.Background(), symbol, interval, since, limit)
	return res, err
}

// GetCandlesWithContext returns candles and the exchange name
func (c *UnifiedClient) GetCandlesWithContext(ctx context.Context, symbol string, interval string, since time.Time, limit int) ([]models.Candle, string, error) {
	return executeWithRetry(c, ctx, "GetCandles", func(exchange Exchange) ([]models.Candle, error) {
		return exchange.GetCandles(symbol, interval, since, limit)
	})
}

// GetTrades returns trade history
func (c *UnifiedClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	res, _, err := c.GetTradesWithContext(context.Background(), symbol, since, limit)
	return res, err
}

// GetTradesWithContext returns trades and the exchange name
func (c *UnifiedClient) GetTradesWithContext(ctx context.Context, symbol string, since time.Time, limit int) ([]models.Trade, string, error) {
	return executeWithRetry(c, ctx, "GetTrades", func(exchange Exchange) ([]models.Trade, error) {
		return exchange.GetTrades(symbol, since, limit)
	})
}

// GetBalance returns a single asset balance
func (c *UnifiedClient) GetBalance(currency string) (*Balance, error) {
	res, _, err := c.GetBalanceWithContext(context.Background(), currency)
	return res, err
}

// GetBalanceWithContext returns a balance along with exchange metadata
func (c *UnifiedClient) GetBalanceWithContext(ctx context.Context, currency string) (*Balance, string, error) {
	return executeWithRetry(c, ctx, "GetBalance", func(exchange Exchange) (*Balance, error) {
		return exchange.GetBalance(currency)
	})
}

// GetBalances returns the full balance map
func (c *UnifiedClient) GetBalances() (map[string]*Balance, error) {
	res, _, err := c.GetBalancesWithContext(context.Background())
	return res, err
}

// GetBalancesWithContext returns balances and the exchange name
func (c *UnifiedClient) GetBalancesWithContext(ctx context.Context) (map[string]*Balance, string, error) {
	return executeWithRetry(c, ctx, "GetBalances", func(exchange Exchange) (map[string]*Balance, error) {
		return exchange.GetBalances()
	})
}

// CreateOrder creates a new order using unified parameters
func (c *UnifiedClient) CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error) {
	res, _, err := c.CreateOrderWithContext(context.Background(), symbol, side, orderType, amount, price)
	return res, err
}

// CreateOrderWithContext places an order and returns metadata including the serving exchange
func (c *UnifiedClient) CreateOrderWithContext(ctx context.Context, symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, string, error) {
	order, name, err := executeWithRetry(c, ctx, "CreateOrder", func(exchange Exchange) (*Order, error) {
		return exchange.CreateOrder(symbol, side, orderType, amount, price)
	})
	if err != nil {
		return nil, name, err
	}
	return order, name, nil
}

// PlaceOrderWithContext places a raw order payload and returns ID + exchange name
func (c *UnifiedClient) PlaceOrderWithContext(ctx context.Context, order interface{}) (string, string, error) {
	return executeWithRetry(c, ctx, "PlaceOrder", func(exchange Exchange) (string, error) {
		return exchange.PlaceOrder(order)
	})
}

// GetOrder fetches a single order by ID
func (c *UnifiedClient) GetOrder(symbol string, orderID string) (*Order, error) {
	res, _, err := c.GetOrderWithContext(context.Background(), symbol, orderID)
	return res, err
}

// GetOrderWithContext fetches an order along with the exchange used
func (c *UnifiedClient) GetOrderWithContext(ctx context.Context, symbol string, orderID string) (*Order, string, error) {
	return executeWithRetry(c, ctx, "GetOrder", func(exchange Exchange) (*Order, error) {
		return exchange.GetOrder(symbol, orderID)
	})
}

// GetOrders returns a slice of orders
func (c *UnifiedClient) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	res, _, err := c.GetOrdersWithContext(context.Background(), symbol, since, limit)
	return res, err
}

// GetOrdersWithContext returns orders plus metadata about which exchange served them
func (c *UnifiedClient) GetOrdersWithContext(ctx context.Context, symbol string, since time.Time, limit int) ([]Order, string, error) {
	return executeWithRetry(c, ctx, "GetOrders", func(exchange Exchange) ([]Order, error) {
		return exchange.GetOrders(symbol, since, limit)
	})
}

// CancelOrder cancels a specific order
func (c *UnifiedClient) CancelOrder(symbol string, orderID string) error {
	_, err := c.CancelOrderWithContext(context.Background(), symbol, orderID)
	return err
}

// CancelOrderWithContext cancels an order and returns the exchange used
func (c *UnifiedClient) CancelOrderWithContext(ctx context.Context, symbol string, orderID string) (string, error) {
	_, name, err := executeWithRetry(c, ctx, "CancelOrder", func(exchange Exchange) (struct{}, error) {
		return struct{}{}, exchange.CancelOrder(symbol, orderID)
	})
	return name, err
}

// CancelAllOrders cancels every order for a symbol
func (c *UnifiedClient) CancelAllOrders(symbol string) error {
	_, err := c.CancelAllOrdersWithContext(context.Background(), symbol)
	return err
}

// CancelAllOrdersWithContext cancels all orders for a symbol with metadata
func (c *UnifiedClient) CancelAllOrdersWithContext(ctx context.Context, symbol string) (string, error) {
	_, name, err := executeWithRetry(c, ctx, "CancelAllOrders", func(exchange Exchange) (struct{}, error) {
		return struct{}{}, exchange.CancelAllOrders(symbol)
	})
	return name, err
}

// GetOrderStatus retrieves the status of an order
func (c *UnifiedClient) GetOrderStatus(ctx context.Context, orderID string) (OrderStatus, string, error) {
	return executeWithRetry(c, ctx, "GetOrderStatus", func(exchange Exchange) (OrderStatus, error) {
		return exchange.GetOrderStatus(orderID)
	})
}

// IsHealthy reports whether any registered exchange is available
func (c *UnifiedClient) IsHealthy() bool {
	if c == nil || c.fallbackManager == nil {
		return false
	}
	statuses := c.fallbackManager.GetAllExchangeStatuses()
	for _, info := range statuses {
		if info.Status != ExchangeStatusDown {
			return true
		}
	}
	return false
}

// FetchMarketDataWithContext fetches normalized market data with caching
func (c *UnifiedClient) FetchMarketDataWithContext(ctx context.Context, symbol string) (models.MarketData, string, error) {
	cacheKey := c.cacheKey("marketdata:", symbol)
	if value, ok := c.getCached(cacheKey); ok {
		if md, ok := value.(models.MarketData); ok {
			return md, c.GetName(), nil
		}
	}

	result, name, err := executeWithRetry(c, ctx, "FetchMarketData", func(exchange Exchange) (models.MarketData, error) {
		return exchange.FetchMarketData(symbol)
	})
	if err != nil {
		return models.MarketData{}, name, err
	}

	c.setCache(cacheKey, result, c.cacheConfig.MarketDataTTL)
	return result, name, nil
}

// GetMarketSnapshot returns a simplified ticker-style snapshot
func (c *UnifiedClient) GetMarketSnapshot(ctx context.Context, symbol string) (*MarketSnapshot, string, error) {
	ticker, name, err := c.GetTickerWithContext(ctx, symbol)
	if err != nil {
		return nil, name, err
	}

	snapshot := &MarketSnapshot{
		Symbol:    ticker.Symbol,
		Bid:       ticker.Bid,
		Ask:       ticker.Ask,
		Price:     ticker.LastPrice,
		Volume:    ticker.Volume,
		Timestamp: ticker.Timestamp,
		Exchange:  name,
	}
	return snapshot, name, nil
}

// --- Mock implementation -------------------------------------------------

// MockClient is a lightweight exchange implementation for tests and factories
type MockClient struct {
	name      string
	isHealthy bool
	mu        sync.RWMutex
	orders    map[string]Order
	balances  map[string]*Balance
}

// NewMockClient creates a new mock exchange client
func NewMockClient(name string) *MockClient {
	return &MockClient{
		name:      name,
		isHealthy: true,
		orders:    make(map[string]Order),
		balances: map[string]*Balance{
			"USDT": {Asset: "USDT", Free: "1000", Locked: "0"},
		},
	}
}

// SetHealth toggles the reported health state
func (c *MockClient) SetHealth(healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isHealthy = healthy
}

// GetName returns the mock name
func (c *MockClient) GetName() string { return c.name }

// IsHealthy reports health flag
func (c *MockClient) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isHealthy
}

// FetchMarketData returns synthetic data
func (c *MockClient) FetchMarketData(symbol string) (models.MarketData, error) {
	return models.MarketData{
		Symbol:    symbol,
		Timestamp: time.Now(),
		Open:      100,
		High:      105,
		Low:       95,
		Close:     102,
		Volume:    250,
	}, nil
}

// GetTradingPairs returns two mock pairs
func (c *MockClient) GetTradingPairs() ([]TradingPair, error) {
	return []TradingPair{
		{Symbol: "BTC/USDT", BaseAsset: "BTC", QuoteAsset: "USDT"},
		{Symbol: "ETH/USDT", BaseAsset: "ETH", QuoteAsset: "USDT"},
	}, nil
}

// GetTicker returns deterministic ticker data
func (c *MockClient) GetTicker(symbol string) (*models.Ticker, error) {
	return &models.Ticker{Symbol: symbol, Bid: 100, Ask: 101, LastPrice: 100.5, Volume: 10, Timestamp: time.Now()}, nil
}

// GetOrderBook returns a simple three-level book
func (c *MockClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	book := &models.OrderBook{Symbol: symbol, Timestamp: time.Now()}
	for i := 0; i < depth; i++ {
		book.Bids = append(book.Bids, models.OrderBookEntry{Price: 100 - float64(i), Amount: float64(i + 1)})
		book.Asks = append(book.Asks, models.OrderBookEntry{Price: 101 + float64(i), Amount: float64(i + 1)})
	}
	return book, nil
}

// GetCandles returns stubbed candles
func (c *MockClient) GetCandles(symbol string, interval string, since time.Time, limit int) ([]models.Candle, error) {
	candles := make([]models.Candle, limit)
	for i := 0; i < limit; i++ {
		candles[i] = models.Candle{Symbol: symbol, OpenTime: since.Add(time.Duration(i) * time.Minute), Open: 100, High: 105, Low: 95, Close: 100.5, Volume: 5}
	}
	return candles, nil
}

// GetTrades returns stubbed trades
func (c *MockClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	trades := make([]models.Trade, limit)
	for i := range trades {
		trades[i] = models.Trade{Symbol: symbol, Price: 100.5, Quantity: 0.1, ExecutionTime: since.Add(time.Duration(i) * time.Second)}
	}
	return trades, nil
}

// GetBalance returns a balance entry if present
func (c *MockClient) GetBalance(asset string) (*Balance, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	bal, ok := c.balances[asset]
	if !ok {
		return nil, fmt.Errorf("balance for %s not found", asset)
	}
	copy := *bal
	return &copy, nil
}

// GetBalances returns a copy of balances map
func (c *MockClient) GetBalances() (map[string]*Balance, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copy := make(map[string]*Balance, len(c.balances))
	for k, v := range c.balances {
		val := *v
		copy[k] = &val
	}
	return copy, nil
}

// CreateOrder records the order and returns it
func (c *MockClient) CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error) {
	order := Order{
		ID:        fmt.Sprintf("order-%d", time.Now().UnixNano()),
		Symbol:    symbol,
		Side:      side,
		Type:      orderType,
		Amount:    amount,
		Price:     price,
		Status:    OrderStatusNew,
		CreatedAt: time.Now(),
	}
	c.mu.Lock()
	c.orders[order.ID] = order
	c.mu.Unlock()
	return &order, nil
}

// PlaceOrder accepts both Order structs and generic payloads
func (c *MockClient) PlaceOrder(order interface{}) (string, error) {
	switch v := order.(type) {
	case Order:
		res, err := c.CreateOrder(v.Symbol, v.Side, v.Type, v.Amount, v.Price)
		if err != nil {
			return "", err
		}
		return res.ID, nil
	case *Order:
		if v == nil {
			return "", errors.New("order payload is nil")
		}
		res, err := c.CreateOrder(v.Symbol, v.Side, v.Type, v.Amount, v.Price)
		if err != nil {
			return "", err
		}
		return res.ID, nil
	default:
		return "", errors.New("unsupported order payload type")
	}
}

// GetOrder returns an order by ID if known
func (c *MockClient) GetOrder(symbol string, orderID string) (*Order, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	order, ok := c.orders[orderID]
	if !ok {
		return nil, fmt.Errorf("order %s not found", orderID)
	}
	copy := order
	return &copy, nil
}

// GetOrders returns stored orders
func (c *MockClient) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	orders := make([]Order, 0, len(c.orders))
	for _, order := range c.orders {
		if order.Symbol == symbol {
			orders = append(orders, order)
		}
		if limit > 0 && len(orders) >= limit {
			break
		}
	}
	return orders, nil
}

// CancelOrder updates order status if tracked
func (c *MockClient) CancelOrder(symbol string, orderID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	order, ok := c.orders[orderID]
	if !ok {
		return fmt.Errorf("order %s not found", orderID)
	}
	order.Status = OrderStatusCancelled
	c.orders[orderID] = order
	return nil
}

// CancelAllOrders clears internal storage
func (c *MockClient) CancelAllOrders(symbol string) error {
	c.mu.Lock()
	for id, order := range c.orders {
		if order.Symbol == symbol {
			delete(c.orders, id)
		}
	}
	c.mu.Unlock()
	return nil
}

// GetOrderStatus returns the status of the order if tracked
func (c *MockClient) GetOrderStatus(orderID string) (OrderStatus, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if order, ok := c.orders[orderID]; ok {
		return order.Status, nil
	}
	return OrderStatusNew, fmt.Errorf("order %s not found", orderID)
}
