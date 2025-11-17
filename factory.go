package exchange

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/evdnx/goexchange/cache"
	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/golog"
	"github.com/evdnx/gowscl"
)

// ExchangeType represents a type of cryptocurrency exchange
type ExchangeType string

const (
	// ExchangeBinance represents Binance exchange
	ExchangeBinance ExchangeType = "binance"
	// ExchangeCoinbase represents Coinbase exchange
	ExchangeCoinbase ExchangeType = "coinbase"
)

// ExchangeConfig represents configuration for an exchange
type ExchangeConfig struct {
	Type      ExchangeType
	APIKey    string
	APISecret string
	Testnet   bool
}

// ExchangeFactory centralizes creation and registration of exchanges
// so the rest of the codebase has a single entry point.
type ExchangeFactory struct {
	configs map[ExchangeType]ExchangeConfig
	mu      sync.RWMutex
}

// NewExchangeFactory creates a new exchange factory
func NewExchangeFactory() *ExchangeFactory {
	return &ExchangeFactory{
		configs: make(map[ExchangeType]ExchangeConfig),
	}
}

// RegisterExchange registers an exchange with the factory
func (f *ExchangeFactory) RegisterExchange(config ExchangeConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configs[config.Type] = config
}

// CreateExchangeClient creates an exchange client based on the configuration
func (f *ExchangeFactory) CreateExchangeClient(config ExchangeConfig) (Client, error) {
	switch config.Type {
	case ExchangeBinance:
		return NewBinanceClient(config.APIKey, config.APISecret, config.Testnet, nil), nil
	case ExchangeCoinbase:
		return NewCoinbaseClient(config.APIKey, config.APISecret, "", config.Testnet, nil), nil
	default:
		return nil, fmt.Errorf("unsupported exchange type: %s", config.Type)
	}
}

// GetAllExchangeClients returns clients for all registered exchanges
func (f *ExchangeFactory) GetAllExchangeClients() (map[ExchangeType]Client, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	clients := make(map[ExchangeType]Client)
	for exchangeType, config := range f.configs {
		client, err := f.CreateExchangeClient(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create client for %s: %w", exchangeType, err)
		}
		clients[exchangeType] = client
	}
	return clients, nil
}

// GetExchangeCapabilities returns the capabilities for a specific exchange type
func GetExchangeCapabilities(exchangeType ExchangeType) []ExchangeCapability {
	switch exchangeType {
	case ExchangeBinance:
		return []ExchangeCapability{
			CapabilitySpotTrading,
			CapabilityMarginTrading,
			CapabilityFuturesTrading,
			CapabilityWebSocket,
			CapabilityHistoricalData,
		}
	case ExchangeCoinbase:
		return []ExchangeCapability{
			CapabilitySpotTrading,
			CapabilityWebSocket,
			CapabilityHistoricalData,
		}
	default:
		return []ExchangeCapability{}
	}
}

// CreateCapableClient wraps a client with capability metadata.
func (f *ExchangeFactory) CreateCapableClient(config ExchangeConfig) (CapableClient, error) {
	client, err := f.CreateExchangeClient(config)
	if err != nil {
		return nil, err
	}
	capabilities := GetExchangeCapabilities(config.Type)
	var wsClient *gowscl.Client
	return NewBaseCapableClient(client, capabilities, wsClient), nil
}

// GetAllCapableClients returns capable clients for all registered exchanges
func (f *ExchangeFactory) GetAllCapableClients() (map[ExchangeType]CapableClient, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	clients := make(map[ExchangeType]CapableClient)
	for exchangeType, config := range f.configs {
		client, err := f.CreateCapableClient(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create capable client for %s: %w", exchangeType, err)
		}
		clients[exchangeType] = client
	}
	return clients, nil
}

// --- Cache helpers ---------------------------------------------------------

// CreateCachedExchange wraps an exchange with caching capabilities
func CreateCachedExchange(exchange Exchange) Exchange {
	if exchange == nil {
		return nil
	}
	return NewCachedExchange(exchange)
}

// CreateCachedExchangeWithConfig wraps an exchange with caching capabilities using the provided config
func CreateCachedExchangeWithConfig(exchange Exchange, config cache.Config) Exchange {
	if exchange == nil {
		return nil
	}
	return NewCachedExchangeWithConfig(exchange, config)
}

// WrapAllExchangesWithCache wraps all exchanges in a slice with caching capabilities
func WrapAllExchangesWithCache(exchanges []Exchange) []Exchange {
	cachedExchanges := make([]Exchange, len(exchanges))
	for i, exchange := range exchanges {
		cachedExchanges[i] = CreateCachedExchange(exchange)
	}
	return cachedExchanges
}

// WrapAllExchangesWithCacheConfig wraps all exchanges in a slice with caching capabilities using the provided config
func WrapAllExchangesWithCacheConfig(exchanges []Exchange, config cache.Config) []Exchange {
	cachedExchanges := make([]Exchange, len(exchanges))
	for i, exchange := range exchanges {
		cachedExchanges[i] = CreateCachedExchangeWithConfig(exchange, config)
	}
	return cachedExchanges
}

// --- Resilient factory -----------------------------------------------------

// ResilientExchangeFactory creates and manages exchange clients with fallback capabilities
type ResilientExchangeFactory struct {
	fallbackManager *FallbackManager
	unifiedClient   *UnifiedClient
	logger          *golog.Logger
	healthChecker   HealthChecker
}

const exchangeFactoryComponent = "exchange_factory"

// NewResilientExchangeFactory creates a new resilient exchange factory
func NewResilientExchangeFactory(healthChecker HealthChecker) *ResilientExchangeFactory {
	logger := defaultLogger()
	fallbackConfig := DefaultFallbackConfig()
	fallbackManager := NewFallbackManager(fallbackConfig, healthChecker)
	unifiedClient := NewUnifiedClient(fallbackManager)

	return &ResilientExchangeFactory{
		fallbackManager: fallbackManager,
		unifiedClient:   unifiedClient,
		logger:          logger,
		healthChecker:   healthChecker,
	}
}

// RegisterExchange registers an exchange with the factory
func (f *ResilientExchangeFactory) RegisterExchange(name string, client interface{}, isPrimary bool) {
	wrapper := NewExchangeWrapper(name, client)
	f.fallbackManager.RegisterExchange(name, wrapper, isPrimary)
	f.logger.Info(
		"Registered exchange: "+name+" (primary: "+boolToString(isPrimary)+")",
		golog.String("component", exchangeFactoryComponent),
	)
}

// SetFallbackPriority sets the priority order for fallback exchanges
func (f *ResilientExchangeFactory) SetFallbackPriority(priority []string) error {
	return f.fallbackManager.SetFallbackPriority(priority)
}

// Start starts the exchange factory
func (f *ResilientExchangeFactory) Start() {
	f.fallbackManager.Start()
	f.logger.Info("Exchange factory started", golog.String("component", exchangeFactoryComponent))
}

// Stop stops the exchange factory
func (f *ResilientExchangeFactory) Stop() {
	f.fallbackManager.Stop()
	f.logger.Info("Exchange factory stopped", golog.String("component", exchangeFactoryComponent))
}

// GetFallbackManager returns the fallback manager
func (f *ResilientExchangeFactory) GetFallbackManager() *FallbackManager {
	return f.fallbackManager
}

// GetUnifiedClient returns the composite unified client
func (f *ResilientExchangeFactory) GetUnifiedClient() *UnifiedClient {
	return f.unifiedClient
}

// GetExchangeStatuses returns the status of all registered exchanges
func (f *ResilientExchangeFactory) GetExchangeStatuses() map[string]ExchangeHealthInfo {
	return f.fallbackManager.GetAllExchangeStatuses()
}

// FetchMarketData fetches market data with fallback and retry
func (f *ResilientExchangeFactory) FetchMarketData(ctx context.Context, symbol string) (models.MarketData, string, error) {
	return f.unifiedClient.FetchMarketDataWithContext(ctx, symbol)
}

// GetOrderBook gets the order book with fallback and retry
func (f *ResilientExchangeFactory) GetOrderBook(ctx context.Context, symbol string, depth int) (*models.OrderBook, string, error) {
	return f.unifiedClient.GetOrderBookWithContext(ctx, symbol, depth)
}

// GetBalance gets the balance with fallback and retry
func (f *ResilientExchangeFactory) GetBalance(ctx context.Context, asset string) (*Balance, string, error) {
	return f.unifiedClient.GetBalanceWithContext(ctx, asset)
}

// PlaceOrder places an order with fallback and retry
func (f *ResilientExchangeFactory) PlaceOrder(ctx context.Context, order interface{}) (string, string, error) {
	return f.unifiedClient.PlaceOrderWithContext(ctx, order)
}

// CancelOrder cancels an order with fallback and retry
func (f *ResilientExchangeFactory) CancelOrder(ctx context.Context, symbol string, orderID string) (string, error) {
	return f.unifiedClient.CancelOrderWithContext(ctx, symbol, orderID)
}

// GetOrderStatus gets the order status with fallback and retry
func (f *ResilientExchangeFactory) GetOrderStatus(ctx context.Context, orderID string) (OrderStatus, string, error) {
	return f.unifiedClient.GetOrderStatus(ctx, orderID)
}

// GetTicker gets the ticker with fallback and retry
func (f *ResilientExchangeFactory) GetTicker(ctx context.Context, symbol string) (*models.Ticker, string, error) {
	return f.unifiedClient.GetTickerWithContext(ctx, symbol)
}

// GetCandles gets candles with fallback and retry
func (f *ResilientExchangeFactory) GetCandles(ctx context.Context, symbol string, timeframe string, since time.Time, limit int) ([]models.Candle, string, error) {
	return f.unifiedClient.GetCandlesWithContext(ctx, symbol, timeframe, since, limit)
}

// GetTrades gets trades with fallback and retry
func (f *ResilientExchangeFactory) GetTrades(ctx context.Context, symbol string, since time.Time, limit int) ([]models.Trade, string, error) {
	return f.unifiedClient.GetTradesWithContext(ctx, symbol, since, limit)
}

// GetBalances gets all balances with fallback and retry
func (f *ResilientExchangeFactory) GetBalances(ctx context.Context) (map[string]*Balance, string, error) {
	return f.unifiedClient.GetBalancesWithContext(ctx)
}

// GetOrders gets orders with fallback and retry
func (f *ResilientExchangeFactory) GetOrders(ctx context.Context, symbol string, since time.Time, limit int) ([]Order, string, error) {
	return f.unifiedClient.GetOrdersWithContext(ctx, symbol, since, limit)
}

// CancelAllOrders cancels all orders with fallback and retry
func (f *ResilientExchangeFactory) CancelAllOrders(ctx context.Context, symbol string) (string, error) {
	return f.unifiedClient.CancelAllOrdersWithContext(ctx, symbol)
}

// --- Unified factory -------------------------------------------------------

// UnifiedExchangeFactory creates exchange implementations that satisfy the Exchange interface
type UnifiedExchangeFactory interface {
	CreateClient(exchangeType string, apiKey string, apiSecret string, testnet bool) (Exchange, error)
	GetSupportedExchanges() []string
}

// DefaultUnifiedExchangeFactory is the default implementation of UnifiedExchangeFactory
type DefaultUnifiedExchangeFactory struct{}

// NewDefaultUnifiedExchangeFactory creates a new default exchange factory
func NewDefaultUnifiedExchangeFactory() *DefaultUnifiedExchangeFactory {
	return &DefaultUnifiedExchangeFactory{}
}

// CreateClient creates a client for an exchange
func (f *DefaultUnifiedExchangeFactory) CreateClient(exchangeType string, apiKey string, apiSecret string, testnet bool) (Exchange, error) {
	switch exchangeType {
	case string(ExchangeBinance):
		return NewMockClient("Binance"), nil
	case string(ExchangeCoinbase):
		return NewMockClient("Coinbase"), nil
	default:
		return nil, fmt.Errorf("unsupported exchange type: %s", exchangeType)
	}
}

// GetSupportedExchanges returns the list of supported exchanges
func (f *DefaultUnifiedExchangeFactory) GetSupportedExchanges() []string {
	return []string{string(ExchangeBinance), string(ExchangeCoinbase)}
}

// MockUnifiedExchangeFactory is a mock implementation of UnifiedExchangeFactory for testing
type MockUnifiedExchangeFactory struct{}

// NewMockUnifiedExchangeFactory creates a new mock exchange factory
func NewMockUnifiedExchangeFactory() *MockUnifiedExchangeFactory {
	return &MockUnifiedExchangeFactory{}
}

// CreateClient creates a client for an exchange
func (f *MockUnifiedExchangeFactory) CreateClient(exchangeType string, apiKey string, apiSecret string, testnet bool) (Exchange, error) {
	return NewMockClient(exchangeType), nil
}

// GetSupportedExchanges returns the list of supported exchanges
func (f *MockUnifiedExchangeFactory) GetSupportedExchanges() []string {
	return []string{string(ExchangeBinance), string(ExchangeCoinbase)}
}

// Helper function to convert bool to string
func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
