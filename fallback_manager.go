package exchange

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/golog"
)

// ExchangeStatus represents the current status of an exchange
type ExchangeStatus string

const (
	// ExchangeStatusUp indicates the exchange is operational
	ExchangeStatusUp ExchangeStatus = "UP"
	// ExchangeStatusDegraded indicates the exchange is operational but with issues
	ExchangeStatusDegraded ExchangeStatus = "DEGRADED"
	// ExchangeStatusDown indicates the exchange is not operational
	ExchangeStatusDown ExchangeStatus = "DOWN"
)

// ExchangeHealthInfo contains health information about an exchange
type ExchangeHealthInfo struct {
	Status           ExchangeStatus
	LastChecked      time.Time
	FailureCount     int
	ConsecutiveFails int
	LastError        error
	ResponseTime     time.Duration
	RateLimitInfo    map[string]interface{}
}

// FallbackConfig contains configuration for the fallback manager
type FallbackConfig struct {
	// HealthCheckInterval is how often to check exchange health
	HealthCheckInterval time.Duration
	// FailureThreshold is the number of consecutive failures before marking an exchange as down
	FailureThreshold int
	// RecoveryThreshold is the number of consecutive successes before marking a down exchange as up
	RecoveryThreshold int
	// CircuitBreakerTimeout is how long to wait before trying a down exchange again
	CircuitBreakerTimeout time.Duration
	// FallbackPriority is the order of exchanges to try when the primary is down
	FallbackPriority []string
	// EnableMetrics determines whether to collect metrics
	EnableMetrics bool
}

// DefaultFallbackConfig returns the default fallback configuration
func DefaultFallbackConfig() FallbackConfig {
	return FallbackConfig{
		HealthCheckInterval:   30 * time.Second,
		FailureThreshold:      3,
		RecoveryThreshold:     2,
		CircuitBreakerTimeout: 5 * time.Minute,
		FallbackPriority:      []string{},
		EnableMetrics:         true,
	}
}

// FallbackManager manages exchange fallbacks when exchanges are unavailable
// HealthChecker is an optional hook that allows external health systems to
// register checks for each exchange managed by the fallback layer.
type HealthChecker interface {
	RegisterCheck(name string, fn func(context.Context) error)
}

// FallbackManager manages exchange fallbacks when exchanges are unavailable
type FallbackManager struct {
	exchanges       map[string]Exchange
	healthInfo      map[string]*ExchangeHealthInfo
	config          FallbackConfig
	primaryExchange string
	mu              sync.RWMutex
	logger          *golog.Logger
	healthChecker   HealthChecker
	ctx             context.Context
	cancel          context.CancelFunc
}

const fallbackManagerComponent = "fallback_manager"

// NewFallbackManager creates a new fallback manager
func NewFallbackManager(config FallbackConfig, healthChecker HealthChecker) *FallbackManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &FallbackManager{
		exchanges:     make(map[string]Exchange),
		healthInfo:    make(map[string]*ExchangeHealthInfo),
		config:        config,
		logger:        defaultLogger(),
		healthChecker: healthChecker,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// RegisterExchange registers an exchange with the fallback manager
func (fm *FallbackManager) RegisterExchange(name string, exchange Exchange, isPrimary bool) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fm.exchanges[name] = exchange
	fm.healthInfo[name] = &ExchangeHealthInfo{
		Status:           ExchangeStatusUp,
		LastChecked:      time.Now(),
		FailureCount:     0,
		ConsecutiveFails: 0,
		RateLimitInfo:    make(map[string]interface{}),
	}

	if isPrimary {
		fm.primaryExchange = name
	}

	// Register health check for this exchange
	if fm.healthChecker != nil {
		fm.healthChecker.RegisterCheck(name, func(ctx context.Context) error {
			info := fm.GetExchangeHealth(name)
			if info.Status == ExchangeStatusDown {
				return errors.New("exchange is down")
			}
			return nil
		})
	}

	fm.logger.Info(
		fmt.Sprintf("Registered exchange %s (primary: %v)", name, isPrimary),
		golog.String("component", fallbackManagerComponent),
	)
}

// PrimaryExchange returns the currently configured primary exchange name
func (fm *FallbackManager) PrimaryExchange() string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.primaryExchange
}

// SetFallbackPriority sets the priority order for fallback exchanges
func (fm *FallbackManager) SetFallbackPriority(priority []string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Validate that all exchanges in the priority list are registered
	for _, name := range priority {
		if _, exists := fm.exchanges[name]; !exists {
			return fmt.Errorf("exchange %s in priority list is not registered", name)
		}
	}

	fm.config.FallbackPriority = priority
	return nil
}

// Start starts the fallback manager's health check routine
func (fm *FallbackManager) Start() {
	go fm.healthCheckLoop()
}

// Stop stops the fallback manager
func (fm *FallbackManager) Stop() {
	fm.cancel()
}

// healthCheckLoop periodically checks the health of all exchanges
func (fm *FallbackManager) healthCheckLoop() {
	ticker := time.NewTicker(fm.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fm.checkAllExchanges()
		case <-fm.ctx.Done():
			fm.logger.Info(
				"Stopping fallback manager health check loop",
				golog.String("component", fallbackManagerComponent),
			)
			return
		}
	}
}

// checkAllExchanges checks the health of all registered exchanges
func (fm *FallbackManager) checkAllExchanges() {
	fm.mu.RLock()
	exchangeNames := make([]string, 0, len(fm.exchanges))
	for name := range fm.exchanges {
		exchangeNames = append(exchangeNames, name)
	}
	fm.mu.RUnlock()

	for _, name := range exchangeNames {
		fm.checkExchangeHealth(name)
	}
}

// checkExchangeHealth checks the health of a specific exchange
func (fm *FallbackManager) checkExchangeHealth(name string) {
	fm.mu.RLock()
	exchange, exists := fm.exchanges[name]
	info := fm.healthInfo[name]
	fm.mu.RUnlock()

	if !exists || info == nil {
		return
	}

	startTime := time.Now()
	isHealthy := exchange.IsHealthy()
	responseTime := time.Since(startTime)

	fm.mu.Lock()
	defer fm.mu.Unlock()

	info.LastChecked = time.Now()
	info.ResponseTime = responseTime

	// Update health status based on the check result
	if isHealthy {
		// Exchange is healthy
		if info.Status == ExchangeStatusDown {
			info.ConsecutiveFails = 0
			// Only mark as UP if we've had enough consecutive successes
			if info.ConsecutiveFails == 0 {
				info.Status = ExchangeStatusUp
				fm.logger.Info(
					fmt.Sprintf("Exchange %s recovered and is now UP", name),
					golog.String("component", fallbackManagerComponent),
				)
			}
		} else {
			// Reset failure counters
			info.ConsecutiveFails = 0
			if info.Status == ExchangeStatusDegraded && responseTime < 500*time.Millisecond {
				info.Status = ExchangeStatusUp
				fm.logger.Info(
					fmt.Sprintf("Exchange %s performance improved and is now UP", name),
					golog.String("component", fallbackManagerComponent),
				)
			}
		}
	} else {
		// Exchange is unhealthy
		info.FailureCount++
		info.ConsecutiveFails++

		if info.Status == ExchangeStatusUp {
			if info.ConsecutiveFails >= fm.config.FailureThreshold {
				info.Status = ExchangeStatusDown
				fm.logger.Warn(
					fmt.Sprintf("Exchange %s is now DOWN after %d consecutive failures", name, info.ConsecutiveFails),
					golog.String("component", fallbackManagerComponent),
				)
			} else if responseTime > 1*time.Second {
				info.Status = ExchangeStatusDegraded
				fm.logger.Warn(
					fmt.Sprintf("Exchange %s is now DEGRADED due to high response time: %v", name, responseTime),
					golog.String("component", fallbackManagerComponent),
				)
			}
		}
	}
}

// GetExchangeHealth returns the health information for an exchange
func (fm *FallbackManager) GetExchangeHealth(name string) ExchangeHealthInfo {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	info, exists := fm.healthInfo[name]
	if !exists {
		return ExchangeHealthInfo{
			Status:      ExchangeStatusDown,
			LastChecked: time.Time{},
		}
	}

	// Return a copy to avoid race conditions
	return ExchangeHealthInfo{
		Status:           info.Status,
		LastChecked:      info.LastChecked,
		FailureCount:     info.FailureCount,
		ConsecutiveFails: info.ConsecutiveFails,
		LastError:        info.LastError,
		ResponseTime:     info.ResponseTime,
		RateLimitInfo:    info.RateLimitInfo,
	}
}

// GetHealthyExchange returns a healthy exchange based on fallback priority
func (fm *FallbackManager) GetHealthyExchange() (string, Exchange, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	// First check if primary exchange is healthy
	if primaryInfo, exists := fm.healthInfo[fm.primaryExchange]; exists && primaryInfo.Status == ExchangeStatusUp {
		return fm.primaryExchange, fm.exchanges[fm.primaryExchange], nil
	}

	// If primary is degraded but not down, still use it
	if primaryInfo, exists := fm.healthInfo[fm.primaryExchange]; exists && primaryInfo.Status == ExchangeStatusDegraded {
		return fm.primaryExchange, fm.exchanges[fm.primaryExchange], nil
	}

	// Try exchanges in priority order
	for _, name := range fm.config.FallbackPriority {
		if info, exists := fm.healthInfo[name]; exists && info.Status != ExchangeStatusDown {
			return name, fm.exchanges[name], nil
		}
	}

	// If no priority exchanges are healthy, try any healthy exchange
	for name, info := range fm.healthInfo {
		if info.Status != ExchangeStatusDown {
			return name, fm.exchanges[name], nil
		}
	}

	return "", nil, errors.New("no healthy exchanges available")
}

// RecordExchangeError records an error for an exchange
func (fm *FallbackManager) RecordExchangeError(name string, err error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	info, exists := fm.healthInfo[name]
	if !exists {
		return
	}

	info.FailureCount++
	info.ConsecutiveFails++
	info.LastError = err

	// Update status if needed
	if info.ConsecutiveFails >= fm.config.FailureThreshold {
		wasDown := info.Status == ExchangeStatusDown
		info.Status = ExchangeStatusDown

		if !wasDown {
			fm.logger.Warn(
				fmt.Sprintf("Exchange %s is now DOWN after %d consecutive failures. Last error: %v", name, info.ConsecutiveFails, err),
				golog.String("component", fallbackManagerComponent),
			)
		}
	}
}

// RecordExchangeSuccess records a successful operation for an exchange
func (fm *FallbackManager) RecordExchangeSuccess(name string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	info, exists := fm.healthInfo[name]
	if !exists {
		return
	}

	// Reset consecutive failures
	info.ConsecutiveFails = 0

	// If exchange was down, check if it should be marked as up
	if info.Status == ExchangeStatusDown {
		info.Status = ExchangeStatusDegraded
		fm.logger.Info(
			fmt.Sprintf("Exchange %s status improved to DEGRADED after successful operation", name),
			golog.String("component", fallbackManagerComponent),
		)
	}
}

// ExecuteWithFallback executes a function with fallback to other exchanges if the primary fails
func ExecuteWithFallback[T any](fm *FallbackManager, operation string, fn func(Exchange) (T, error)) (T, string, error) {
	var zero T

	primaryName, primaryExchange, err := fm.GetHealthyExchange()
	if err == nil {
		result, execErr := fn(primaryExchange)
		if execErr == nil {
			fm.RecordExchangeSuccess(primaryName)
			return result, primaryName, nil
		}

		fm.RecordExchangeError(primaryName, execErr)
		fm.logger.Warn(
			fmt.Sprintf("Operation %s failed on primary exchange %s: %v", operation, primaryName, execErr),
			golog.String("component", fallbackManagerComponent),
		)
	} else {
		// if no healthy exchange, we'll still iterate over priority list below
		primaryName = ""
	}

	fm.mu.RLock()
	priorityList := make([]string, len(fm.config.FallbackPriority))
	copy(priorityList, fm.config.FallbackPriority)
	fm.mu.RUnlock()

	for _, name := range priorityList {
		if name == primaryName {
			continue
		}

		info := fm.GetExchangeHealth(name)
		if info.Status == ExchangeStatusDown {
			continue
		}

		fm.mu.RLock()
		exchange, exists := fm.exchanges[name]
		fm.mu.RUnlock()
		if !exists {
			continue
		}

		fm.logger.Info(
			fmt.Sprintf("Trying fallback exchange %s for operation %s", name, operation),
			golog.String("component", fallbackManagerComponent),
		)
		result, execErr := fn(exchange)
		if execErr == nil {
			fm.RecordExchangeSuccess(name)
			fm.logger.Info(
				fmt.Sprintf("Operation %s succeeded on fallback exchange %s", operation, name),
				golog.String("component", fallbackManagerComponent),
			)
			return result, name, nil
		}

		fm.RecordExchangeError(name, execErr)
		fm.logger.Warn(
			fmt.Sprintf("Operation %s failed on fallback exchange %s: %v", operation, name, execErr),
			golog.String("component", fallbackManagerComponent),
		)
	}

	return zero, "", fmt.Errorf("operation %s failed on all available exchanges", operation)
}

// GetAllExchangeStatuses returns the status of all registered exchanges
func (fm *FallbackManager) GetAllExchangeStatuses() map[string]ExchangeHealthInfo {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	result := make(map[string]ExchangeHealthInfo)
	for name, info := range fm.healthInfo {
		result[name] = ExchangeHealthInfo{
			Status:           info.Status,
			LastChecked:      info.LastChecked,
			FailureCount:     info.FailureCount,
			ConsecutiveFails: info.ConsecutiveFails,
			ResponseTime:     info.ResponseTime,
			RateLimitInfo:    info.RateLimitInfo,
		}
	}

	return result
}

// Exchange is an interface that all exchange implementations must satisfy
type Exchange interface {
	GetName() string
	IsHealthy() bool
	FetchMarketData(symbol string) (models.MarketData, error)
	GetTradingPairs() ([]TradingPair, error)
	GetTicker(symbol string) (*models.Ticker, error)
	GetOrderBook(symbol string, depth int) (*models.OrderBook, error)
	GetCandles(symbol string, timeframe string, since time.Time, limit int) ([]models.Candle, error)
	GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error)
	GetBalance(asset string) (*Balance, error)
	GetBalances() (map[string]*Balance, error)
	CreateOrder(symbol string, side OrderSide, orderType OrderType, amount float64, price float64) (*Order, error)
	PlaceOrder(order interface{}) (string, error)
	GetOrder(symbol string, orderID string) (*Order, error)
	GetOrders(symbol string, since time.Time, limit int) ([]Order, error)
	CancelOrder(symbol string, orderID string) error
	CancelAllOrders(symbol string) error
	GetOrderStatus(orderID string) (OrderStatus, error)
}
