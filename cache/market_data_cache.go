package cache

import (
	"fmt"
	"time"

	"github.com/evdnx/goexchange/models"
)

// MarketDataCache is a specialized cache for market data
type MarketDataCache struct {
	cache  *Cache
	config Config
}

// NewMarketDataCache creates a new market data cache with default configuration
func NewMarketDataCache() *MarketDataCache {
	config := DefaultConfig()
	return NewMarketDataCacheWithConfig(config)
}

// NewMarketDataCacheWithConfig creates a new market data cache with the given configuration
func NewMarketDataCacheWithConfig(config Config) *MarketDataCache {
	return &MarketDataCache{
		cache:  New(config),
		config: config,
	}
}

// CacheKey generates a cache key for market data
func (m *MarketDataCache) CacheKey(symbol string, timeframe string, timestamp time.Time) string {
	return fmt.Sprintf("%s:%s:%d", symbol, timeframe, timestamp.Unix())
}

// CacheKeyRange generates a cache key for a range of market data
func (m *MarketDataCache) CacheKeyRange(symbol string, timeframe string, start, end time.Time) string {
	return fmt.Sprintf("%s:%s:%d:%d", symbol, timeframe, start.Unix(), end.Unix())
}

// SetMarketData caches a single market data point
func (m *MarketDataCache) SetMarketData(data models.MarketData, ttl time.Duration) {
	if !m.config.Enabled {
		return
	}

	if ttl <= 0 {
		ttl = m.config.MarketDataTTL
	}

	key := m.CacheKey(data.Symbol, "1m", data.Timestamp)
	m.cache.Set(key, data, ttl)
}

// GetMarketData retrieves a single market data point from the cache
func (m *MarketDataCache) GetMarketData(symbol string, timeframe string, timestamp time.Time) (models.MarketData, bool) {
	key := m.CacheKey(symbol, timeframe, timestamp)
	if value, found := m.cache.Get(key); found {
		if data, ok := value.(models.MarketData); ok {
			return data, true
		}
	}
	return models.MarketData{}, false
}

// SetMarketDataRange caches a range of market data
func (m *MarketDataCache) SetMarketDataRange(symbol string, timeframe string, start, end time.Time, data []models.MarketData, ttl time.Duration) {
	if !m.config.Enabled {
		return
	}

	if ttl <= 0 {
		ttl = m.config.HistoricalDataTTL
	}

	key := m.CacheKeyRange(symbol, timeframe, start, end)
	m.cache.Set(key, data, ttl)
}

// GetMarketDataRange retrieves a range of market data from the cache
func (m *MarketDataCache) GetMarketDataRange(symbol string, timeframe string, start, end time.Time) ([]models.MarketData, bool) {
	key := m.CacheKeyRange(symbol, timeframe, start, end)
	if value, found := m.cache.Get(key); found {
		if data, ok := value.([]models.MarketData); ok {
			return data, true
		}
	}
	return nil, false
}

// Clear clears all cached market data
func (m *MarketDataCache) Clear() {
	m.cache.Clear()
}

// Stop stops the cache's background processes
func (m *MarketDataCache) Stop() {
	m.cache.Stop()
}

// IsEnabled returns whether caching is enabled
func (m *MarketDataCache) IsEnabled() bool {
	return m.config.Enabled
}
