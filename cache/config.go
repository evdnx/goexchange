package cache

import (
	"time"
)

// Config represents cache configuration settings
type Config struct {
	// Enabled determines if caching is enabled
	Enabled bool

	// DefaultTTL is the default time-to-live for cached items
	DefaultTTL time.Duration

	// MarketDataTTL is the time-to-live for market data
	MarketDataTTL time.Duration

	// HistoricalDataTTL is the time-to-live for historical data
	HistoricalDataTTL time.Duration

	// MaxCacheSize is the maximum number of items in the cache (0 = unlimited)
	MaxCacheSize int

	// CleanupInterval is the interval at which expired items are cleaned up
	CleanupInterval time.Duration
}

// DefaultConfig returns the default cache configuration
func DefaultConfig() Config {
	return Config{
		Enabled:           true,
		DefaultTTL:        time.Hour,
		MarketDataTTL:     5 * time.Minute, // Recent market data expires quickly
		HistoricalDataTTL: 24 * time.Hour,  // Historical data can be cached longer
		MaxCacheSize:      10000,           // Limit to 10,000 items
		CleanupInterval:   time.Minute,
	}
}
