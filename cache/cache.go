package cache

import (
	"math"
	"sync"
	"time"
)

// Item represents a cached item with expiration
type Item struct {
	Value      interface{}
	Expiration int64
}

// Cache is a simple in-memory cache with expiration
type Cache struct {
	items           map[string]Item
	mu              sync.RWMutex
	maxSize         int
	cleanupInterval time.Duration
	stopJanitor     chan bool
}

// New creates a new cache with the given configuration
func New(config Config) *Cache {
	cache := &Cache{
		items:           make(map[string]Item),
		maxSize:         config.MaxCacheSize,
		cleanupInterval: config.CleanupInterval,
		stopJanitor:     make(chan bool),
	}

	// Start the janitor to clean expired items
	go cache.janitor()

	return cache
}

// Set adds an item to the cache with the given expiration duration
func (c *Cache) Set(key string, value interface{}, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration int64
	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.items[key] = Item{
		Value:      value,
		Expiration: expiration,
	}

	// If we've exceeded the max size, remove the oldest item
	if c.maxSize > 0 && len(c.items) > c.maxSize {
		c.removeOldest()
	}
}

// removeOldest removes the oldest item from the cache
func (c *Cache) removeOldest() {
	var oldestKey string
	var oldestTime int64 = math.MaxInt64

	// Find the oldest item
	for key, item := range c.items {
		if item.Expiration > 0 && item.Expiration < oldestTime {
			oldestKey = key
			oldestTime = item.Expiration
		}
	}

	// If we found an item, delete it
	if oldestKey != "" {
		delete(c.items, oldestKey)
	}
}

// Get retrieves an item from the cache
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found {
		return nil, false
	}

	// Check if the item has expired
	if item.Expiration > 0 && time.Now().UnixNano() > item.Expiration {
		return nil, false
	}

	return item.Value, true
}

// Delete removes an item from the cache
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

// Clear removes all items from the cache
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]Item)
}

// janitor cleans up expired items from the cache
func (c *Cache) janitor() {
	interval := c.cleanupInterval
	if interval <= 0 {
		interval = time.Minute // Default to 1 minute if not set
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stopJanitor:
			return
		}
	}
}

// Stop stops the janitor goroutine
func (c *Cache) Stop() {
	close(c.stopJanitor)
}

// deleteExpired deletes expired items from the cache
func (c *Cache) deleteExpired() {
	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if item.Expiration > 0 && now > item.Expiration {
			delete(c.items, key)
		}
	}
}
