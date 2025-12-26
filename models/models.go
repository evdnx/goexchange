package models

import "time"

// Candle represents OHLCV candle data.
type Candle struct {
	Exchange  string
	Symbol    string
	Interval  string
	OpenTime  time.Time
	CloseTime time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

// MarketData represents market data for a trading pair, supporting both candlestick and ticker data.
type MarketData struct {
	ID        *string   `json:"id,omitempty"`
	Symbol    string    `json:"symbol"`
	Exchange  *string   `json:"exchange,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
	Interval  *string   `json:"interval,omitempty"`
	Price     *float64  `json:"price,omitempty"`
	Bid       *float64  `json:"bid,omitempty"`
	Ask       *float64  `json:"ask,omitempty"`
}

// Symbol describes an exchange trading symbol and its status.
type Symbol struct {
	Symbol     string `json:"symbol"`
	Status     string `json:"status"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
}

// Trade represents a trade event.
type Trade struct {
	ID            string
	Symbol        string
	Exchange      string
	Strategy      string
	Type          string
	Quantity      float64
	Price         float64
	Fee           float64
	Total         float64
	ExecutionTime time.Time
	OrderID       string
}

// Ticker represents a ticker update from an exchange.
type Ticker struct {
	Exchange  string
	Symbol    string
	LastPrice float64
	Timestamp time.Time
	Volume    float64
	Bid       float64
	Ask       float64
	// OHLC fields (optional, populated when available from exchange)
	Open  float64
	High  float64
	Low   float64
	Close float64
}

// UserData represents user-specific events, e.g. account updates.
type UserData struct {
	Exchange  string
	EventType string                 // e.g., "outboundAccountPosition", "executionReport"
	Data      map[string]interface{} // Flexible field for various user data types
}

// OrderBook represents the state of bids and asks for a symbol.
type OrderBook struct {
	Exchange  string
	Symbol    string
	Bids      []OrderBookEntry
	Asks      []OrderBookEntry
	Timestamp time.Time
}

// OrderBookEntry represents an entry in an order book.
type OrderBookEntry struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}
