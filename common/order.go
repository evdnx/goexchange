package common

import (
	"time"
)

// TimeInForce represents the time in force of an order
type TimeInForce string

const (
	// TimeInForceGTC represents Good Till Canceled
	TimeInForceGTC TimeInForce = "GTC"
	// TimeInForceIOC represents Immediate Or Cancel
	TimeInForceIOC TimeInForce = "IOC"
	// TimeInForceFOK represents Fill Or Kill
	TimeInForceFOK TimeInForce = "FOK"
)

// OrderSide represents the side of an order (buy or sell)
type OrderSide string

const (
	// OrderSideBuy represents a buy order
	OrderSideBuy OrderSide = "buy"
	// OrderSideSell represents a sell order
	OrderSideSell OrderSide = "sell"
)

// String returns the string representation of OrderSide
func (s OrderSide) String() string {
	return string(s)
}

// OrderSideFromString converts a string to OrderSide
func OrderSideFromString(s string) OrderSide {
	return OrderSide(s)
}

// OrderType represents the type of an order
type OrderType string

const (
	// OrderTypeMarket represents a market order
	OrderTypeMarket OrderType = "market"
	// OrderTypeLimit represents a limit order
	OrderTypeLimit OrderType = "limit"
	// OrderTypeStopLoss represents a stop loss order
	OrderTypeStopLoss OrderType = "stop_loss"
	// OrderTypeStopLimit represents a stop limit order
	OrderTypeStopLimit OrderType = "stop_limit"
)

// String returns the string representation of OrderType
func (t OrderType) String() string {
	return string(t)
}

// OrderTypeFromString converts a string to OrderType
func OrderTypeFromString(s string) OrderType {
	return OrderType(s)
}

// OrderStatus represents the status of an order
type OrderStatus string

const (
	// OrderStatusNew represents a new order
	OrderStatusNew OrderStatus = "new"
	// OrderStatusPartiallyFilled represents a partially filled order
	OrderStatusPartiallyFilled OrderStatus = "partially_filled"
	// OrderStatusFilled represents a filled order
	OrderStatusFilled OrderStatus = "filled"
	// OrderStatusCancelled represents a cancelled order
	OrderStatusCancelled OrderStatus = "cancelled"
	// OrderStatusRejected represents a rejected order
	OrderStatusRejected OrderStatus = "rejected"
	// OrderStatusExpired represents an expired order
	OrderStatusExpired OrderStatus = "expired"
)

// String returns the string representation of OrderStatus
func (s OrderStatus) String() string {
	return string(s)
}

// OrderStatusFromString converts a string to OrderStatus
func OrderStatusFromString(s string) OrderStatus {
	return OrderStatus(s)
}

// Order represents an order on an exchange
type Order struct {
	ID              string      `json:"id"`
	ClientOrderID   string      `json:"client_order_id"`
	Symbol          string      `json:"symbol"`
	Side            OrderSide   `json:"side"`
	Type            OrderType   `json:"type"`
	Status          OrderStatus `json:"status"`
	Price           float64     `json:"price"`
	Amount          float64     `json:"amount"`
	FilledAmount    float64     `json:"filled_amount"`
	RemainingAmount float64     `json:"remaining_amount"`
	Fee             float64     `json:"fee"`
	FeeCurrency     string      `json:"fee_currency"`
	CreatedAt       time.Time   `json:"created_at"`
	UpdatedAt       time.Time   `json:"updated_at"`
	// Adding Quantity for backward compatibility
	Quantity  float64   `json:"quantity"`
	Timestamp time.Time `json:"timestamp"`
}

// Balance represents an asset balance
type Balance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

// TradingPair represents a trading pair
type TradingPair struct {
	Symbol     string
	BaseAsset  string
	QuoteAsset string
}
