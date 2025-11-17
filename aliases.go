package goexchange

import (
	"time"

	"github.com/evdnx/goexchange/exchange"
)

type (
	// Re-export domain types so existing consumers can continue using goexchange.Order, etc.
	TimeInForce    = exchange.TimeInForce
	OrderSide      = exchange.OrderSide
	OrderType      = exchange.OrderType
	OrderStatus    = exchange.OrderStatus
	Order          = exchange.Order
	Balance        = exchange.Balance
	TradingPair    = exchange.TradingPair
	ErrorType      = exchange.ErrorType
	ExchangeError  = exchange.ExchangeError
	ExchangeClient = exchange.ExchangeClient
	BaseClient     = exchange.BaseClient
)

const (
	TimeInForceGTC = exchange.TimeInForceGTC
	TimeInForceIOC = exchange.TimeInForceIOC
	TimeInForceFOK = exchange.TimeInForceFOK

	OrderSideBuy  = exchange.OrderSideBuy
	OrderSideSell = exchange.OrderSideSell

	OrderTypeMarket    = exchange.OrderTypeMarket
	OrderTypeLimit     = exchange.OrderTypeLimit
	OrderTypeStopLoss  = exchange.OrderTypeStopLoss
	OrderTypeStopLimit = exchange.OrderTypeStopLimit

	OrderStatusNew             = exchange.OrderStatusNew
	OrderStatusPartiallyFilled = exchange.OrderStatusPartiallyFilled
	OrderStatusFilled          = exchange.OrderStatusFilled
	OrderStatusCancelled       = exchange.OrderStatusCancelled
	OrderStatusRejected        = exchange.OrderStatusRejected
	OrderStatusExpired         = exchange.OrderStatusExpired

	ErrorTypeHTTP           = exchange.ErrorTypeHTTP
	ErrorTypeNetwork        = exchange.ErrorTypeNetwork
	ErrorTypeRateLimit      = exchange.ErrorTypeRateLimit
	ErrorTypeAuthentication = exchange.ErrorTypeAuthentication
	ErrorTypeParsing        = exchange.ErrorTypeParsing
	ErrorTypeValidation     = exchange.ErrorTypeValidation
	ErrorTypeExchange       = exchange.ErrorTypeExchange
	ErrorTypeSystem         = exchange.ErrorTypeSystem
	ErrorTypeUnknown        = exchange.ErrorTypeUnknown
)

var (
	ErrNotImplemented = exchange.ErrNotImplemented
)

func NewBaseClient(name, apiKey, apiSecret string, testnet bool) *BaseClient {
	return exchange.NewBaseClient(name, apiKey, apiSecret, testnet)
}

func NewExchangeError(errType ErrorType, code string, message string, cause error) *ExchangeError {
	return exchange.NewExchangeError(errType, code, message, cause)
}

func NewNetworkError(code string, message string, cause error, retriable bool) *ExchangeError {
	return exchange.NewNetworkError(code, message, cause, retriable)
}

func NewExchangeHTTPError(statusCode int, body []byte, message string) *ExchangeError {
	return exchange.NewExchangeHTTPError(statusCode, body, message)
}

func NewParsingError(message string, cause error, rawData []byte) *ExchangeError {
	return exchange.NewParsingError(message, cause, rawData)
}

func NewValidationError(code string, message string) *ExchangeError {
	return exchange.NewValidationError(code, message)
}

func NewRateLimitError(message string, retryAfter time.Duration) *ExchangeError {
	return exchange.NewRateLimitError(message, retryAfter)
}

func NewAuthenticationError(message string) *ExchangeError {
	return exchange.NewAuthenticationError(message)
}

func IsNetworkError(err error) bool {
	return exchange.IsNetworkError(err)
}

func IsHTTPError(err error) bool {
	return exchange.IsHTTPError(err)
}

func IsRateLimitError(err error) bool {
	return exchange.IsRateLimitError(err)
}

func IsAuthenticationError(err error) bool {
	return exchange.IsAuthenticationError(err)
}

func IsParsingError(err error) bool {
	return exchange.IsParsingError(err)
}

func IsValidationError(err error) bool {
	return exchange.IsValidationError(err)
}

func IsRetriable(err error) bool {
	return exchange.IsRetriable(err)
}
