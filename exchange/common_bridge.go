package exchange

import (
	"time"

	common "github.com/evdnx/goexchange/exchange/common"
	"github.com/evdnx/gohttpcl"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

// Re-export shared types so existing consumers of the exchange package keep working.
type (
	ExchangeClient = common.ExchangeClient
	BaseClient     = common.BaseClient
	Balance        = common.Balance
	TradingPair    = common.TradingPair
	Order          = common.Order
	OrderSide      = common.OrderSide
	OrderType      = common.OrderType
	OrderStatus    = common.OrderStatus
	TimeInForce    = common.TimeInForce
	ErrorType      = common.ErrorType
	ExchangeError  = common.ExchangeError
)

// Re-export shared constants.
const (
	TimeInForceGTC TimeInForce = common.TimeInForceGTC
	TimeInForceIOC TimeInForce = common.TimeInForceIOC
	TimeInForceFOK TimeInForce = common.TimeInForceFOK

	OrderSideBuy  OrderSide = common.OrderSideBuy
	OrderSideSell OrderSide = common.OrderSideSell

	OrderTypeMarket    OrderType = common.OrderTypeMarket
	OrderTypeLimit     OrderType = common.OrderTypeLimit
	OrderTypeStopLoss  OrderType = common.OrderTypeStopLoss
	OrderTypeStopLimit OrderType = common.OrderTypeStopLimit

	OrderStatusNew             OrderStatus = common.OrderStatusNew
	OrderStatusPartiallyFilled OrderStatus = common.OrderStatusPartiallyFilled
	OrderStatusFilled          OrderStatus = common.OrderStatusFilled
	OrderStatusCancelled       OrderStatus = common.OrderStatusCancelled
	OrderStatusRejected        OrderStatus = common.OrderStatusRejected
	OrderStatusExpired         OrderStatus = common.OrderStatusExpired

	ErrorTypeHTTP           ErrorType = common.ErrorTypeHTTP
	ErrorTypeNetwork        ErrorType = common.ErrorTypeNetwork
	ErrorTypeRateLimit      ErrorType = common.ErrorTypeRateLimit
	ErrorTypeAuthentication ErrorType = common.ErrorTypeAuthentication
	ErrorTypeParsing        ErrorType = common.ErrorTypeParsing
	ErrorTypeValidation     ErrorType = common.ErrorTypeValidation
	ErrorTypeExchange       ErrorType = common.ErrorTypeExchange
	ErrorTypeSystem         ErrorType = common.ErrorTypeSystem
	ErrorTypeUnknown        ErrorType = common.ErrorTypeUnknown
)

// Re-export common package functions and variables.
var (
	ErrNotImplemented = common.ErrNotImplemented
)

func NewBaseClient(name, apiKey, apiSecret string, testnet bool) *BaseClient {
	return common.NewBaseClient(name, apiKey, apiSecret, testnet)
}

func NewExchangeError(errType ErrorType, code string, message string, cause error) *ExchangeError {
	return common.NewExchangeError(errType, code, message, cause)
}

func NewNetworkError(code string, message string, cause error, retriable bool) *ExchangeError {
	return common.NewNetworkError(code, message, cause, retriable)
}

func NewExchangeHTTPError(statusCode int, body []byte, message string) *ExchangeError {
	return common.NewExchangeHTTPError(statusCode, body, message)
}

func NewParsingError(message string, cause error, rawData []byte) *ExchangeError {
	return common.NewParsingError(message, cause, rawData)
}

func NewValidationError(code string, message string) *ExchangeError {
	return common.NewValidationError(code, message)
}

func NewRateLimitError(message string, retryAfter time.Duration) *ExchangeError {
	return common.NewRateLimitError(message, retryAfter)
}

func NewAuthenticationError(message string) *ExchangeError {
	return common.NewAuthenticationError(message)
}

func IsNetworkError(err error) bool        { return common.IsNetworkError(err) }
func IsHTTPError(err error) bool           { return common.IsHTTPError(err) }
func IsRateLimitError(err error) bool      { return common.IsRateLimitError(err) }
func IsAuthenticationError(err error) bool { return common.IsAuthenticationError(err) }
func IsParsingError(err error) bool        { return common.IsParsingError(err) }
func IsValidationError(err error) bool     { return common.IsValidationError(err) }
func IsRetriable(err error) bool           { return common.IsRetriable(err) }

func OrderSideFromString(s string) OrderSide { return common.OrderSideFromString(s) }
func OrderTypeFromString(s string) OrderType { return common.OrderTypeFromString(s) }
func OrderStatusFromString(s string) OrderStatus {
	return common.OrderStatusFromString(s)
}

func defaultLogger() *golog.Logger {
	return common.DefaultLogger()
}

func newHTTPMetricsCollector(m *metrics.Metrics, service string) gohttpcl.MetricsCollector {
	return common.NewHTTPMetricsCollector(m, service)
}
