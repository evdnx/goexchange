package exchange

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ErrorType represents the category of an error
type ErrorType string

const (
	// ErrorTypeHTTP represents HTTP-related errors (status codes, etc.)
	ErrorTypeHTTP ErrorType = "http"

	// ErrorTypeNetwork represents network-related errors (connection issues, timeouts, etc.)
	ErrorTypeNetwork ErrorType = "network"

	// ErrorTypeRateLimit represents rate limiting errors
	ErrorTypeRateLimit ErrorType = "rate_limit"

	// ErrorTypeAuthentication represents authentication errors
	ErrorTypeAuthentication ErrorType = "authentication"

	// ErrorTypeParsing represents JSON parsing or data format errors
	ErrorTypeParsing ErrorType = "parsing"

	// ErrorTypeValidation represents validation errors (invalid parameters, etc.)
	ErrorTypeValidation ErrorType = "validation"

	// ErrorTypeExchange represents exchange-specific errors
	ErrorTypeExchange ErrorType = "exchange"

	// ErrorTypeSystem represents system-level errors (out of memory, etc.)
	ErrorTypeSystem ErrorType = "system"

	// ErrorTypeUnknown represents unknown errors
	ErrorTypeUnknown ErrorType = "unknown"
)

// ExchangeError is the base error type for all exchange-related errors
type ExchangeError struct {
	Type        ErrorType
	Code        string
	Message     string
	StatusCode  int
	RawResponse []byte
	Timestamp   time.Time
	Retriable   bool
	Cause       error
}

// Error returns the error message
func (e *ExchangeError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("[%s:%s] %s (HTTP %d)", e.Type, e.Code, e.Message, e.StatusCode)
	}
	return fmt.Sprintf("[%s:%s] %s", e.Type, e.Code, e.Message)
}

// Unwrap returns the underlying cause of the error
func (e *ExchangeError) Unwrap() error {
	return e.Cause
}

// IsRetriable returns whether the error is retriable
func (e *ExchangeError) IsRetriable() bool {
	return e.Retriable
}

// ParseJSON parses the error body as JSON
func (e *ExchangeError) ParseJSON(v interface{}) error {
	return json.Unmarshal(e.RawResponse, v)
}

// NewExchangeError creates a new exchange error
func NewExchangeError(errType ErrorType, code string, message string, cause error) *ExchangeError {
	return &ExchangeError{
		Type:      errType,
		Code:      code,
		Message:   message,
		Timestamp: time.Now(),
		Retriable: false,
		Cause:     cause,
	}
}

// NewNetworkError creates a new network error
func NewNetworkError(code string, message string, cause error, retriable bool) *ExchangeError {
	return &ExchangeError{
		Type:      ErrorTypeNetwork,
		Code:      code,
		Message:   message,
		Timestamp: time.Now(),
		Retriable: retriable,
		Cause:     cause,
	}
}

// NewExchangeHTTPError creates a new HTTP error with enhanced information
func NewExchangeHTTPError(statusCode int, body []byte, message string) *ExchangeError {
	retriable := statusCode >= 500 || statusCode == http.StatusTooManyRequests

	errType := ErrorTypeHTTP
	code := fmt.Sprintf("http_%d", statusCode)

	// Categorize common HTTP errors
	switch statusCode {
	case http.StatusTooManyRequests:
		errType = ErrorTypeRateLimit
		code = "rate_limit_exceeded"
	case http.StatusUnauthorized, http.StatusForbidden:
		errType = ErrorTypeAuthentication
		code = "authentication_failed"
	case http.StatusBadRequest:
		errType = ErrorTypeValidation
		code = "invalid_request"
	}

	return &ExchangeError{
		Type:        errType,
		Code:        code,
		Message:     message,
		StatusCode:  statusCode,
		RawResponse: body,
		Timestamp:   time.Now(),
		Retriable:   retriable,
	}
}

// NewParsingError creates a new parsing error
func NewParsingError(message string, cause error, rawData []byte) *ExchangeError {
	return &ExchangeError{
		Type:        ErrorTypeParsing,
		Code:        "json_parse_error",
		Message:     message,
		Timestamp:   time.Now(),
		Retriable:   false,
		Cause:       cause,
		RawResponse: rawData,
	}
}

// NewValidationError creates a new validation error
func NewValidationError(code string, message string) *ExchangeError {
	return &ExchangeError{
		Type:      ErrorTypeValidation,
		Code:      code,
		Message:   message,
		Timestamp: time.Now(),
		Retriable: false,
	}
}

// NewRateLimitError creates a new rate limit error
func NewRateLimitError(message string, retryAfter time.Duration) *ExchangeError {
	return &ExchangeError{
		Type:      ErrorTypeRateLimit,
		Code:      "rate_limit_exceeded",
		Message:   message,
		Timestamp: time.Now(),
		Retriable: true,
	}
}

// NewAuthenticationError creates a new authentication error
func NewAuthenticationError(message string) *ExchangeError {
	return &ExchangeError{
		Type:      ErrorTypeAuthentication,
		Code:      "authentication_failed",
		Message:   message,
		Timestamp: time.Now(),
		Retriable: false,
	}
}

// IsNetworkError checks if the error is a network error
func IsNetworkError(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.Type == ErrorTypeNetwork
}

// IsHTTPError checks if the error is an HTTP error
func IsHTTPError(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.Type == ErrorTypeHTTP
}

// IsRateLimitError checks if the error is a rate limit error
func IsRateLimitError(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.Type == ErrorTypeRateLimit
}

// IsAuthenticationError checks if the error is an authentication error
func IsAuthenticationError(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.Type == ErrorTypeAuthentication
}

// IsParsingError checks if the error is a parsing error
func IsParsingError(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.Type == ErrorTypeParsing
}

// IsValidationError checks if the error is a validation error
func IsValidationError(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.Type == ErrorTypeValidation
}

// IsRetriable checks if the error is retriable
func IsRetriable(err error) bool {
	var exchangeErr *ExchangeError
	if err == nil {
		return false
	}
	if asErr, ok := err.(*ExchangeError); ok {
		exchangeErr = asErr
	} else {
		return false
	}
	return exchangeErr.IsRetriable()
}
