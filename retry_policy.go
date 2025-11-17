package exchange

import (
	"errors"
	"math/rand"
	"net"
	"strings"
	"time"
)

// RetryPolicy defines how retries should be handled
type RetryPolicy struct {
	// MaxRetries is the maximum number of retries
	MaxRetries int

	// InitialBackoff is the initial backoff duration
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration
	MaxBackoff time.Duration

	// BackoffFactor is the factor by which the backoff increases
	BackoffFactor float64

	// JitterFactor is the factor by which jitter is applied
	JitterFactor float64

	// RetryableStatusCodes is a map of HTTP status codes that should be retried
	RetryableStatusCodes map[int]bool

	// RetryBudget limits the total number of retries across all requests
	RetryBudget *RetryBudget
}

// RetryBudget implements a token bucket for limiting the total number of retries
type RetryBudget struct {
	// MaxRetryRatio is the maximum ratio of retries to requests
	MaxRetryRatio float64

	// RetryRatio is the current ratio of retries to requests
	RetryRatio float64

	// TotalRequests is the total number of requests
	TotalRequests int64

	// TotalRetries is the total number of retries
	TotalRetries int64
}

// NewRetryBudget creates a new retry budget
func NewRetryBudget(maxRetryRatio float64) *RetryBudget {
	return &RetryBudget{
		MaxRetryRatio: maxRetryRatio,
		RetryRatio:    0,
		TotalRequests: 0,
		TotalRetries:  0,
	}
}

// AllowRetry checks if a retry is allowed based on the retry budget
func (rb *RetryBudget) AllowRetry() bool {
	// Always allow at least one retry
	if rb.TotalRequests < 100 {
		return true
	}

	// Calculate current retry ratio
	rb.RetryRatio = float64(rb.TotalRetries) / float64(rb.TotalRequests)

	// Allow retry if we're under budget
	return rb.RetryRatio < rb.MaxRetryRatio
}

// RecordRequest records a request
func (rb *RetryBudget) RecordRequest() {
	rb.TotalRequests++
}

// RecordRetry records a retry
func (rb *RetryBudget) RecordRetry() {
	rb.TotalRetries++
}

// DefaultRetryPolicy returns a default retry policy
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		BackoffFactor:  2.0,
		JitterFactor:   0.2,
		RetryableStatusCodes: map[int]bool{
			408: true, // Request Timeout
			429: true, // Too Many Requests
			500: true, // Internal Server Error
			502: true, // Bad Gateway
			503: true, // Service Unavailable
			504: true, // Gateway Timeout
			507: true, // Insufficient Storage
			509: true, // Bandwidth Limit Exceeded
			520: true, // Unknown Error (Cloudflare)
			521: true, // Web Server Is Down (Cloudflare)
			522: true, // Connection Timed Out (Cloudflare)
			523: true, // Origin Is Unreachable (Cloudflare)
			524: true, // A Timeout Occurred (Cloudflare)
			525: true, // SSL Handshake Failed (Cloudflare)
			526: true, // Invalid SSL Certificate (Cloudflare)
			527: true, // Railgun Error (Cloudflare)
		},
		RetryBudget: NewRetryBudget(0.2), // 20% retry ratio
	}
}

// ShouldRetry determines if a request should be retried based on the error and attempt number
func (p *RetryPolicy) ShouldRetry(err error, statusCode int, attempt int) bool {
	// Don't retry if we've reached the maximum number of retries
	if attempt >= p.MaxRetries {
		return false
	}

	// Check retry budget
	if !p.RetryBudget.AllowRetry() {
		return false
	}

	// Check status code if provided
	if statusCode > 0 {
		if p.RetryableStatusCodes[statusCode] {
			p.RetryBudget.RecordRetry()
			return true
		}
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Retry network timeouts and temporary errors
		if netErr.Timeout() || netErr.Temporary() {
			p.RetryBudget.RecordRetry()
			return true
		}
	}

	// Check for specific error types that should be retried
	if err != nil {
		errStr := err.Error()

		// Check for connection errors
		if strings.Contains(errStr, "connection") &&
			(strings.Contains(errStr, "reset") ||
				strings.Contains(errStr, "refused") ||
				strings.Contains(errStr, "closed")) {
			p.RetryBudget.RecordRetry()
			return true
		}

		// Check for timeout errors
		if strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "deadline exceeded") {
			p.RetryBudget.RecordRetry()
			return true
		}

		// Check for EOF errors
		if strings.Contains(errStr, "EOF") {
			p.RetryBudget.RecordRetry()
			return true
		}
	}

	// Check if it's an ExchangeError and is retriable
	if exchangeErr, ok := err.(*ExchangeError); ok {
		if exchangeErr.IsRetriable() {
			p.RetryBudget.RecordRetry()
			return true
		}
	}

	return false
}

// CalculateBackoff calculates the backoff duration with jitter
func (p *RetryPolicy) CalculateBackoff(attempt int) time.Duration {
	// Calculate base backoff using exponential backoff
	backoff := float64(p.InitialBackoff)
	for i := 1; i < attempt; i++ {
		backoff *= p.BackoffFactor
	}

	// Apply maximum backoff limit
	if backoff > float64(p.MaxBackoff) {
		backoff = float64(p.MaxBackoff)
	}

	// Apply jitter to avoid thundering herd problem
	jitter := rand.Float64() * p.JitterFactor * backoff
	backoff = backoff + jitter

	return time.Duration(backoff)
}

// BackoffStrategies defines different backoff strategies
type BackoffStrategy string

const (
	// BackoffExponential is exponential backoff with jitter
	BackoffExponential BackoffStrategy = "exponential"

	// BackoffLinear is linear backoff with jitter
	BackoffLinear BackoffStrategy = "linear"

	// BackoffConstant is constant backoff with jitter
	BackoffConstant BackoffStrategy = "constant"

	// BackoffFibonacci is fibonacci backoff with jitter
	BackoffFibonacci BackoffStrategy = "fibonacci"
)

// CalculateBackoffWithStrategy calculates backoff using the specified strategy
func (p *RetryPolicy) CalculateBackoffWithStrategy(attempt int, strategy BackoffStrategy) time.Duration {
	var backoff float64

	switch strategy {
	case BackoffExponential:
		// Exponential backoff: initialBackoff * backoffFactor^(attempt-1)
		backoff = float64(p.InitialBackoff)
		for i := 1; i < attempt; i++ {
			backoff *= p.BackoffFactor
		}
	case BackoffLinear:
		// Linear backoff: initialBackoff * attempt
		backoff = float64(p.InitialBackoff) * float64(attempt)
	case BackoffConstant:
		// Constant backoff: initialBackoff
		backoff = float64(p.InitialBackoff)
	case BackoffFibonacci:
		// Fibonacci backoff: initialBackoff * fibonacci(attempt)
		backoff = float64(p.InitialBackoff) * float64(fibonacci(attempt))
	default:
		// Default to exponential backoff
		backoff = float64(p.InitialBackoff)
		for i := 1; i < attempt; i++ {
			backoff *= p.BackoffFactor
		}
	}

	// Apply maximum backoff limit
	if backoff > float64(p.MaxBackoff) {
		backoff = float64(p.MaxBackoff)
	}

	// Apply jitter to avoid thundering herd problem
	jitter := rand.Float64() * p.JitterFactor * backoff
	backoff = backoff + jitter

	return time.Duration(backoff)
}

// fibonacci calculates the nth Fibonacci number
func fibonacci(n int) int {
	if n <= 0 {
		return 0
	}
	if n == 1 {
		return 1
	}
	return fibonacci(n-1) + fibonacci(n-2)
}
