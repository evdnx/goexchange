package security

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// APIKeyMiddleware is a middleware for API key authentication
type APIKeyMiddleware struct {
	keyManager *KeyManager
	keyName    string
}

// NewAPIKeyMiddleware creates a new APIKeyMiddleware
func NewAPIKeyMiddleware(keyManager *KeyManager, keyName string) *APIKeyMiddleware {
	return &APIKeyMiddleware{
		keyManager: keyManager,
		keyName:    keyName,
	}
}

// Middleware returns an http.Handler middleware function
func (m *APIKeyMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get API key from header
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			// Try to get from query parameter
			apiKey = r.URL.Query().Get("api_key")
		}

		if apiKey == "" {
			http.Error(w, "Unauthorized: Missing API key", http.StatusUnauthorized)
			return
		}

		// Get the expected API key
		expectedKey, err := m.keyManager.GetKey(m.keyName)
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Compare API keys using constant-time comparison
		if subtle.ConstantTimeCompare([]byte(apiKey), []byte(expectedKey)) != 1 {
			http.Error(w, "Unauthorized: Invalid API key", http.StatusUnauthorized)
			return
		}

		// API key is valid, proceed with the request
		next.ServeHTTP(w, r)
	})
}

// APIKeyContext is the context key for the API key
type APIKeyContext string

const (
	// APIKeyContextKey is the key for the API key in the context
	APIKeyContextKey APIKeyContext = "api_key"
)

// WithAPIKey adds the API key to the context
func WithAPIKey(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, APIKeyContextKey, apiKey)
}

// GetAPIKey gets the API key from the context
func GetAPIKey(ctx context.Context) (string, bool) {
	apiKey, ok := ctx.Value(APIKeyContextKey).(string)
	return apiKey, ok
}

// APIKeyAuthFunc is a function that authenticates an API key
type APIKeyAuthFunc func(apiKey string) bool

// APIKeyAuth is middleware for API key authentication with a custom auth function
func APIKeyAuth(authFunc APIKeyAuthFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get API key from header
			apiKey := r.Header.Get("X-API-Key")
			if apiKey == "" {
				// Try to get from query parameter
				apiKey = r.URL.Query().Get("api_key")
			}

			if apiKey == "" {
				http.Error(w, "Unauthorized: Missing API key", http.StatusUnauthorized)
				return
			}

			// Authenticate the API key
			if !authFunc(apiKey) {
				http.Error(w, "Unauthorized: Invalid API key", http.StatusUnauthorized)
				return
			}

			// API key is valid, add it to the context
			ctx := WithAPIKey(r.Context(), apiKey)

			// Proceed with the request
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// TokenMiddleware is a middleware for token authentication
type TokenMiddleware struct {
	tokenManager *PasetoTokenManager
}

// NewTokenMiddleware creates a new TokenMiddleware
func NewTokenMiddleware(tokenManager *PasetoTokenManager) *TokenMiddleware {
	return &TokenMiddleware{
		tokenManager: tokenManager,
	}
}

// GenerateToken generates a PASETO token
func (m *TokenMiddleware) GenerateToken(subject string, expiration time.Duration, custom map[string]interface{}) (string, error) {
	return m.tokenManager.GenerateToken(subject, expiration, custom)
}

// ValidateToken validates a PASETO token
func (m *TokenMiddleware) ValidateToken(token string) (*TokenClaims, error) {
	return m.tokenManager.ValidateToken(token)
}

// Middleware returns an http.Handler middleware function
func (m *TokenMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get token from Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Unauthorized: Missing Authorization header", http.StatusUnauthorized)
			return
		}

		// Check if it's a Bearer token
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "Unauthorized: Invalid Authorization header format", http.StatusUnauthorized)
			return
		}

		// Get the token
		token := parts[1]

		// Validate the token
		claims, err := m.ValidateToken(token)
		if err != nil {
			http.Error(w, fmt.Sprintf("Unauthorized: %s", err), http.StatusUnauthorized)
			return
		}

		// Add claims to context
		ctx := context.WithValue(r.Context(), TokenClaimsContextKey, claims)

		// Proceed with the request
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TokenClaimsContext is the context key for token claims
type TokenClaimsContext string

const (
	// TokenClaimsContextKey is the key for token claims in the context
	TokenClaimsContextKey TokenClaimsContext = "token_claims"
)

// GetTokenClaims gets the token claims from the context
func GetTokenClaims(ctx context.Context) (*TokenClaims, bool) {
	claims, ok := ctx.Value(TokenClaimsContextKey).(*TokenClaims)
	return claims, ok
}
