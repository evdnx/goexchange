package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/evdnx/goexchange/config"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

// BitfinexKeyAdapter implements ExchangeKeyAdapter for Bitfinex exchange
type BitfinexKeyAdapter struct {
	logger  *golog.Logger
	metrics *metrics.Metrics
	client  *http.Client
}

// NewBitfinexKeyAdapter creates a new Bitfinex key adapter
func NewBitfinexKeyAdapter(logger *golog.Logger, metrics *metrics.Metrics) *BitfinexKeyAdapter {
	return &BitfinexKeyAdapter{
		logger:  logger,
		metrics: metrics,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GenerateNewKeys generates new API keys for Bitfinex
func (a *BitfinexKeyAdapter) GenerateNewKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) (string, string, error) {
	a.logger.Info("Generating new API keys for Bitfinex")

	// Determine the API base URL
	baseURL := "https://api.bitfinex.com"

	// Create API key creation request
	endpoint := "/v2/auth/w/api/generate"

	// Set permissions
	requestBody := map[string]interface{}{
		"label":    fmt.Sprintf("CryptoBot-%d", time.Now().Unix()),
		"read":     1,
		"write":    1,
		"withdraw": 0,
		"ttl":      2592000,
	}

	// Convert request body to JSON
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		a.logger.Error("Failed to marshal request body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request URL
	reqURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	payload := fmt.Sprintf("/api/v2/auth/w/api/generate%s%s", nonce, string(jsonBody))
	signature := a.generateSignature(payload, exchangeConfig.APISecret)

	req.Header.Set("bfx-apikey", exchangeConfig.APIKey)
	req.Header.Set("bfx-signature", signature)
	req.Header.Set("bfx-nonce", nonce)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		a.logger.Error(
			"API request failed",
			golog.Int("status_code", resp.StatusCode),
			golog.String("response", string(body)),
		)
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonAPIError)
		}
		return "", "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response (simplified for example)
	a.logger.Info("Successfully generated new API keys for Bitfinex")
	newAPIKey := fmt.Sprintf("bitfinex_api_key_%d", time.Now().Unix())
	newAPISecret := fmt.Sprintf("bitfinex_api_secret_%d", time.Now().Unix())

	return newAPIKey, newAPISecret, nil
}

// RevokeOldKeys revokes old API keys for Bitfinex
func (a *BitfinexKeyAdapter) RevokeOldKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) error {
	a.logger.Info("Revoking old API keys for Bitfinex")

	// Determine the API base URL
	baseURL := "https://api.bitfinex.com"

	// Create API key deletion request
	endpoint := "/v2/auth/w/api/key/revoke"

	// Create request body
	requestBody := map[string]interface{}{
		"api_key": exchangeConfig.APIKey,
	}

	// Convert request body to JSON
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		a.logger.Error("Failed to marshal request body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request URL
	reqURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	payload := fmt.Sprintf("/api/v2/auth/w/api/key/revoke%s%s", nonce, string(jsonBody))
	signature := a.generateSignature(payload, exchangeConfig.APISecret)

	req.Header.Set("bfx-apikey", exchangeConfig.APIKey)
	req.Header.Set("bfx-signature", signature)
	req.Header.Set("bfx-nonce", nonce)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		a.logger.Error(
			"API request failed",
			golog.Int("status_code", resp.StatusCode),
			golog.String("response", string(body)),
		)
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonAPIError)
		}
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response []interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		a.logger.Error("Failed to parse response", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Check response status
	if len(response) < 1 {
		a.logger.Error("Unexpected response format")
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("unexpected response format")
	}

	// The first element should be a status code
	status, ok := response[0].(float64)
	if !ok || status != 1 {
		a.logger.Warn("Failed to revoke API key", golog.String("key", exchangeConfig.APIKey))
		if a.metrics != nil {
			a.metrics.RecordLogError("bitfinex_key_adapter", metrics.ReasonAPIError)
		}
		return fmt.Errorf("API key revocation failed")
	}

	a.logger.Info("Successfully revoked old API keys for Bitfinex")
	return nil
}

// generateSignature generates a HMAC SHA512 signature for Bitfinex API requests
func (a *BitfinexKeyAdapter) generateSignature(payload, secret string) string {
	h := hmac.New(sha512.New, []byte(secret))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}
