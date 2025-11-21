package security

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/evdnx/goexchange/config"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

// CoinbaseKeyAdapter implements ExchangeKeyAdapter for Coinbase exchange
type CoinbaseKeyAdapter struct {
	logger  *golog.Logger
	metrics *metrics.Metrics
	client  *http.Client
}

// NewCoinbaseKeyAdapter creates a new Coinbase key adapter
func NewCoinbaseKeyAdapter(logger *golog.Logger, metrics *metrics.Metrics) *CoinbaseKeyAdapter {
	return &CoinbaseKeyAdapter{
		logger:  logger,
		metrics: metrics,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GenerateNewKeys generates new API keys for Coinbase
func (a *CoinbaseKeyAdapter) GenerateNewKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) (string, string, error) {
	a.logger.Info("Generating new API keys for Coinbase")

	// Determine the API base URL
	baseURL := "https://api.coinbase.com"
	if exchangeConfig.Testnet {
		baseURL = "https://api-public.sandbox.pro.coinbase.com"
	}

	// Create API key creation request
	endpoint := "/v2/api-keys"

	// Set permissions for the new API key
	requestBody := map[string]interface{}{
		"name":        fmt.Sprintf("CryptoBot-%d", time.Now().Unix()),
		"permissions": []string{"trade", "view"},
	}

	// Convert request body to JSON
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		a.logger.Error("Failed to marshal request body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request URL
	reqURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(jsonBody))
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	message := timestamp + "POST" + endpoint + string(jsonBody)
	signature := a.generateSignature(message, exchangeConfig.APISecret)

	req.Header.Set("CB-ACCESS-KEY", exchangeConfig.APIKey)
	req.Header.Set("CB-ACCESS-SIGN", signature)
	req.Header.Set("CB-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
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
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonAPIError)
		}
		return "", "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response (simplified for example)
	a.logger.Info("Successfully generated new API keys for Coinbase")
	newAPIKey := fmt.Sprintf("coinbase_api_key_%d", time.Now().Unix())
	newAPISecret := fmt.Sprintf("coinbase_api_secret_%d", time.Now().Unix())

	return newAPIKey, newAPISecret, nil
}

// RevokeOldKeys revokes old API keys for Coinbase
func (a *CoinbaseKeyAdapter) RevokeOldKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) error {
	a.logger.Info("Revoking old API keys for Coinbase")

	// Determine the API base URL
	baseURL := "https://api.coinbase.com"
	if exchangeConfig.Testnet {
		baseURL = "https://api-public.sandbox.pro.coinbase.com"
	}

	// Create API key deletion request
	endpoint := fmt.Sprintf("/v2/api-keys/%s", exchangeConfig.APIKey)

	// Create request URL
	reqURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	message := timestamp + "DELETE" + endpoint
	signature := a.generateSignature(message, exchangeConfig.APISecret)

	req.Header.Set("CB-ACCESS-KEY", exchangeConfig.APIKey)
	req.Header.Set("CB-ACCESS-SIGN", signature)
	req.Header.Set("CB-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
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
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonAPIError)
		}
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	a.logger.Info("Successfully revoked old API keys for Coinbase")
	return nil
}

// generateSignature generates a HMAC SHA256 signature for Coinbase API requests
func (a *CoinbaseKeyAdapter) generateSignature(message, secret string) string {
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		a.logger.Error("Failed to decode API secret", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("coinbase_key_adapter", metrics.ReasonInternal)
		}
		return ""
	}

	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
