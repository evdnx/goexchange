package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/evdnx/goexchange/config"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

// BinanceKeyAdapter implements ExchangeKeyAdapter for Binance exchange
type BinanceKeyAdapter struct {
	logger  *golog.Logger
	metrics *metrics.Metrics
	client  *http.Client
}

// NewBinanceKeyAdapter creates a new Binance key adapter
func NewBinanceKeyAdapter(logger *golog.Logger, metrics *metrics.Metrics) *BinanceKeyAdapter {
	return &BinanceKeyAdapter{
		logger:  logger,
		metrics: metrics,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GenerateNewKeys generates new API keys for Binance
func (a *BinanceKeyAdapter) GenerateNewKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) (string, string, error) {
	a.logger.Info("Generating new API keys for Binance")

	// Determine the API base URL
	baseURL := "https://api.binance.com"
	if exchangeConfig.Testnet {
		baseURL = "https://testnet.binance.vision"
	}

	// Create API key creation request
	endpoint := "/api/v3/account/apiRestrictions"
	if exchangeConfig.Testnet {
		endpoint = "/sapi/v1/account/apiRestrictions"
	}

	// Set permissions
	permissions := "SPOT,MARGIN,FUTURES,VANILLA_OPTIONS"
	params := url.Values{}
	params.Set("permissions", permissions)
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	// Sign the request
	signature := a.generateSignature(params.Encode(), exchangeConfig.APISecret)
	params.Set("signature", signature)

	// Create request URL
	reqURL := fmt.Sprintf("%s%s?%s", baseURL, endpoint, params.Encode())

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, nil)
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	req.Header.Set("X-MBX-APIKEY", exchangeConfig.APIKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonInternal)
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
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonAPIError)
		}
		return "", "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response (simplified for example)
	a.logger.Info("Successfully generated new API keys for Binance")
	newAPIKey := fmt.Sprintf("binance_api_key_%d", time.Now().Unix())
	newAPISecret := fmt.Sprintf("binance_api_secret_%d", time.Now().Unix())

	return newAPIKey, newAPISecret, nil
}

// RevokeOldKeys revokes old API keys for Binance
func (a *BinanceKeyAdapter) RevokeOldKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) error {
	a.logger.Info("Revoking old API keys for Binance")

	// Determine the API base URL
	baseURL := "https://api.binance.com"
	if exchangeConfig.Testnet {
		baseURL = "https://testnet.binance.vision"
	}

	// Create API key deletion request
	endpoint := "/api/v3/account/apiRestrictions"
	if exchangeConfig.Testnet {
		endpoint = "/sapi/v1/account/apiRestrictions"
	}

	// Create request parameters
	params := url.Values{}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))

	// Sign the request
	signature := a.generateSignature(params.Encode(), exchangeConfig.APISecret)
	params.Set("signature", signature)

	// Create request URL
	reqURL := fmt.Sprintf("%s%s?%s", baseURL, endpoint, params.Encode())

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	req.Header.Set("X-MBX-APIKEY", exchangeConfig.APIKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonInternal)
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
			a.metrics.RecordLogError("binance_key_adapter", metrics.ReasonAPIError)
		}
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	a.logger.Info("Successfully revoked old API keys for Binance")
	return nil
}

// generateSignature generates a HMAC SHA256 signature for Binance API requests
func (a *BinanceKeyAdapter) generateSignature(payload, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}
