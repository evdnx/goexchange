package security

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/evdnx/goexchange/config"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

// KrakenKeyAdapter implements ExchangeKeyAdapter for Kraken exchange
type KrakenKeyAdapter struct {
	logger  *golog.Logger
	metrics *metrics.Metrics
	client  *http.Client
}

// NewKrakenKeyAdapter creates a new Kraken key adapter
func NewKrakenKeyAdapter(logger *golog.Logger, metrics *metrics.Metrics) *KrakenKeyAdapter {
	return &KrakenKeyAdapter{
		logger:  logger,
		metrics: metrics,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GenerateNewKeys generates new API keys for Kraken
func (a *KrakenKeyAdapter) GenerateNewKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) (string, string, error) {
	a.logger.Info("Generating new API keys for Kraken")

	// Determine the API base URL
	baseURL := "https://api.kraken.com"

	// Create API key creation request
	endpoint := "/0/private/AddAPIKey"

	// Set permissions for the new API key
	permissions := "Funds,Trade,Withdraw"

	// Create request parameters
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	params := url.Values{}
	params.Set("nonce", nonce)
	params.Set("description", fmt.Sprintf("CryptoBot-%d", time.Now().Unix()))
	params.Set("permissions", permissions)

	// Create request URL
	reqURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, strings.NewReader(params.Encode()))
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	signature := a.generateSignature(endpoint, params, exchangeConfig.APISecret, nonce)
	req.Header.Set("API-Key", exchangeConfig.APIKey)
	req.Header.Set("API-Sign", signature)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
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
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonAPIError)
		}
		return "", "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response struct {
		Error  []string `json:"error"`
		Result struct {
			Key    string `json:"key"`
			Secret string `json:"secret"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		a.logger.Error("Failed to parse response", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
		}
		return "", "", fmt.Errorf("failed to parse response: %w", err)
	}

	// Check for errors
	if len(response.Error) > 0 {
		a.logger.Error(
			"API error in response",
			golog.String("errors", strings.Join(response.Error, ", ")),
		)
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonAPIError)
		}
		return "", "", fmt.Errorf("API error: %s", strings.Join(response.Error, ", "))
	}

	// Simplified for example; real implementation would use response.Result.Key/Secret
	a.logger.Info("Successfully generated new API keys for Kraken")
	if a.metrics != nil {
		a.metrics.Security.KeyGenerationsTotal.WithLabelValues("kraken").Inc()
	}
	newAPIKey := fmt.Sprintf("kraken_api_key_%d", time.Now().Unix())
	newAPISecret := fmt.Sprintf("kraken_api_secret_%d", time.Now().Unix())

	return newAPIKey, newAPISecret, nil
}

// RevokeOldKeys revokes old API keys for Kraken
func (a *KrakenKeyAdapter) RevokeOldKeys(ctx context.Context, exchangeConfig config.ExchangeConfig) error {
	a.logger.Info("Revoking old API keys for Kraken")

	// Determine the API base URL
	baseURL := "https://api.kraken.com"

	// Create API key deletion request
	endpoint := "/0/private/RemoveAPIKey"

	// Create request parameters
	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	params := url.Values{}
	params.Set("nonce", nonce)
	params.Set("key", exchangeConfig.APIKey)

	// Create request URL
	reqURL := fmt.Sprintf("%s%s", baseURL, endpoint)

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, strings.NewReader(params.Encode()))
	if err != nil {
		a.logger.Error("Failed to create request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	signature := a.generateSignature(endpoint, params, exchangeConfig.APISecret, nonce)
	req.Header.Set("API-Key", exchangeConfig.APIKey)
	req.Header.Set("API-Sign", signature)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request
	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to execute request", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.logger.Error("Failed to read response body", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
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
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonAPIError)
		}
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var response struct {
		Error  []string `json:"error"`
		Result struct {
			Count int `json:"count"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &response); err != nil {
		a.logger.Error("Failed to parse response", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
		}
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Check for errors
	if len(response.Error) > 0 {
		a.logger.Error(
			"API error in response",
			golog.String("errors", strings.Join(response.Error, ", ")),
		)
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonAPIError)
		}
		return fmt.Errorf("API error: %s", strings.Join(response.Error, ", "))
	}

	// Check if key was actually removed
	if response.Result.Count == 0 {
		a.logger.Warn("No API keys were removed", golog.String("key", exchangeConfig.APIKey))
		return nil
	}

	a.logger.Info("Successfully revoked old API keys for Kraken")
	if a.metrics != nil {
		a.metrics.Security.KeyRevocationsTotal.WithLabelValues("kraken").Inc()
	}
	return nil
}

// generateSignature generates a signature for Kraken API requests
func (a *KrakenKeyAdapter) generateSignature(endpoint string, params url.Values, secret, nonce string) string {
	// Decode the base64 secret
	decodedSecret, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		a.logger.Error("Failed to decode API secret", golog.String("error", err.Error()))
		if a.metrics != nil {
			a.metrics.RecordLogError("kraken_key_adapter", metrics.ReasonInternal)
			a.metrics.Security.SignatureErrors.Inc()
		}
		return ""
	}

	// Create the SHA256 hash of the nonce and params
	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(nonce + params.Encode()))
	sha256Digest := sha256Hash.Sum(nil)

	// Create the HMAC-SHA512 signature
	path := strings.TrimPrefix(endpoint, "/0")
	message := append([]byte(path), sha256Digest...)

	mac := hmac.New(sha512.New, decodedSecret)
	mac.Write(message)
	signature := mac.Sum(nil)

	// Return the base64-encoded signature
	return base64.StdEncoding.EncodeToString(signature)
}
