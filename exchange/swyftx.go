package exchange

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	common "github.com/evdnx/goexchange/common"
	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/gohttpcl"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

const (
	swyftxHTTPTimeout        = 30 * time.Second // Increased from 12s to accommodate slower chart endpoints
	swyftxAssetCacheTTL      = 30 * time.Minute
	swyftxTokenRefreshLeeway = 45 * time.Second
	swyftxUserAgent          = "GoExchangeClient/1.0"
	swyftxDefaultRateAmount  = 100.0
	// swyftxTokenExpiry is the default token expiry if JWT parsing fails.
	// According to Swyftx API docs, access tokens last for a week (7 days).
	// This fallback is only used if JWT expiry cannot be parsed from the token.
	swyftxTokenExpiry = 7 * 24 * time.Hour
	// swyftxTokenRefreshInterval is how often to proactively refresh the access token.
	// Tokens are refreshed every 5 days to ensure they remain valid.
	swyftxTokenRefreshInterval = 5 * 24 * time.Hour
)

// SwyftxClient implements the ExchangeClient interface for the Swyftx exchange.
type SwyftxClient struct {
	*common.BaseClient
	baseURL       string
	publicBaseURL string
	authBaseURL   string
	httpClient    *gohttpcl.Client
	metrics       *metrics.Metrics
	logger        *golog.Logger
	userAgent     string
	httpTimeout   time.Duration

	tokenMu          sync.RWMutex
	accessToken      string
	tokenExpiry      time.Time
	tokenRefreshedAt time.Time

	assetMu       sync.RWMutex
	assets        map[string]*swyftxAsset
	assetsByID    map[int]*swyftxAsset
	assetPairs    []common.TradingPair
	assetsFetched time.Time
}

// swyftxAsset describes an asset returned by the /markets/assets/ endpoint.
type swyftxAsset struct {
	ID              int      `json:"id"`
	Code            string   `json:"code"`
	Name            string   `json:"name"`
	PrimaryRaw      boolish  `json:"primary"`
	Secondary       boolish  `json:"secondary"`
	PriceScale      int      `json:"price_scale"`
	MinOrder        floatish `json:"minimum_order"`
	DepositEnabled  boolish  `json:"deposit_enabled"`
	WithdrawEnabled boolish  `json:"withdraw_enabled"`
	Delisting       boolish  `json:"delisting"`
	BuyDisabled     boolish  `json:"buyDisabled"`
}

func (a *swyftxAsset) Primary() bool {
	if a == nil {
		return false
	}
	return bool(a.PrimaryRaw)
}

type boolish bool

func (b *boolish) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	if len(s) == 0 || s == "null" {
		*b = false
		return nil
	}
	if s[0] == '"' && s[len(s)-1] == '"' {
		s = strings.Trim(s, "\"")
	}
	switch strings.ToLower(s) {
	case "true":
		*b = true
		return nil
	case "false":
		*b = false
		return nil
	}
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*b = val != 0
	return nil
}

type floatish float64

func (f *floatish) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	if len(s) == 0 || s == "null" {
		*f = 0
		return nil
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = strings.Trim(s, "\"")
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*f = floatish(v)
	return nil
}

// NewSwyftxClient creates a new Swyftx API client.
// According to Swyftx API documentation:
// - Production base URL: https://api.swyftx.com.au
// - Demo base URL: https://api.demo.swyftx.com.au
// When testnet is true, the client uses the demo base URL.
// Note: Demo mode may have limitations or differences compared to production.
func NewSwyftxClient(apiKey, apiSecret string, testnet bool, metricsClient *metrics.Metrics) *SwyftxClient {
	baseURL := "https://api.swyftx.com.au"
	publicBaseURL := baseURL
	authBaseURL := baseURL
	if testnet {
		baseURL = "https://api.demo.swyftx.com.au"
	}
	if testnet {
		publicBaseURL = "https://api.swyftx.com.au"
		authBaseURL = publicBaseURL
	}
	client := &SwyftxClient{
		BaseClient:    common.NewBaseClient("Swyftx", apiKey, apiSecret, testnet),
		baseURL:       strings.TrimRight(baseURL, "/"),
		publicBaseURL: strings.TrimRight(publicBaseURL, "/"),
		authBaseURL:   strings.TrimRight(authBaseURL, "/"),
		metrics:       metricsClient,
		logger:        common.DefaultLogger(),
		userAgent:     swyftxUserAgent,
		httpTimeout:   swyftxHTTPTimeout,
	}
	client.httpClient = createSwyftxHTTPClient(metricsClient)
	return client
}

func createSwyftxHTTPClient(metricsClient *metrics.Metrics) *gohttpcl.Client {
	opts := []gohttpcl.Option{
		gohttpcl.WithTimeout(swyftxHTTPTimeout),
		gohttpcl.WithMaxRetries(4),
		gohttpcl.WithMinBackoff(200 * time.Millisecond),
		gohttpcl.WithMaxBackoff(10 * time.Second),
		gohttpcl.WithBackoffFactor(2.0),
		gohttpcl.WithBackoffStrategy(gohttpcl.BackoffExponential),
		gohttpcl.WithRetryBudget(0.2, time.Minute),
	}
	if collector := common.NewHTTPMetricsCollector(metricsClient, "Swyftx"); collector != nil {
		opts = append(opts, gohttpcl.WithMetrics(collector))
	}
	return gohttpcl.New(opts...)
}

func (c *SwyftxClient) doRequest(ctx context.Context, method, path string, body interface{}, auth bool) ([]byte, error) {
	return c.doRequestWithBase(ctx, method, path, body, auth, c.baseURL)
}

func (c *SwyftxClient) doPublicRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	publicBase := c.publicBaseURL
	if publicBase == "" {
		publicBase = c.baseURL
	}
	return c.doRequestWithBase(ctx, method, path, body, false, publicBase)
}

func (c *SwyftxClient) doRequestWithBase(ctx context.Context, method, path string, body interface{}, auth bool, baseURL string) ([]byte, error) {
	var token string
	if auth {
		var err error
		token, err = c.getAccessToken(ctx)
		if err != nil {
			return nil, err
		}
	}
	return c.doRequestWithBaseAndToken(ctx, method, path, body, token, baseURL)
}

func (c *SwyftxClient) doRequestWithBaseAndToken(ctx context.Context, method, path string, body interface{}, token string, baseURL string) ([]byte, error) {
	return c.doRequestWithBaseAndTokenTimeout(ctx, method, path, body, token, baseURL, c.httpTimeout)
}

func (c *SwyftxClient) doRequestWithBaseAndTokenTimeout(ctx context.Context, method, path string, body interface{}, token string, baseURL string, timeout time.Duration) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	// User-Agent header is required on most requests per Swyftx API documentation
	headers := map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   c.userAgent,
	}
	if token != "" {
		// JWT token must be included in Authorization header as "Bearer <token>"
		headers["Authorization"] = "Bearer " + token
	}
	fullURL := strings.TrimRight(baseURL, "/") + path
	opts := make([]gohttpcl.ReqOption, 0, len(headers))
	for k, v := range headers {
		opts = append(opts, gohttpcl.WithHeader(k, v))
	}
	var resp *http.Response
	var err error
	switch method {
	case http.MethodGet:
		resp, err = c.httpClient.Get(ctx, fullURL, timeout, nil, opts...)
	case http.MethodPost:
		resp, err = c.httpClient.Post(ctx, fullURL, bytes.NewReader(bodyBytes), timeout, nil, opts...)
	case http.MethodPut:
		resp, err = c.httpClient.Put(ctx, fullURL, bytes.NewReader(bodyBytes), timeout, nil, opts...)
	case http.MethodDelete:
		resp, err = c.httpClient.Delete(ctx, fullURL, timeout, nil, opts...)
	default:
		return nil, fmt.Errorf("unsupported method %s", method)
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		// Parse Swyftx error response format: {"error":{"error":"OrderError","message":"Unable to retrieve user orders"}}
		errorMsg := parseSwyftxErrorMessage(data)
		return nil, common.NewExchangeHTTPError(resp.StatusCode, data, errorMsg)
	}
	return data, nil
}

// parseSwyftxErrorMessage extracts the error message from Swyftx error responses.
// Swyftx returns errors in the format: {"error":{"error":"OrderError","message":"Unable to retrieve user orders"}}
func parseSwyftxErrorMessage(data []byte) string {
	if len(data) == 0 {
		return "unknown error"
	}
	var errorResp struct {
		Error struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &errorResp); err == nil {
		if errorResp.Error.Message != "" {
			return errorResp.Error.Message
		}
		if errorResp.Error.Error != "" {
			return errorResp.Error.Error
		}
	}
	// Fallback to raw string if parsing fails
	return string(data)
}

// getAccessToken retrieves a valid JWT access token.
// According to Swyftx API documentation:
// - API keys (refresh tokens) are used to generate access tokens via /auth/refresh/
// - The refresh endpoint requires authentication via Authorization header with Bearer token
// - Access tokens (JWTs) last for a week before expiring
// - An API key can be used to generate any number of access tokens
// Tokens are proactively refreshed every 5 days to ensure they remain valid.
func (c *SwyftxClient) getAccessToken(ctx context.Context) (string, error) {
	c.tokenMu.RLock()
	token := c.accessToken
	expires := c.tokenExpiry
	refreshedAt := c.tokenRefreshedAt
	c.tokenMu.RUnlock()

	// Check if token needs refresh: either expired or 5 days have passed since last refresh
	needsRefresh := token == "" ||
		time.Until(expires) <= swyftxTokenRefreshLeeway ||
		(!refreshedAt.IsZero() && time.Since(refreshedAt) >= swyftxTokenRefreshInterval)

	if !needsRefresh {
		return token, nil
	}

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	// Double-check after acquiring write lock
	needsRefresh = c.accessToken == "" ||
		time.Until(c.tokenExpiry) <= swyftxTokenRefreshLeeway ||
		(!c.tokenRefreshedAt.IsZero() && time.Since(c.tokenRefreshedAt) >= swyftxTokenRefreshInterval)

	if !needsRefresh {
		return c.accessToken, nil
	}
	if c.APIKey() == "" {
		return "", common.NewAuthenticationError("swyftx api key required")
	}
	// Refresh Access Token endpoint: POST /auth/refresh/
	// Request body: {"apiKey": "<api_key>"}
	// Response: {"accessToken": "<jwt_token>"}
	// This endpoint requires authentication via Authorization header with Bearer token
	body := map[string]string{"apiKey": c.APIKey()}
	authBase := c.authBaseURL
	if authBase == "" {
		authBase = c.baseURL
	}
	// Use existing token if available (even if expired) for refresh request
	var existingToken string
	if c.accessToken != "" {
		existingToken = c.accessToken
	}
	data, err := c.doRequestWithBaseAndToken(ctx, http.MethodPost, "/auth/refresh/", body, existingToken, authBase)
	if err != nil && c.publicBaseURL != "" && !strings.EqualFold(authBase, c.publicBaseURL) {
		if httpErr, ok := err.(*common.ExchangeError); ok && httpErr.StatusCode == http.StatusNotFound {
			data, err = c.doRequestWithBaseAndToken(ctx, http.MethodPost, "/auth/refresh/", body, existingToken, c.publicBaseURL)
		}
	}
	if err != nil {
		return "", err
	}
	var resp struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", err
	}
	if resp.AccessToken == "" {
		return "", errors.New("swyftx: empty access token")
	}
	// Parse JWT expiry from token. Access tokens last for a week per API docs.
	expiresAt := parseJWTExpiry(resp.AccessToken)
	c.accessToken = resp.AccessToken
	c.tokenRefreshedAt = time.Now()
	if expiresAt.IsZero() {
		// Fallback: use default token expiry if JWT parsing fails
		c.tokenExpiry = time.Now().Add(swyftxTokenExpiry)
	} else {
		c.tokenExpiry = expiresAt
	}
	return c.accessToken, nil
}

func parseJWTExpiry(token string) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}
	}
	if claims.Exp == 0 {
		return time.Time{}
	}
	return time.Unix(claims.Exp, 0)
}

func (c *SwyftxClient) ensureAssets(ctx context.Context) error {
	c.assetMu.RLock()
	valid := time.Since(c.assetsFetched) < swyftxAssetCacheTTL && len(c.assets) > 0
	c.assetMu.RUnlock()
	if valid {
		return nil
	}
	c.assetMu.Lock()
	defer c.assetMu.Unlock()
	if time.Since(c.assetsFetched) < swyftxAssetCacheTTL && len(c.assets) > 0 {
		return nil
	}
	data, err := c.doPublicRequest(ctx, http.MethodGet, "/markets/assets/", nil)
	if err != nil {
		return err
	}
	var assets []swyftxAsset
	if err := json.Unmarshal(data, &assets); err != nil {
		return err
	}
	c.assets = make(map[string]*swyftxAsset, len(assets))
	c.assetsByID = make(map[int]*swyftxAsset, len(assets))
	for i := range assets {
		asset := assets[i]
		code := strings.ToUpper(asset.Code)
		c.assets[code] = &assets[i]
		c.assetsByID[asset.ID] = &assets[i]
	}
	c.assetsFetched = time.Now()
	// Don't build trading pairs here - build them lazily when GetTradingPairs() is called
	// This avoids expensive HTTP calls during asset initialization
	c.assetPairs = nil
	return nil
}

func (c *SwyftxClient) buildTradingPairsLocked(ctx context.Context) []common.TradingPair {
	if len(c.assets) == 0 {
		return nil
	}
	// Filter primary assets (quotes): primary = 1, deposit_enabled = 1, withdraw_enabled = 1
	var quotes []*swyftxAsset
	for _, asset := range c.assets {
		if asset.Primary() &&
			bool(asset.DepositEnabled) &&
			bool(asset.WithdrawEnabled) {
			quotes = append(quotes, asset)
		}
	}
	if len(quotes) == 0 {
		return nil
	}
	// Filter secondary assets (bases): secondary = 1, deposit_enabled = 1, withdraw_enabled = 1,
	// no delisting, and not disabled. Also verify tradability with the API.
	var pairs []common.TradingPair
	for _, quote := range quotes {
		// Check context cancellation before processing each quote
		select {
		case <-ctx.Done():
			return pairs // Return partial results if context is cancelled
		default:
		}
		for _, base := range c.assets {
			// Check context cancellation before processing each base
			select {
			case <-ctx.Done():
				return pairs // Return partial results if context is cancelled
			default:
			}
			if !bool(base.Secondary) || base.ID == quote.ID {
				continue
			}
			// Apply activity filters for secondary assets
			if !bool(base.DepositEnabled) ||
				!bool(base.WithdrawEnabled) ||
				bool(base.Delisting) ||
				bool(base.BuyDisabled) {
				continue
			}
			// Verify the asset is actually tradable by checking with the API
			if !c.isAssetTradable(ctx, base.Code, quote.Code) {
				continue
			}
			symbol := fmt.Sprintf("%s/%s", base.Code, quote.Code)
			pairs = append(pairs, common.TradingPair{
				Symbol:     symbol,
				BaseAsset:  base.Code,
				QuoteAsset: quote.Code,
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Symbol < pairs[j].Symbol })
	return pairs
}

func (c *SwyftxClient) resolveSymbol(ctx context.Context, symbol string) (*swyftxAsset, *swyftxAsset, error) {
	if err := c.ensureAssets(ctx); err != nil {
		return nil, nil, err
	}
	parts := strings.NewReplacer("-", "/", " ", "").Replace(symbol)
	segments := strings.Split(parts, "/")
	if len(segments) != 2 {
		return nil, nil, fmt.Errorf("invalid symbol %s", symbol)
	}
	baseCode := strings.ToUpper(strings.TrimSpace(segments[0]))
	quoteCode := strings.ToUpper(strings.TrimSpace(segments[1]))
	c.assetMu.RLock()
	base := c.assets[baseCode]
	quote := c.assets[quoteCode]
	c.assetMu.RUnlock()
	if base == nil || quote == nil {
		return nil, nil, fmt.Errorf("unknown swyftx symbol %s", symbol)
	}
	return base, quote, nil
}

// GetTradingPairs returns supported trading pairs.
func (c *SwyftxClient) GetTradingPairs() ([]common.TradingPair, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err := c.ensureAssets(ctx); err != nil {
		return nil, err
	}
	c.assetMu.Lock()
	// Build trading pairs lazily if not already built
	if c.assetPairs == nil {
		c.assetPairs = c.buildTradingPairsLocked(ctx)
	}
	pairs := make([]common.TradingPair, len(c.assetPairs))
	copy(pairs, c.assetPairs)
	c.assetMu.Unlock()
	return pairs, nil
}

// ScalpingCoin represents a coin ranked for scalping suitability.
type ScalpingCoin struct {
	Code       string  // Asset code (e.g., "XRP")
	Name       string  // Asset name
	Symbol     string  // Trading pair symbol (e.g., "XRP/AUD")
	Volume     float64 // 24h volume in quote currency (AUD)
	Volatility float64 // Daily volatility percentage (std dev of log returns)
	Score      float64 // Ranking score (volume * volatility)
}

// FindScalpingCoins analyzes all active trading pairs to find the most suitable coins for scalping.
// It filters coins by minimum volume, calculates volatility from 24h of 1-minute candles,
// and ranks them by a score combining volume and volatility.
//
// Parameters:
//   - quoteAsset: The quote currency to use (default: "AUD"). Must be a primary asset.
//   - minVolume: Minimum 24h volume threshold in quote currency (default: 500000)
//   - topN: Number of top coins to return (default: 10)
//   - rateLimitDelay: Delay between API calls to respect rate limits in milliseconds (default: 200ms)
//
// Returns a sorted list of ScalpingCoin structs, ranked by score (volume * volatility).
func (c *SwyftxClient) FindScalpingCoins(quoteAsset string, minVolume float64, topN int, rateLimitDelay time.Duration) ([]ScalpingCoin, error) {
	ctx := context.Background()
	if err := c.ensureAssets(ctx); err != nil {
		return nil, err
	}

	// Default values
	if quoteAsset == "" {
		quoteAsset = "AUD"
	}
	if minVolume <= 0 {
		minVolume = 500000
	}
	if topN <= 0 {
		topN = 10
	}
	if rateLimitDelay <= 0 {
		rateLimitDelay = 200 * time.Millisecond
	}

	quoteAsset = strings.ToUpper(quoteAsset)
	c.assetMu.RLock()
	quote := c.assets[quoteAsset]
	if quote == nil || !quote.Primary() {
		c.assetMu.RUnlock()
		return nil, fmt.Errorf("quote asset %s not found or not a primary asset", quoteAsset)
	}

	// Get all active secondary assets (bases) that can be traded
	var secondaryAssets []*swyftxAsset
	for _, asset := range c.assets {
		if bool(asset.Secondary) &&
			asset.ID != quote.ID &&
			bool(asset.DepositEnabled) &&
			bool(asset.WithdrawEnabled) &&
			!bool(asset.Delisting) &&
			!bool(asset.BuyDisabled) {
			// Verify the asset is actually tradable by checking with the API
			if c.isAssetTradable(ctx, asset.Code, quoteAsset) {
				secondaryAssets = append(secondaryAssets, asset)
			}
		}
	}
	c.assetMu.RUnlock()

	if len(secondaryAssets) == 0 {
		return nil, fmt.Errorf("no active secondary assets found for quote %s", quoteAsset)
	}

	// Analyze each asset
	var rankedCoins []ScalpingCoin
	var allAnalyzedCoins []ScalpingCoin // Track all coins for fallback
	since := time.Now().Add(-24 * time.Hour)

	// Debug counters
	var candlesFetched, candlesInsufficient, volumeFiltered, volatilityFiltered int

	var firstError error
	var errorCount int

	for i, asset := range secondaryAssets {
		// Rate limiting: add delay between requests (except for the first one)
		if i > 0 {
			time.Sleep(rateLimitDelay)
		}

		symbol := fmt.Sprintf("%s/%s", asset.Code, quoteAsset)

		// Fetch 24h of 1-minute candles (1440 candles = 24 hours)
		candles, err := c.GetCandles(symbol, "1m", since, 1440)
		if err != nil {
			// Track first error for debugging
			if firstError == nil {
				firstError = err
			}
			errorCount++
			// Log first few errors for debugging
			if errorCount <= 3 {
				c.logger.Debugf("GetCandles failed for %s: %v", symbol, err)
			}
			// Skip assets that fail to fetch data
			continue
		}
		candlesFetched++

		if len(candles) < 2 {
			candlesInsufficient++
			continue
		}

		// Calculate total volume
		totalVolume := calculateTotalVolume(candles)

		// Calculate volatility from close prices
		volatility := calculateVolatility(candles)

		// Create coin entry (even if it doesn't meet all criteria)
		coin := ScalpingCoin{
			Code:       asset.Code,
			Name:       asset.Name,
			Symbol:     symbol,
			Volume:     totalVolume,
			Volatility: volatility,
			Score:      totalVolume * volatility,
		}

		// Always track all analyzed coins for fallback
		allAnalyzedCoins = append(allAnalyzedCoins, coin)

		// Filter by minimum volume
		if totalVolume < minVolume {
			volumeFiltered++
			continue
		}

		// Filter by volatility
		if volatility <= 0 {
			volatilityFiltered++
			continue
		}

		// Add to ranked coins if it meets all criteria
		rankedCoins = append(rankedCoins, coin)
	}

	// Sort by score (descending)
	sort.Slice(rankedCoins, func(i, j int) bool {
		return rankedCoins[i].Score > rankedCoins[j].Score
	})

	// Debug logging
	c.logger.Debugf("FindScalpingCoins: secondaryAssets=%d, candlesFetched=%d, candlesInsufficient=%d, allAnalyzed=%d, volumeFiltered=%d, volatilityFiltered=%d, ranked=%d",
		len(secondaryAssets), candlesFetched, candlesInsufficient, len(allAnalyzedCoins), volumeFiltered, volatilityFiltered, len(rankedCoins))

	// If no coins were analyzed at all, this indicates a problem
	if len(allAnalyzedCoins) == 0 {
		errMsg := fmt.Sprintf("no coins could be analyzed: secondaryAssets=%d, candlesFetched=%d, candlesInsufficient=%d, errors=%d",
			len(secondaryAssets), candlesFetched, candlesInsufficient, errorCount)
		if firstError != nil {
			errMsg += fmt.Sprintf(" - first error: %v", firstError)
		}
		return nil, fmt.Errorf("%s - check if GetCandles is working correctly", errMsg)
	}

	// If no coins meet the strict criteria, fall back to best available coin
	if len(rankedCoins) == 0 && len(allAnalyzedCoins) > 0 {
		// Sort all analyzed coins by volume (fallback to volume if no volatility)
		sort.Slice(allAnalyzedCoins, func(i, j int) bool {
			// Prefer coins with both volume and volatility, but fall back to volume
			if allAnalyzedCoins[i].Volatility > 0 && allAnalyzedCoins[j].Volatility > 0 {
				return allAnalyzedCoins[i].Score > allAnalyzedCoins[j].Score
			}
			if allAnalyzedCoins[i].Volatility > 0 {
				return true
			}
			if allAnalyzedCoins[j].Volatility > 0 {
				return false
			}
			return allAnalyzedCoins[i].Volume > allAnalyzedCoins[j].Volume
		})
		// Return at least the best coin
		rankedCoins = []ScalpingCoin{allAnalyzedCoins[0]}
	}

	// Return top N
	if len(rankedCoins) > topN {
		rankedCoins = rankedCoins[:topN]
	}

	return rankedCoins, nil
}

// calculateTotalVolume sums the volume from all candles.
func calculateTotalVolume(candles []models.Candle) float64 {
	var total float64
	for _, candle := range candles {
		total += candle.Volume
	}
	return total
}

// calculateVolatility calculates the daily volatility as the standard deviation of log returns.
// Returns volatility as a percentage.
func calculateVolatility(candles []models.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	// Extract close prices
	prices := make([]float64, 0, len(candles))
	for _, candle := range candles {
		if candle.Close > 0 {
			prices = append(prices, candle.Close)
		}
	}

	if len(prices) < 2 {
		return 0
	}

	// Calculate log returns
	logReturns := make([]float64, 0, len(prices)-1)
	for i := 0; i < len(prices)-1; i++ {
		if prices[i] > 0 {
			logReturns = append(logReturns, math.Log(prices[i+1]/prices[i]))
		}
	}

	if len(logReturns) < 2 {
		return 0
	}

	// Calculate mean
	var sum float64
	for _, ret := range logReturns {
		sum += ret
	}
	mean := sum / float64(len(logReturns))

	// Calculate variance
	var variance float64
	for _, ret := range logReturns {
		diff := ret - mean
		variance += diff * diff
	}
	variance /= float64(len(logReturns))

	// Standard deviation (volatility) as percentage
	stdDev := math.Sqrt(variance) * 100

	return stdDev
}

// GetTicker returns ticker data for a symbol.
func (c *SwyftxClient) GetTicker(symbol string) (*models.Ticker, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	info, err := c.getBasicMarketInfo(ctx, baseAsset.Code)
	if err != nil {
		return nil, err
	}
	rate, err := c.callRate(ctx, baseAsset.Code, quoteAsset.Code, swyftxDefaultRateAmount)
	if err != nil {
		return nil, err
	}
	buyPrice := parseStringFloat(info.Buy)
	sellPrice := parseStringFloat(info.Sell)
	lastPrice := parseStringFloat(rate.Price)
	if lastPrice == 0 {
		// Fallback to mid-price if rate.Price is not available
		if buyPrice > 0 && sellPrice > 0 {
			lastPrice = (buyPrice + sellPrice) / 2
		} else if buyPrice > 0 {
			lastPrice = buyPrice
		} else if sellPrice > 0 {
			lastPrice = sellPrice
		}
	}
	// Validate that we have a valid last price
	if lastPrice <= 0 {
		return nil, fmt.Errorf("swyftx: unable to determine last price for %s (rate.Price=%s, buy=%s, sell=%s)", symbol, rate.Price, info.Buy, info.Sell)
	}
	return &models.Ticker{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		LastPrice: lastPrice,
		Volume:    info.Volume24H,
		Bid:       sellPrice,
		Ask:       buyPrice,
		Timestamp: time.Now(),
	}, nil
}

// FetchMarketData returns a simplified OHLC snapshot using public ticker endpoints.
func (c *SwyftxClient) FetchMarketData(symbol string) (models.MarketData, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return models.MarketData{}, err
	}
	info, err := c.getBasicMarketInfo(ctx, baseAsset.Code)
	if err != nil {
		return models.MarketData{}, err
	}
	rate, err := c.callRate(ctx, baseAsset.Code, quoteAsset.Code, swyftxDefaultRateAmount)
	if err != nil {
		return models.MarketData{}, err
	}

	lastPrice := parseStringFloat(rate.Price)
	buy := parseStringFloat(info.Buy)
	sell := parseStringFloat(info.Sell)
	if lastPrice == 0 {
		lastPrice = (buy + sell) / 2
	}

	high := lastPrice
	low := lastPrice
	if buy > 0 {
		if buy > high {
			high = buy
		}
		if low == 0 || buy < low {
			low = buy
		}
	}
	if sell > 0 {
		if sell > high {
			high = sell
		}
		if low == 0 || sell < low {
			low = sell
		}
	}

	price := lastPrice
	bid := sell
	ask := buy

	return models.MarketData{
		Symbol:    symbol,
		Timestamp: time.Now(),
		Open:      lastPrice,
		High:      high,
		Low:       low,
		Close:     lastPrice,
		Volume:    info.Volume24H,
		Price:     &price,
		Bid:       &bid,
		Ask:       &ask,
	}, nil
}

type swyftxBasicInfo struct {
	Code      string  `json:"code"`
	ID        int     `json:"id"`
	Buy       string  `json:"buy"`
	Sell      string  `json:"sell"`
	Volume24H float64 `json:"volume24H"`
}

func (c *SwyftxClient) getBasicMarketInfo(ctx context.Context, code string) (*swyftxBasicInfo, error) {
	path := fmt.Sprintf("/markets/info/basic/%s/", strings.ToUpper(code))
	data, err := c.doPublicRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var resp []swyftxBasicInfo
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, fmt.Errorf("swyftx: missing market info for %s", code)
	}
	return &resp[0], nil
}

func (c *SwyftxClient) callRate(ctx context.Context, buy, sell string, amount float64) (*swyftxRateResponse, error) {
	body := map[string]string{
		"buy":    strings.ToUpper(buy),
		"sell":   strings.ToUpper(sell),
		"amount": strconv.FormatFloat(amount, 'f', -1, 64),
		"limit":  strings.ToUpper(sell),
	}
	data, err := c.doRequest(ctx, http.MethodPost, "/orders/rate/", body, false)
	if err != nil {
		return nil, err
	}
	var resp swyftxRateResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// isAssetTradable checks if a base asset can be traded against a quote asset by attempting
// to fetch market info and rate data. Returns true if the asset is tradable, false otherwise.
func (c *SwyftxClient) isAssetTradable(ctx context.Context, baseCode, quoteCode string) bool {
	// Try to get basic market info first
	_, err := c.getBasicMarketInfo(ctx, baseCode)
	if err != nil {
		return false
	}
	// Try to call rate API to verify tradability
	_, err = c.callRate(ctx, baseCode, quoteCode, swyftxDefaultRateAmount)
	if err != nil {
		// Check if it's a non-tradable asset error
		if httpErr, ok := err.(*common.ExchangeError); ok {
			errMsg := strings.ToLower(httpErr.Message)
			// Check message and raw response for non-tradable indicators
			if strings.Contains(errMsg, "non-tradable") || strings.Contains(errMsg, "not tradable") {
				return false
			}
			// Also check raw response if available
			if len(httpErr.RawResponse) > 0 {
				rawMsg := strings.ToLower(string(httpErr.RawResponse))
				if strings.Contains(rawMsg, "non-tradable") || strings.Contains(rawMsg, "not tradable") {
					return false
				}
			}
			// For validation errors with "buy asset" in the message, assume non-tradable
			if httpErr.Type == common.ErrorTypeValidation && strings.Contains(errMsg, "buy asset") {
				return false
			}
		}
		return false
	}
	return true
}

type swyftxRateResponse struct {
	Price      string `json:"price"`
	Total      string `json:"total"`
	Amount     string `json:"amount"`
	FeePerUnit string `json:"feePerUnit"`
}

// GetOrderBook attempts to fetch the authenticated order book. If authentication
// fails, it falls back to a synthetic book derived from ticker data.
func (c *SwyftxClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/orderbook/%s/%s/", strings.ToUpper(baseAsset.Code), strings.ToUpper(quoteAsset.Code))
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return c.syntheticOrderBook(symbol, baseAsset, quoteAsset)
	}
	var resp swyftxOrderBookResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	bids := resp.Bids()
	asks := resp.Asks()
	if depth > 0 {
		if len(bids) > depth {
			bids = bids[:depth]
		}
		if len(asks) > depth {
			asks = asks[:depth]
		}
	}
	return &models.OrderBook{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now(),
	}, nil
}

func (c *SwyftxClient) syntheticOrderBook(symbol string, base, quote *swyftxAsset) (*models.OrderBook, error) {
	info, err := c.getBasicMarketInfo(context.Background(), base.Code)
	if err != nil {
		return nil, err
	}
	bidPrice := parseStringFloat(info.Sell)
	askPrice := parseStringFloat(info.Buy)
	return &models.OrderBook{
		Exchange: c.GetName(),
		Symbol:   symbol,
		Bids: []models.OrderBookEntry{{
			Price:  bidPrice,
			Amount: float64(base.MinOrder),
		}},
		Asks: []models.OrderBookEntry{{
			Price:  askPrice,
			Amount: float64(base.MinOrder),
		}},
		Timestamp: time.Now(),
	}, nil
}

type swyftxOrderBookResponse struct {
	Buy     []swyftxOrderBookEntry `json:"buy"`
	Sell    []swyftxOrderBookEntry `json:"sell"`
	BidsRaw []swyftxOrderBookEntry `json:"bids"`
	AsksRaw []swyftxOrderBookEntry `json:"asks"`
}

func (r swyftxOrderBookResponse) Bids() []models.OrderBookEntry {
	entries := r.Buy
	if len(entries) == 0 {
		entries = r.BidsRaw
	}
	return convertOrderBookEntries(entries)
}

func (r swyftxOrderBookResponse) Asks() []models.OrderBookEntry {
	entries := r.Sell
	if len(entries) == 0 {
		entries = r.AsksRaw
	}
	return convertOrderBookEntries(entries)
}

type swyftxOrderBookEntry struct {
	Price    string `json:"price"`
	Amount   string `json:"amount"`
	Quantity string `json:"quantity"`
	Total    string `json:"total"`
}

func convertOrderBookEntries(entries []swyftxOrderBookEntry) []models.OrderBookEntry {
	result := make([]models.OrderBookEntry, 0, len(entries))
	for _, entry := range entries {
		amount := parseStringFloat(entry.Amount)
		if amount == 0 {
			amount = parseStringFloat(entry.Quantity)
		}
		result = append(result, models.OrderBookEntry{
			Price:  parseStringFloat(entry.Price),
			Amount: amount,
		})
	}
	return result
}

// GetCandles fetches OHLCV data using the Swyftx charts v2 endpoint.
// Based on Swyftx OpenAPI spec: /charts/v2/getBars/{baseAsset}/{secondaryAsset}/{side}
// Parameters:
//   - baseAsset: Asset code (secondary asset/base)
//   - secondaryAsset: Asset code (primary asset/quote)
//   - side: "ask" or "bid"
//   - resolution: Time span for candles (e.g., "1m", "5m", "1h")
//   - timeStart: Start time (milliseconds, required)
//   - timeEnd: End time (milliseconds, required)
//   - limit: Number of candles (max 10000, optional)
func (c *SwyftxClient) GetCandles(symbol, interval string, since time.Time, limit int) ([]models.Candle, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	resolution, err := swyftxResolution(interval)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 500
	}
	if limit > 10000 {
		limit = 10000 // API maximum per OpenAPI spec
	}

	// Calculate timeStart and timeEnd
	timeStart := since
	if timeStart.IsZero() {
		// Default to 24 hours ago if not specified
		timeStart = time.Now().Add(-24 * time.Hour)
	}
	// Calculate timeEnd based on timeStart + (limit * resolution minutes)
	// This ensures the time range matches the requested number of candles
	calculatedEnd := timeStart.Add(time.Duration(limit) * time.Duration(resolution) * time.Minute)

	// Round down current time to the start of the current resolution period
	// Then go back one period to ensure we don't request incomplete candles
	now := time.Now()
	resolutionDuration := time.Duration(resolution) * time.Minute
	currentPeriodStart := now.Truncate(resolutionDuration)
	// Latest complete period end is one period before current period start
	latestCompleteEnd := currentPeriodStart

	// Use the earlier of calculatedEnd or latestCompleteEnd
	// This ensures we don't request incomplete candles
	var timeEnd time.Time
	if calculatedEnd.Before(latestCompleteEnd) || calculatedEnd.Equal(latestCompleteEnd) {
		timeEnd = calculatedEnd
	} else {
		timeEnd = latestCompleteEnd
	}

	// Ensure timeEnd is always strictly after timeStart (at least one resolution period)
	if !timeEnd.After(timeStart) {
		timeEnd = timeStart.Add(resolutionDuration)
	}

	// Build query parameters according to OpenAPI spec
	// Resolution should be string format: "1m", "5m", "1h", "4h", "1d"
	query := url.Values{}
	// Normalize interval to lowercase for API (e.g., "1m", "5m", "1h", "4h", "1d")
	resolutionStr := strings.ToLower(interval)
	if resolutionStr == "" {
		resolutionStr = "1m" // Default to 1 minute
	}
	query.Set("resolution", resolutionStr)
	query.Set("timeStart", strconv.FormatInt(timeStart.Unix()*1000, 10))
	query.Set("timeEnd", strconv.FormatInt(timeEnd.Unix()*1000, 10))
	// Limit is optional - API will return candles in the time range if limit is not specified
	// Only include limit if explicitly requested and within API limits
	if limit > 0 && limit <= 10000 {
		// Note: Some API implementations may prefer time range over limit, so we include it but API may ignore it
		query.Set("limit", strconv.Itoa(limit))
	}

	// Use the OpenAPI v2 endpoint: /charts/v2/getBars/{baseAsset}/{secondaryAsset}/{side}/
	// According to OpenAPI spec: baseAsset is the quote currency (e.g., AUD), secondaryAsset is the base currency (e.g., TRX)
	// This matches GetLatestBar and ResolveSymbol implementations
	// Note: URL format requires trailing slash after {side}
	path := fmt.Sprintf("/charts/v2/getBars/%s/%s/bid/?%s",
		strings.ToUpper(quoteAsset.Code), // baseAsset = quote currency (e.g., AUD)
		strings.ToUpper(baseAsset.Code),  // secondaryAsset = base currency (e.g., TRX)
		query.Encode())

	// Use chart request method with longer timeout
	// Chart endpoints require authentication per OpenAPI spec
	chartTimeout := 30 * time.Second
	token, err := c.getAccessToken(ctx)
	if err != nil {
		return nil, err
	}
	data, err := c.doRequestWithBaseAndTokenTimeout(ctx, http.MethodGet, path, nil, token, c.baseURL, chartTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch candles for %s: %w", symbol, err)
	}

	// Check if response is an error object first
	var errorResp struct {
		Error struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &errorResp); err == nil && errorResp.Error.Error != "" {
		return nil, fmt.Errorf("failed to fetch candles for %s: [validation:invalid_request] %s", symbol, string(data))
	}

	// API returns candles wrapped in an object: {"candles": [...]}
	var respWrapper struct {
		Candles []swyftxCandle `json:"candles"`
	}
	if err := json.Unmarshal(data, &respWrapper); err != nil {
		// Fallback: try to unmarshal as direct array (for backward compatibility)
		var resp []swyftxCandle
		if err2 := json.Unmarshal(data, &resp); err2 == nil {
			respWrapper.Candles = resp
		} else {
			// If unmarshaling fails, check if it's an empty object or different format
			var emptyObj map[string]interface{}
			if json.Unmarshal(data, &emptyObj) == nil && len(emptyObj) == 0 {
				// Empty response, return empty candles
				return []models.Candle{}, nil
			}
			return nil, fmt.Errorf("failed to parse candles response for %s: %w (response: %s)", symbol, err, string(data))
		}
	}
	resp := respWrapper.Candles
	candles := make([]models.Candle, 0, len(resp))
	for _, candle := range resp {
		openTime := time.UnixMilli(candle.Time)
		candles = append(candles, models.Candle{
			Exchange:  c.GetName(),
			Symbol:    symbol,
			Interval:  interval,
			OpenTime:  openTime,
			CloseTime: openTime.Add(time.Duration(resolution) * time.Minute),
			Open:      float64(candle.Open),
			High:      float64(candle.High),
			Low:       float64(candle.Low),
			Close:     float64(candle.Close),
			Volume:    candle.Volume,
		})
	}
	return candles, nil
}

// GetLatestBar fetches the latest candle/bar for a symbol using the Swyftx charts v2 endpoint.
// Based on Swyftx OpenAPI spec: /charts/v2/getLatestBar
// Parameters:
//   - symbol: Trading pair symbol (e.g., "BTC/AUD")
//   - interval: Time interval (e.g., "1m", "5m", "1h")
//   - side: "ask" or "bid" (defaults to "bid" if empty)
func (c *SwyftxClient) GetLatestBar(symbol, interval, side string) (*models.Candle, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	resolution, err := swyftxResolution(interval)
	if err != nil {
		return nil, err
	}
	if side == "" {
		side = "bid"
	}
	if side != "ask" && side != "bid" {
		return nil, fmt.Errorf("side must be 'ask' or 'bid', got: %s", side)
	}

	// Build query parameters according to OpenAPI spec
	query := url.Values{}
	query.Set("baseAsset", strings.ToUpper(quoteAsset.Code))     // baseAsset in API = quote
	query.Set("secondaryAsset", strings.ToUpper(baseAsset.Code)) // secondaryAsset in API = base
	query.Set("side", side)
	query.Set("resolution", strconv.Itoa(resolution))

	path := fmt.Sprintf("/charts/v2/getLatestBar?%s", query.Encode())
	// Use chart request method with longer timeout
	// Try public request first (chart data is typically public market data)
	// Fall back to authenticated request if public fails
	publicBase := c.publicBaseURL
	if publicBase == "" {
		publicBase = c.baseURL
	}
	chartTimeout := 30 * time.Second
	data, err := c.doRequestWithBaseAndTokenTimeout(ctx, http.MethodGet, path, nil, "", publicBase, chartTimeout)
	if err != nil {
		// Fallback to authenticated request if public request fails
		var token string
		token, err = c.getAccessToken(ctx)
		if err == nil {
			data, err = c.doRequestWithBaseAndTokenTimeout(ctx, http.MethodGet, path, nil, token, c.baseURL, chartTimeout)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch latest bar for %s: %w", symbol, err)
		}
	}
	var resp []swyftxCandle
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, fmt.Errorf("no latest bar data returned for %s", symbol)
	}
	candle := resp[0]
	openTime := time.UnixMilli(candle.Time)
	return &models.Candle{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		Interval:  interval,
		OpenTime:  openTime,
		CloseTime: openTime.Add(time.Duration(resolution) * time.Minute),
		Open:      float64(candle.Open),
		High:      float64(candle.High),
		Low:       float64(candle.Low),
		Close:     float64(candle.Close),
		Volume:    candle.Volume,
	}, nil
}

// flexibleFloat can unmarshal from both string and number
type flexibleFloat float64

func (f *flexibleFloat) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as number first
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		*f = flexibleFloat(num)
		return nil
	}
	// Try to unmarshal as string
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	parsed, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return err
	}
	*f = flexibleFloat(parsed)
	return nil
}

func (f flexibleFloat) String() string {
	return strconv.FormatFloat(float64(f), 'f', -1, 64)
}

// flexibleString can unmarshal from both string and number
type flexibleString string

func (s *flexibleString) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string first
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = flexibleString(str)
		return nil
	}
	// Try to unmarshal as number
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		*s = flexibleString(strconv.FormatFloat(num, 'f', -1, 64))
		return nil
	}
	// If both fail, return error
	return fmt.Errorf("cannot unmarshal %s into flexibleString", string(data))
}

func (s flexibleString) String() string {
	return string(s)
}

type swyftxCandle struct {
	Time   int64         `json:"time"`
	Open   flexibleFloat `json:"open"`
	High   flexibleFloat `json:"high"`
	Low    flexibleFloat `json:"low"`
	Close  flexibleFloat `json:"close"`
	Volume float64       `json:"volume"`
}

func swyftxResolution(interval string) (int, error) {
	if interval == "" {
		return 60, nil
	}
	unit := interval[len(interval)-1]
	value, err := strconv.Atoi(interval[:len(interval)-1])
	if err != nil {
		return 0, fmt.Errorf("invalid interval %s", interval)
	}
	switch unit {
	case 'm', 'M':
		return value, nil
	case 'h', 'H':
		return value * 60, nil
	case 'd', 'D':
		return value * 60 * 24, nil
	default:
		return 0, fmt.Errorf("unsupported interval unit %c", unit)
	}
}

// GetTrades returns historical trade-like records derived from the asset history endpoint.
// Supports pagination via limit and page query parameters per Swyftx API documentation.
// Pagination format: ?limit={resultsPerPage}&page={pageNumber}
// Page numbers are one-based (page=1 is the first page).
func (c *SwyftxClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	return c.GetTradesWithPagination(symbol, since, limit, 0)
}

// GetTradesWithPagination returns historical trade-like records with pagination support.
// page parameter: 0 or negative means no pagination, positive values are one-based (page 1 is first page).
func (c *SwyftxClient) GetTradesWithPagination(symbol string, since time.Time, limit, page int) ([]models.Trade, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	if since.Unix() > 0 {
		query.Set("startDate", strconv.FormatInt(since.Unix()*1000, 10))
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
	query.Set("type", "TRADE")
	query.Set("sortDirection", "desc")
	query.Set("sortKey", "date")
	path := fmt.Sprintf("/portfolio/assetHistory/%d/?%s", baseAsset.ID, query.Encode())
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp swyftxAssetHistoryResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	trades := make([]models.Trade, 0, len(resp.Items))
	for _, item := range resp.Items {
		if quoteAsset.ID != 0 && item.PrimaryAsset != quoteAsset.ID {
			continue
		}
		secondaryAmount := parseStringFloat(item.SecondaryAmount)
		primaryAmount := parseStringFloat(item.PrimaryAmount)
		if secondaryAmount == 0 || primaryAmount == 0 {
			continue
		}
		price := primaryAmount / secondaryAmount
		tradeTime := time.UnixMilli(item.Date)
		if !since.IsZero() && tradeTime.Before(since) {
			continue
		}
		trades = append(trades, models.Trade{
			Exchange:      c.GetName(),
			Symbol:        symbol,
			ID:            item.UUID,
			Price:         price,
			Quantity:      secondaryAmount,
			Fee:           parseStringFloat(item.FeeAmount),
			ExecutionTime: tradeTime,
			OrderID:       item.UUID,
			Type:          strings.ToLower(item.Type),
		})
		if limit > 0 && len(trades) >= limit {
			break
		}
	}
	slices.Reverse(trades)
	return trades, nil
}

type swyftxAssetHistoryResponse struct {
	Items []struct {
		UUID            string `json:"uuid"`
		Date            int64  `json:"date"`
		Type            string `json:"type"`
		PrimaryAsset    int    `json:"primaryAsset"`
		SecondaryAsset  int    `json:"secondaryAsset"`
		PrimaryAmount   string `json:"primaryAmount"`
		SecondaryAmount string `json:"secondaryAmount"`
		FeeAmount       string `json:"feeAmount"`
	} `json:"items"`
}

// GetBalance returns balance for a single currency.
func (c *SwyftxClient) GetBalance(currency string) (*common.Balance, error) {
	balances, err := c.GetBalances()
	if err != nil {
		return nil, err
	}
	bal, ok := balances[strings.ToUpper(currency)]
	if !ok {
		return nil, fmt.Errorf("balance for %s not found", currency)
	}
	return bal, nil
}

// GetBalances returns all balances.
func (c *SwyftxClient) GetBalances() (map[string]*common.Balance, error) {
	ctx := context.Background()
	if err := c.ensureAssets(ctx); err != nil {
		return nil, err
	}
	data, err := c.doRequest(ctx, http.MethodGet, "/user/balance/", nil, true)
	if err != nil {
		return nil, err
	}
	var resp []struct {
		AssetID          int    `json:"assetId"`
		AvailableBalance string `json:"availableBalance"`
		LockedBalance    string `json:"lockedBalance"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	c.assetMu.RLock()
	defer c.assetMu.RUnlock()
	balances := make(map[string]*common.Balance, len(resp))
	for _, entry := range resp {
		asset := c.assetsByID[entry.AssetID]
		if asset == nil {
			continue
		}
		balances[asset.Code] = &common.Balance{
			Asset:  asset.Code,
			Free:   entry.AvailableBalance,
			Locked: entry.LockedBalance,
		}
	}
	return balances, nil
}

// CreateOrder submits a new order using the /orders/ endpoint.
func (c *SwyftxClient) CreateOrder(symbol string, side common.OrderSide, orderType common.OrderType, amount, price float64) (*common.Order, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	orderTypeID, err := mapSwyftxOrderType(side, orderType)
	if err != nil {
		return nil, err
	}
	quantity := amount
	assetQuantity := baseAsset.Code
	if side == common.OrderSideBuy {
		assetQuantity = quoteAsset.Code
	}
	// orderType must be an integer, not a string, per Swyftx API specification
	body := map[string]interface{}{
		"primary":       strings.ToUpper(quoteAsset.Code),
		"secondary":     strings.ToUpper(baseAsset.Code),
		"quantity":      strconv.FormatFloat(quantity, 'f', -1, 64),
		"assetQuantity": strings.ToUpper(assetQuantity),
		"orderType":     orderTypeID, // Send as integer, not string
	}
	// Only include trigger for LIMIT (3, 4) and STOP_LIMIT (5, 6) orders
	// MARKET orders (1, 2) should not include trigger
	if price > 0 && (orderTypeID == 3 || orderTypeID == 4 || orderTypeID == 5 || orderTypeID == 6) {
		body["trigger"] = strconv.FormatFloat(price, 'f', baseAsset.PriceScale, 64)
	}
	data, err := c.doRequest(ctx, http.MethodPost, "/orders/", body, true)
	if err != nil {
		return nil, err
	}
	var resp swyftxOrderEnvelope
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return convertSwyftxOrder(symbol, &resp.Order)
}

func mapSwyftxOrderType(side common.OrderSide, orderType common.OrderType) (int, error) {
	switch orderType {
	case common.OrderTypeMarket:
		if side == common.OrderSideBuy {
			return 1, nil
		}
		return 2, nil
	case common.OrderTypeLimit:
		if side == common.OrderSideBuy {
			return 3, nil
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("unsupported order type %s", orderType)
	}
}

type swyftxOrderEnvelope struct {
	OrderUUID string          `json:"orderUuid"`
	Order     swyftxOrderBody `json:"order"`
}

type swyftxOrderBody struct {
	OrderUUID      string         `json:"orderUuid"`
	OrderType      int            `json:"order_type"`
	PrimaryAsset   int            `json:"primary_asset"`
	SecondaryAsset int            `json:"secondary_asset"`
	QuantityAsset  int            `json:"quantity_asset"`
	Quantity       flexibleString `json:"quantity"`
	Trigger        flexibleString `json:"trigger"`
	Status         int            `json:"status"`
	Created        int64          `json:"created_time"`
	Updated        int64          `json:"updated_time"`
	Amount         flexibleString `json:"amount"`
	Total          flexibleString `json:"total"`
	Rate           flexibleString `json:"rate"`
	FeeAmount      flexibleString `json:"feeAmount"`
	FeeAsset       string         `json:"feeAsset"`
}

func convertSwyftxOrder(symbol string, body *swyftxOrderBody) (*common.Order, error) {
	if body == nil {
		return nil, errors.New("swyftx: empty order body")
	}
	price := parseStringFloat(body.Rate.String())
	amount := parseStringFloat(body.Quantity.String())
	status := mapSwyftxOrderStatus(body.Status)
	return &common.Order{
		ID:              body.OrderUUID,
		Symbol:          symbol,
		Side:            mapSwyftxOrderSide(body.OrderType),
		Type:            mapSwyftxOrderTypeToGeneric(body.OrderType),
		Status:          status,
		Price:           price,
		Amount:          amount,
		FilledAmount:    parseStringFloat(body.Amount.String()),
		RemainingAmount: amount - parseStringFloat(body.Amount.String()),
		Fee:             parseStringFloat(body.FeeAmount.String()),
		FeeCurrency:     body.FeeAsset,
		CreatedAt:       time.UnixMilli(body.Created),
		UpdatedAt:       time.UnixMilli(body.Updated),
		Quantity:        amount,
		Timestamp:       time.UnixMilli(body.Created),
		ClientOrderID:   body.OrderUUID,
	}, nil
}

func mapSwyftxOrderStatus(status int) common.OrderStatus {
	switch status {
	case 1, 5:
		return common.OrderStatusNew
	case 2, 6, 8:
		return common.OrderStatusCancelled
	case 3:
		return common.OrderStatusPartiallyFilled
	case 4:
		return common.OrderStatusFilled
	case 7, 9:
		return common.OrderStatusRejected
	default:
		return common.OrderStatusNew
	}
}

func mapSwyftxOrderSide(orderType int) common.OrderSide {
	switch orderType {
	case 1, 3, 5:
		return common.OrderSideBuy
	case 2, 4, 6:
		return common.OrderSideSell
	default:
		return common.OrderSideBuy
	}
}

func mapSwyftxOrderTypeToGeneric(orderType int) common.OrderType {
	switch orderType {
	case 1, 2:
		return common.OrderTypeMarket
	case 3, 4:
		return common.OrderTypeLimit
	case 5, 6:
		return common.OrderTypeStopLimit
	default:
		return common.OrderTypeMarket
	}
}

// GetOrder retrieves a specific order by UUID.
func (c *SwyftxClient) GetOrder(symbol, orderID string) (*common.Order, error) {
	path := fmt.Sprintf("/orders/byId/%s", orderID)
	data, err := c.doRequest(context.Background(), http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var body swyftxOrderBody
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	return convertSwyftxOrder(symbol, &body)
}

// GetOrderStatus retrieves the status of an order by ID.
// This is more efficient than GetOrders for checking a single order status.
func (c *SwyftxClient) GetOrderStatus(symbol, orderID string) (common.OrderStatus, error) {
	order, err := c.GetOrder(symbol, orderID)
	if err != nil {
		return "", err
	}
	return order.Status, nil
}

// GetOrders returns orders for a symbol.
// Supports pagination via limit and page query parameters per Swyftx API documentation.
// Pagination format: ?limit={resultsPerPage}&page={pageNumber}
// Page numbers are one-based (page=1 is the first page).
func (c *SwyftxClient) GetOrders(symbol string, since time.Time, limit int) ([]common.Order, error) {
	return c.GetOrdersWithPagination(symbol, since, limit, 0)
}

// GetOrdersWithPagination returns orders for a symbol with pagination support.
// page parameter: 0 or negative means no pagination, positive values are one-based (page 1 is first page).
func (c *SwyftxClient) GetOrdersWithPagination(symbol string, since time.Time, limit, page int) ([]common.Order, error) {
	ctx := context.Background()
	baseAsset, _, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
	path := fmt.Sprintf("/orders/%s?%s", strings.ToUpper(baseAsset.Code), query.Encode())
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp []swyftxOrderBody
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	orders := make([]common.Order, 0, len(resp))
	for _, entry := range resp {
		order, err := convertSwyftxOrder(symbol, &entry)
		if err != nil {
			continue
		}
		if !since.IsZero() && order.CreatedAt.Before(since) {
			continue
		}
		orders = append(orders, *order)
	}
	return orders, nil
}

// CancelOrder cancels an order by UUID.
func (c *SwyftxClient) CancelOrder(symbol, orderID string) error {
	path := fmt.Sprintf("/orders/%s/", orderID)
	_, err := c.doRequest(context.Background(), http.MethodDelete, path, nil, true)
	return err
}

// CancelAllOrders attempts to cancel all open orders for the supplied symbol.
func (c *SwyftxClient) CancelAllOrders(symbol string) error {
	orders, err := c.GetOrders(symbol, time.Time{}, 200)
	if err != nil {
		return err
	}
	for _, order := range orders {
		if order.Status == common.OrderStatusFilled || order.Status == common.OrderStatusCancelled {
			continue
		}
		if cancelErr := c.CancelOrder(symbol, order.ID); cancelErr != nil {
			return cancelErr
		}
	}
	return nil
}

func parseStringFloat(value string) float64 {
	if value == "" {
		return 0
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return f
}

// GetUserProfile retrieves the user's profile information.
// Based on Swyftx OpenAPI spec: GET /user
func (c *SwyftxClient) GetUserProfile() (*SwyftxUserProfile, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/user", nil, true)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Profile SwyftxUserProfile `json:"profile"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp.Profile, nil
}

// SwyftxUserProfile represents user profile information from the Swyftx API.
type SwyftxUserProfile struct {
	DOB  int64 `json:"dob"`
	Name struct {
		First string `json:"first"`
		Last  string `json:"last"`
	} `json:"name"`
	Email    string `json:"email"`
	Phone    string `json:"phone"`
	Currency struct {
		ID   int    `json:"id"`
		Code string `json:"code"`
	} `json:"currency"`
	UserHash string                 `json:"user_hash"`
	Metadata map[string]interface{} `json:"metadata"`
	Settings map[string]interface{} `json:"userSettings"`
}

// GetUserCurrency retrieves the user's default currency.
// Based on Swyftx OpenAPI spec: GET /user (returns currency in profile)
func (c *SwyftxClient) GetUserCurrency() (string, error) {
	profile, err := c.GetUserProfile()
	if err != nil {
		return "", err
	}
	return profile.Currency.Code, nil
}

// GetUserFees retrieves general order fees.
// Based on Swyftx OpenAPI spec: GET /user/fees
func (c *SwyftxClient) GetUserFees() (float64, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/user/fees", nil, true)
	if err != nil {
		return 0, err
	}
	var resp struct {
		GeneralOrderFee string `json:"generalOrderFee"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, err
	}
	return parseStringFloat(resp.GeneralOrderFee), nil
}

// GetOrderFee retrieves the order fee for a specific trading pair and direction.
// Based on Swyftx OpenAPI spec: GET /user/fees/{primary}/{secondary}/{direction}
// Parameters:
//   - symbol: Trading pair symbol (e.g., "BTC/AUD")
//   - direction: "BUY" or "SELL"
func (c *SwyftxClient) GetOrderFee(symbol, direction string) (float64, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return 0, err
	}
	direction = strings.ToUpper(direction)
	if direction != "BUY" && direction != "SELL" {
		return 0, fmt.Errorf("direction must be 'BUY' or 'SELL', got: %s", direction)
	}
	path := fmt.Sprintf("/user/fees/%d/%d/%s", quoteAsset.ID, baseAsset.ID, direction)
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return 0, err
	}
	var resp struct {
		OrderFee string `json:"orderFee"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, err
	}
	return parseStringFloat(resp.OrderFee), nil
}

// GetMarketDetail retrieves detailed market information for an asset.
// Based on Swyftx OpenAPI spec: GET /markets/info/detail/{assetCode}
func (c *SwyftxClient) GetMarketDetail(assetCode string) (*SwyftxMarketDetail, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/markets/info/detail/%s", strings.ToUpper(assetCode))
	data, err := c.doPublicRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var resp []SwyftxMarketDetail
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, fmt.Errorf("no market detail found for %s", assetCode)
	}
	return &resp[0], nil
}

// SwyftxMarketDetail represents detailed market information from the Swyftx API.
type SwyftxMarketDetail struct {
	Name        string `json:"name"`
	ID          int    `json:"id"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Mineable    int    `json:"mineable"`
	Spread      string `json:"spread"`
	Rank        int    `json:"rank"`
	RankSuffix  string `json:"rankSuffix"`
	Volume      struct {
		Volume24H float64 `json:"24H"`
		Volume1W  float64 `json:"1W"`
		Volume1M  float64 `json:"1M"`
		MarketCap float64 `json:"marketCap"`
	} `json:"volume"`
	URLs struct {
		Website  string `json:"website"`
		Twitter  string `json:"twitter"`
		Reddit   string `json:"reddit"`
		TechDoc  string `json:"techDoc"`
		Explorer string `json:"explorer"`
	} `json:"urls"`
	Supply struct {
		Circulating float64 `json:"circulating"`
		Total       float64 `json:"total"`
		Max         float64 `json:"max"`
	} `json:"supply"`
}

// ============================================================================
// Address Management Methods
// ============================================================================

// GetDepositAddress retrieves active deposit addresses for an asset.
// Based on Swyftx OpenAPI spec: GET /address/deposit
func (c *SwyftxClient) GetDepositAddress(assetID string, networkID int) (map[string]interface{}, error) {
	ctx := context.Background()
	query := url.Values{}
	query.Set("assetId", assetID)
	query.Set("networkId", strconv.Itoa(networkID))
	path := fmt.Sprintf("/address/deposit?%s", query.Encode())
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// CreateWithdrawAddress creates a new withdrawal address for an asset.
// Based on Swyftx OpenAPI spec: POST /address/withdraw/{assetCode}
func (c *SwyftxClient) CreateWithdrawAddress(assetCode string, address map[string]interface{}) (map[string]interface{}, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/address/withdraw/%s", strings.ToUpper(assetCode))
	body := map[string]interface{}{"address": address}
	data, err := c.doRequest(ctx, http.MethodPost, path, body, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// RemoveWithdrawAddress removes a withdrawal address.
// Based on Swyftx OpenAPI spec: POST /address/withdraw/{addressId}
func (c *SwyftxClient) RemoveWithdrawAddress(addressID int) error {
	ctx := context.Background()
	path := fmt.Sprintf("/address/withdraw/%d", addressID)
	_, err := c.doRequest(ctx, http.MethodPost, path, nil, true)
	return err
}

// VerifyWithdrawAddress verifies a withdrawal address using a token from email.
// Based on Swyftx OpenAPI spec: POST /address/withdraw/verify/{token}
func (c *SwyftxClient) VerifyWithdrawAddress(token string) error {
	ctx := context.Background()
	path := fmt.Sprintf("/address/withdraw/verify/%s", token)
	_, err := c.doRequest(ctx, http.MethodPost, path, nil, true)
	return err
}

// VerifyBSB verifies a BSB (Bank State Branch) number.
// Based on Swyftx OpenAPI spec: GET /address/withdraw/bsb-verify/{bsb}
func (c *SwyftxClient) VerifyBSB(bsb string) (map[string]interface{}, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/address/withdraw/bsb-verify/%s", bsb)
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ============================================================================
// Withdrawal Methods
// ============================================================================

// RequestWithdrawal requests a withdrawal of funds.
// Based on Swyftx OpenAPI spec: GET /funds/withdraw/{assetCode}
func (c *SwyftxClient) RequestWithdrawal(assetCode string, quantity string, addressID int, reason interface{}) (map[string]interface{}, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/funds/withdraw/%s", strings.ToUpper(assetCode))
	body := map[string]interface{}{
		"quantity":   quantity,
		"address_id": addressID,
	}
	if reason != nil {
		body["reason"] = reason
	}
	data, err := c.doRequest(ctx, http.MethodGet, path, body, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWithdrawalPermissions checks withdrawal permissions for an asset.
// Based on Swyftx OpenAPI spec: GET /funds/withdrawalPermissions/{assetCode}
func (c *SwyftxClient) GetWithdrawalPermissions(assetCode string) (*SwyftxWithdrawalPermissions, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/funds/withdrawalPermissions/%s", strings.ToUpper(assetCode))
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp SwyftxWithdrawalPermissions
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SwyftxWithdrawalPermissions represents withdrawal permissions from the Swyftx API.
type SwyftxWithdrawalPermissions struct {
	CanWithdraw bool   `json:"canWithdraw"`
	Message     string `json:"message"`
}

// GetWithdrawalLimits retrieves withdrawal limits.
// Based on Swyftx OpenAPI spec: GET /limits/withdrawal
func (c *SwyftxClient) GetWithdrawalLimits() (*SwyftxWithdrawalLimits, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/limits/withdrawal", nil, true)
	if err != nil {
		return nil, err
	}
	var resp SwyftxWithdrawalLimits
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SwyftxWithdrawalLimits represents withdrawal limits from the Swyftx API.
type SwyftxWithdrawalLimits struct {
	Used            float64 `json:"used"`
	Remaining       float64 `json:"remaining"`
	Limit           float64 `json:"limit"`
	RollingCycleHrs int     `json:"rollingCycleHrs"`
}

// ============================================================================
// History Methods
// ============================================================================

// GetDepositHistory retrieves deposit history for an asset.
// Based on Swyftx OpenAPI spec: GET /history/deposit/{asset}
func (c *SwyftxClient) GetDepositHistory(asset string, limit, page int) ([]map[string]interface{}, error) {
	ctx := context.Background()
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
	path := fmt.Sprintf("/history/deposit/%s", strings.ToUpper(asset))
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp []map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWithdrawHistory retrieves withdrawal history for an asset.
// Based on Swyftx OpenAPI spec: GET /history/withdraw/{asset}
func (c *SwyftxClient) GetWithdrawHistory(asset string, limit, page int) ([]map[string]interface{}, error) {
	ctx := context.Background()
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
	path := fmt.Sprintf("/history/withdraw/%s", strings.ToUpper(asset))
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp []map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetAllHistory retrieves all history for a specific type and asset.
// Based on Swyftx OpenAPI spec: GET /history/all/{type}/{assetId}
func (c *SwyftxClient) GetAllHistory(historyType string, assetID int, limit, page int) ([]map[string]interface{}, error) {
	ctx := context.Background()
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if page > 0 {
		query.Set("page", strconv.Itoa(page))
	}
	path := fmt.Sprintf("/history/all/%s/%d", historyType, assetID)
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp []map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ============================================================================
// Rate Methods
// ============================================================================

// GetMultiRates retrieves exchange rates for multiple pairs.
// Based on Swyftx OpenAPI spec: POST /orders/rate/multi
func (c *SwyftxClient) GetMultiRates(rateRequests []map[string]string) ([]map[string]interface{}, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodPost, "/orders/rate/multi", rateRequests, false)
	if err != nil {
		return nil, err
	}
	var resp []map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetSwapRate retrieves the exchange rate for a swap pair.
// Based on Swyftx OpenAPI spec: POST /orders/rate/swap
func (c *SwyftxClient) GetSwapRate(buy, sell, amount, limit string, intermediary string) (*swyftxRateResponse, error) {
	ctx := context.Background()
	body := map[string]string{
		"buy":    strings.ToUpper(buy),
		"sell":   strings.ToUpper(sell),
		"amount": amount,
		"limit":  strings.ToUpper(limit),
	}
	if intermediary != "" {
		body["intermediary"] = strings.ToUpper(intermediary)
	}
	data, err := c.doRequest(ctx, http.MethodPost, "/orders/rate/swap", body, false)
	if err != nil {
		return nil, err
	}
	var resp swyftxRateResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ExecuteSwap executes a swap between two assets.
// Based on Swyftx OpenAPI spec: POST /swap
func (c *SwyftxClient) ExecuteSwap(buy, sell string, limitAsset int, limitQty string, intermediateAssetID string) (map[string]interface{}, error) {
	ctx := context.Background()
	body := map[string]interface{}{
		"buy":        buy,
		"sell":       sell,
		"limitAsset": limitAsset,
		"limitQty":   limitQty,
	}
	if intermediateAssetID != "" {
		body["intermediateAssetId"] = intermediateAssetID
	}
	data, err := c.doRequest(ctx, http.MethodPost, "/swap", body, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ============================================================================
// User Management Methods
// ============================================================================

// GetAPIKeys retrieves all API keys for the user.
// Based on Swyftx OpenAPI spec: GET /user/apiKeys
func (c *SwyftxClient) GetAPIKeys() ([]SwyftxAPIKey, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/user/apiKeys", nil, true)
	if err != nil {
		return nil, err
	}
	var resp []SwyftxAPIKey
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SwyftxAPIKey represents an API key from the Swyftx API.
type SwyftxAPIKey struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	Scope   string `json:"scope"`
	Created string `json:"created"`
}

// RevokeAPIKey revokes a specific API key.
// Based on Swyftx OpenAPI spec: POST /user/apiKeys/revoke
func (c *SwyftxClient) RevokeAPIKey(keyID string) error {
	ctx := context.Background()
	body := map[string]string{"keyId": keyID}
	_, err := c.doRequest(ctx, http.MethodPost, "/user/apiKeys/revoke", body, true)
	return err
}

// RevokeAllAPIKeys revokes all API keys.
// Based on Swyftx OpenAPI spec: POST /user/apiKeys/revokeAll
func (c *SwyftxClient) RevokeAllAPIKeys() error {
	ctx := context.Background()
	_, err := c.doRequest(ctx, http.MethodPost, "/user/apiKeys/revokeAll", nil, true)
	return err
}

// GetAPIKeyScopes retrieves available API key scopes.
// Based on Swyftx OpenAPI spec: GET /user/apiKeys/scope
func (c *SwyftxClient) GetAPIKeyScopes() (map[string]interface{}, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/user/apiKeys/scope", nil, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateUserSettings updates user account settings.
// Based on Swyftx OpenAPI spec: POST /user/settings
func (c *SwyftxClient) UpdateUserSettings(settings map[string]interface{}) (*SwyftxUserProfile, error) {
	ctx := context.Background()
	body := map[string]interface{}{"data": settings}
	data, err := c.doRequest(ctx, http.MethodPost, "/user/settings", body, true)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Profile SwyftxUserProfile `json:"profile"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp.Profile, nil
}

// GetUserStatistics retrieves user statistics.
// Based on Swyftx OpenAPI spec: GET /user/statistics
func (c *SwyftxClient) GetUserStatistics() (*SwyftxUserStatistics, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/user/statistics", nil, true)
	if err != nil {
		return nil, err
	}
	var resp SwyftxUserStatistics
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SwyftxUserStatistics represents user statistics from the Swyftx API.
type SwyftxUserStatistics struct {
	Orders    int     `json:"orders"`
	Traded    float64 `json:"traded"`
	Deposited float64 `json:"deposited"`
	Withdrawn float64 `json:"withdrawn"`
}

// GetUserAffiliations retrieves user affiliation information.
// Based on Swyftx OpenAPI spec: GET /user/affiliations
func (c *SwyftxClient) GetUserAffiliations(forceRefresh bool) (*SwyftxUserAffiliations, error) {
	ctx := context.Background()
	query := url.Values{}
	if forceRefresh {
		query.Set("forceRefresh", "true")
	}
	path := "/user/affiliations"
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp SwyftxUserAffiliations
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SwyftxUserAffiliations represents user affiliations from the Swyftx API.
type SwyftxUserAffiliations struct {
	ReferralLink       string  `json:"referral_link"`
	ReferredUsers      int     `json:"referred_users"`
	OutstandingBalance float64 `json:"outstandingBalance"`
}

// ConvertDust converts small balances (dust) to a primary asset.
// Based on Swyftx OpenAPI spec: POST /user/balance/dust
func (c *SwyftxClient) ConvertDust(selectedAssetIDs []int, primaryAssetID int) ([]map[string]interface{}, error) {
	ctx := context.Background()
	body := map[string]interface{}{
		"selected": selectedAssetIDs,
		"primary":  primaryAssetID,
	}
	data, err := c.doRequest(ctx, http.MethodPost, "/user/balance/dust", body, true)
	if err != nil {
		return nil, err
	}
	var resp []map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SetUserCurrency sets the user's default currency.
// Based on Swyftx OpenAPI spec: POST /user/currency
func (c *SwyftxClient) SetUserCurrency(assetID int) (*SwyftxUserProfile, error) {
	ctx := context.Background()
	body := map[string]interface{}{
		"profile": map[string]interface{}{
			"defaultAsset": assetID,
		},
	}
	data, err := c.doRequest(ctx, http.MethodPost, "/user/currency", body, true)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Profile SwyftxUserProfile `json:"profile"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp.Profile, nil
}

// ============================================================================
// Portfolio Methods
// ============================================================================

// GetTradePriceHistory retrieves trade price history for an asset.
// Based on Swyftx OpenAPI spec: GET /portfolio/tradePriceHistory/{denotedAssetId}
func (c *SwyftxClient) GetTradePriceHistory(denotedAssetID string) (map[string]interface{}, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/portfolio/tradePriceHistory/%s", denotedAssetID)
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetAllAssetHistory retrieves all asset history with pagination.
// Based on Swyftx OpenAPI spec: GET /portfolio/assetHistory/all
func (c *SwyftxClient) GetAllAssetHistory(page, limit int, sortKey, sortDirection string) ([]map[string]interface{}, error) {
	ctx := context.Background()
	query := url.Values{}
	query.Set("page", strconv.Itoa(page))
	query.Set("limit", strconv.Itoa(limit))
	if sortKey != "" {
		query.Set("sortKey", sortKey)
	}
	if sortDirection != "" {
		query.Set("sortDirection", sortDirection)
	}
	path := fmt.Sprintf("/portfolio/assetHistory/all?%s", query.Encode())
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp []map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ============================================================================
// Utility Methods
// ============================================================================

// GetLiveRates retrieves live rates for all assets in a specific currency.
// Based on Swyftx OpenAPI spec: GET /live-rates/{asset}
func (c *SwyftxClient) GetLiveRates(assetID string) (map[string]interface{}, error) {
	ctx := context.Background()
	path := fmt.Sprintf("/live-rates/%s", assetID)
	data, err := c.doPublicRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetChartSettings retrieves chart settings.
// Based on Swyftx OpenAPI spec: GET /charts/v2/settings
func (c *SwyftxClient) GetChartSettings() (map[string]interface{}, error) {
	ctx := context.Background()
	data, err := c.doRequest(ctx, http.MethodGet, "/charts/v2/settings", nil, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ResolveSymbol resolves a symbol and returns symbol information.
// Based on Swyftx OpenAPI spec: GET /charts/v2/resolveSymbol/{baseAsset}/{secondaryAsset}
func (c *SwyftxClient) ResolveSymbol(symbol string) (map[string]interface{}, error) {
	ctx := context.Background()
	baseAsset, quoteAsset, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/charts/v2/resolveSymbol/%s/%s",
		strings.ToUpper(quoteAsset.Code), // baseAsset in API = quote
		strings.ToUpper(baseAsset.Code))  // secondaryAsset in API = base
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Logout logs out the current session.
// Based on Swyftx OpenAPI spec: POST /auth/logout
func (c *SwyftxClient) Logout() error {
	ctx := context.Background()
	_, err := c.doRequest(ctx, http.MethodPost, "/auth/logout", nil, true)
	if err != nil {
		return err
	}
	// Clear cached token on logout
	c.tokenMu.Lock()
	c.accessToken = ""
	c.tokenExpiry = time.Time{}
	c.tokenRefreshedAt = time.Time{}
	c.tokenMu.Unlock()
	return nil
}
