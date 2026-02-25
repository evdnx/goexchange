package exchange

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	common "github.com/evdnx/goexchange/common"
	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/gohttpcl"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

const (
	irHTTPTimeout   = 30 * time.Second
	irBaseURL       = "https://api.independentreserve.com"
	irAssetCacheTTL = 30 * time.Minute
	irUserAgent     = "GoExchangeClient/1.0"
)

// IndependentReserveClient implements the ExchangeClient interface for Independent Reserve exchange.
// Independent Reserve is an Australian cryptocurrency exchange supporting AUD, USD, NZD, and SGD trading pairs.
type IndependentReserveClient struct {
	*common.BaseClient
	baseURL     string
	httpClient  *gohttpcl.Client
	metrics     *metrics.Metrics
	logger      *golog.Logger
	userAgent   string
	httpTimeout time.Duration

	// Nonce management for authenticated requests
	nonceMu   sync.Mutex
	lastNonce int64

	// Trading pair cache
	pairsMu        sync.RWMutex
	pairs          []common.TradingPair
	pairsFetched   time.Time
	primaryCodes   []string
	secondaryCodes []string
}

// irMarketSummary represents the market summary response from Independent Reserve.
type irMarketSummary struct {
	DayHighestPrice             float64 `json:"DayHighestPrice"`
	DayLowestPrice              float64 `json:"DayLowestPrice"`
	DayAvgPrice                 float64 `json:"DayAvgPrice"`
	DayVolumeXbt                float64 `json:"DayVolumeXbt"`
	DayVolumeXbtInSecondaryCurr float64 `json:"DayVolumeXbtInSecondaryCurrrency"`
	CurrentLowestOfferPrice     float64 `json:"CurrentLowestOfferPrice"`
	CurrentHighestBidPrice      float64 `json:"CurrentHighestBidPrice"`
	LastPrice                   float64 `json:"LastPrice"`
	PrimaryCurrencyCode         string  `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode       string  `json:"SecondaryCurrencyCode"`
	CreatedTimestampUtc         string  `json:"CreatedTimestampUtc"`
}

// irOrderBook represents the order book response from Independent Reserve.
type irOrderBook struct {
	BuyOrders             []irOrderBookEntry `json:"BuyOrders"`
	SellOrders            []irOrderBookEntry `json:"SellOrders"`
	CreatedTimestampUtc   string             `json:"CreatedTimestampUtc"`
	PrimaryCurrencyCode   string             `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode string             `json:"SecondaryCurrencyCode"`
}

// irOrderBookEntry represents an entry in the order book.
type irOrderBookEntry struct {
	OrderType string  `json:"OrderType"`
	Price     float64 `json:"Price"`
	Volume    float64 `json:"Volume"`
}

// irRecentTrades represents the recent trades response from Independent Reserve.
type irRecentTrades struct {
	CreatedTimestampUtc   string    `json:"CreatedTimestampUtc"`
	PrimaryCurrencyCode   string    `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode string    `json:"SecondaryCurrencyCode"`
	Trades                []irTrade `json:"Trades"`
}

// irTrade represents a single trade.
type irTrade struct {
	TradeGuid                   string  `json:"TradeGuid"`
	Taker                       string  `json:"Taker"`
	PrimaryCurrencyAmount       float64 `json:"PrimaryCurrencyAmount"`
	SecondaryCurrencyTradePrice float64 `json:"SecondaryCurrencyTradePrice"`
	TradeTimestampUtc           string  `json:"TradeTimestampUtc"`
}

// irHistorySummary represents the trade history summary response.
type irHistorySummary struct {
	CreatedTimestampUtc              string                 `json:"CreatedTimestampUtc "`
	HistorySummaryItems              []irHistorySummaryItem `json:"HistorySummaryItems"`
	NumberOfHoursInThePastToRetrieve int                    `json:"NumberOfHoursInThePastToRetrieve"`
	PrimaryCurrencyCode              string                 `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode            string                 `json:"SecondaryCurrencyCode"`
}

// irHistorySummaryItem represents a single hour's summary.
type irHistorySummaryItem struct {
	AverageSecondaryCurrencyPrice float64 `json:"AverageSecondaryCurrencyPrice"`
	ClosingSecondaryCurrencyPrice float64 `json:"ClosingSecondaryCurrencyPrice"`
	StartTimestampUtc             string  `json:"StartTimestampUtc"`
	EndTimestampUtc               string  `json:"EndTimestampUtc"`
	HighestSecondaryCurrencyPrice float64 `json:"HighestSecondaryCurrencyPrice"`
	LowestSecondaryCurrencyPrice  float64 `json:"LowestSecondaryCurrencyPrice"`
	NumberOfTrades                int     `json:"NumberOfTrades"`
	OpeningSecondaryCurrencyPrice float64 `json:"OpeningSecondaryCurrencyPrice"`
	PrimaryCurrencyVolume         float64 `json:"PrimaryCurrencyVolume"`
	SecondaryCurrencyVolume       float64 `json:"SecondaryCurrencyVolume"`
}

// irAccount represents an account balance.
type irAccount struct {
	AccountGuid      string  `json:"AccountGuid"`
	AccountStatus    string  `json:"AccountStatus"`
	AvailableBalance float64 `json:"AvailableBalance"`
	CurrencyCode     string  `json:"CurrencyCode"`
	TotalBalance     float64 `json:"TotalBalance"`
}

// irOrderResponse represents an order response from Independent Reserve.
type irOrderResponse struct {
	CreatedTimestampUtc   string  `json:"CreatedTimestampUtc"`
	OrderGuid             string  `json:"OrderGuid"`
	Price                 float64 `json:"Price"`
	PrimaryCurrencyCode   string  `json:"PrimaryCurrencyCode"`
	ReservedAmount        float64 `json:"ReservedAmount"`
	SecondaryCurrencyCode string  `json:"SecondaryCurrencyCode"`
	Status                string  `json:"Status"`
	Type                  string  `json:"Type"`
	VolumeFilled          float64 `json:"VolumeFilled"`
	VolumeOrdered         float64 `json:"VolumeOrdered"`
	VolumeCurrencyType    string  `json:"VolumeCurrencyType,omitempty"`
	AvgPrice              float64 `json:"AvgPrice,omitempty"`
}

// irOrderDetails represents detailed order information.
type irOrderDetails struct {
	OrderGuid             string  `json:"OrderGuid"`
	CreatedTimestampUtc   string  `json:"CreatedTimestampUtc"`
	Type                  string  `json:"Type"`
	VolumeOrdered         float64 `json:"VolumeOrdered"`
	VolumeFilled          float64 `json:"VolumeFilled"`
	Price                 float64 `json:"Price"`
	AvgPrice              float64 `json:"AvgPrice"`
	ReservedAmount        float64 `json:"ReservedAmount"`
	Status                string  `json:"Status"`
	PrimaryCurrencyCode   string  `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode string  `json:"SecondaryCurrencyCode"`
	ClientId              string  `json:"ClientId,omitempty"`
}

// irPagedOrders represents a paginated response of orders.
type irPagedOrders struct {
	PageSize   int           `json:"PageSize"`
	TotalItems int           `json:"TotalItems"`
	TotalPages int           `json:"TotalPages"`
	Data       []irOrderItem `json:"Data"`
}

// irOrderItem represents an order item in a paginated list.
type irOrderItem struct {
	CreatedTimestampUtc   string  `json:"CreatedTimestampUtc"`
	OrderType             string  `json:"OrderType"`
	Volume                float64 `json:"Volume"`
	Outstanding           float64 `json:"Outstanding"`
	Price                 float64 `json:"Price"`
	AvgPrice              float64 `json:"AvgPrice"`
	Value                 float64 `json:"Value"`
	Status                string  `json:"Status"`
	OrderGuid             string  `json:"OrderGuid"`
	PrimaryCurrencyCode   string  `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode string  `json:"SecondaryCurrencyCode"`
	FeePercent            float64 `json:"FeePercent"`
	ClientId              string  `json:"ClientId,omitempty"`
	TimeInForce           string  `json:"TimeInForce,omitempty"`
}

// irPagedTrades represents a paginated response of trades.
type irPagedTrades struct {
	PageSize   int           `json:"PageSize"`
	TotalItems int           `json:"TotalItems"`
	TotalPages int           `json:"TotalPages"`
	Data       []irTradeItem `json:"Data"`
}

// irTradeItem represents a trade item in a paginated list.
type irTradeItem struct {
	TradeGuid             string  `json:"TradeGuid"`
	TradeTimestampUtc     string  `json:"TradeTimestampUtc"`
	OrderGuid             string  `json:"OrderGuid"`
	OrderType             string  `json:"OrderType"`
	OrderTimestampUtc     string  `json:"OrderTimestampUtc"`
	VolumeTraded          float64 `json:"VolumeTraded"`
	Price                 float64 `json:"Price"`
	PrimaryCurrencyCode   string  `json:"PrimaryCurrencyCode"`
	SecondaryCurrencyCode string  `json:"SecondaryCurrencyCode"`
}

// irPrimaryCurrencyConfig represents the currency configuration.
type irPrimaryCurrencyConfig struct {
	Currency       string `json:"Currency"`
	Name           string `json:"Name"`
	IsTradeEnabled bool   `json:"IsTradeEnabled"`
	DecimalPlaces  struct {
		OrderPrimaryCurrency   int `json:"OrderPrimaryCurrency"`
		OrderSecondaryCurrency int `json:"OrderSecondaryCurrency"`
	} `json:"DecimalPlaces"`
	Networks []struct {
		Network             string `json:"Network"`
		IsDelisted          bool   `json:"IsDelisted"`
		IsDepositEnabled    bool   `json:"IsDepositEnabled"`
		IsWithdrawalEnabled bool   `json:"IsWithdrawalEnabled"`
	} `json:"Networks"`
}

// irErrorResponse represents an error response.
type irErrorResponse struct {
	ErrorCode string `json:"ErrorCode"`
	Message   string `json:"Message"`
}

// NewIndependentReserveClient creates a new Independent Reserve API client.
// Independent Reserve does not have a testnet/sandbox environment, so testnet parameter is ignored.
func NewIndependentReserveClient(apiKey, apiSecret string, testnet bool, metricsClient *metrics.Metrics) *IndependentReserveClient {
	client := &IndependentReserveClient{
		BaseClient:  common.NewBaseClient("IndependentReserve", apiKey, apiSecret, testnet),
		baseURL:     irBaseURL,
		metrics:     metricsClient,
		logger:      common.DefaultLogger(),
		userAgent:   irUserAgent,
		httpTimeout: irHTTPTimeout,
	}
	client.httpClient = createIRHTTPClient(metricsClient)
	return client
}

func createIRHTTPClient(metricsClient *metrics.Metrics) *gohttpcl.Client {
	opts := []gohttpcl.Option{
		gohttpcl.WithTimeout(irHTTPTimeout),
		gohttpcl.WithMaxRetries(3),
		gohttpcl.WithMinBackoff(200 * time.Millisecond),
		gohttpcl.WithMaxBackoff(10 * time.Second),
		gohttpcl.WithBackoffFactor(2.0),
		gohttpcl.WithBackoffStrategy(gohttpcl.BackoffExponential),
		gohttpcl.WithRetryBudget(0.2, time.Minute),
	}
	if collector := common.NewHTTPMetricsCollector(metricsClient, "IndependentReserve"); collector != nil {
		opts = append(opts, gohttpcl.WithMetrics(collector))
	}
	return gohttpcl.New(opts...)
}

// getNonce returns a new unique nonce value (Unix timestamp in nanoseconds).
func (c *IndependentReserveClient) getNonce() int64 {
	c.nonceMu.Lock()
	defer c.nonceMu.Unlock()

	nonce := time.Now().UnixNano()
	if nonce <= c.lastNonce {
		nonce = c.lastNonce + 1
	}
	c.lastNonce = nonce
	return nonce
}

// createSignature creates the HMAC-SHA256 signature for a private API request.
// The signature is computed from a comma-separated string of:
// URL, apiKey=..., nonce=..., param1=value1, param2=value2, ...
func (c *IndependentReserveClient) createSignature(url string, nonce int64, params []string) string {
	// Build the message: URL, apiKey=..., nonce=..., then additional params
	parts := make([]string, 0, len(params)+3)
	parts = append(parts, url)
	parts = append(parts, "apiKey="+c.APIKey())
	parts = append(parts, "nonce="+strconv.FormatInt(nonce, 10))
	parts = append(parts, params...)

	message := strings.Join(parts, ",")

	// Compute HMAC-SHA256
	h := hmac.New(sha256.New, []byte(c.APISecret()))
	h.Write([]byte(message))
	return strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
}

// doPublicRequest performs a GET request to a public API endpoint.
func (c *IndependentReserveClient) doPublicRequest(ctx context.Context, path string) ([]byte, error) {
	url := c.baseURL + "/Public/" + path

	opts := []gohttpcl.ReqOption{
		gohttpcl.WithHeader("User-Agent", c.userAgent),
		gohttpcl.WithHeader("Accept", "application/json"),
		gohttpcl.WithHeader("Accept-Encoding", "gzip, deflate"),
	}

	resp, err := c.httpClient.Get(ctx, url, c.httpTimeout, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := c.readResponseBody(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var errResp irErrorResponse
		if json.Unmarshal(body, &errResp) == nil && errResp.ErrorCode != "" {
			return nil, fmt.Errorf("IR API error [%s]: %s", errResp.ErrorCode, errResp.Message)
		}
		return nil, common.NewExchangeHTTPError(resp.StatusCode, body, string(body))
	}

	return body, nil
}

// readResponseBody reads and decompresses the response body if necessary.
func (c *IndependentReserveClient) readResponseBody(resp *http.Response) ([]byte, error) {
	var reader io.ReadCloser
	var err error

	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	return io.ReadAll(reader)
}

// doPrivateRequest performs a POST request to a private API endpoint.
// The params slice should contain parameter strings in the format "paramName=paramValue".
// The order of params must match the API documentation for signature generation.
func (c *IndependentReserveClient) doPrivateRequest(ctx context.Context, method string, params []string, requestBody map[string]interface{}) ([]byte, error) {
	url := c.baseURL + "/Private/" + method
	nonce := c.getNonce()
	signature := c.createSignature(url, nonce, params)

	// Build the request body
	body := make(map[string]interface{})
	body["apiKey"] = c.APIKey()
	body["nonce"] = nonce
	body["signature"] = signature

	// Merge additional request body parameters
	for k, v := range requestBody {
		body[k] = v
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	opts := []gohttpcl.ReqOption{
		gohttpcl.WithHeader("User-Agent", c.userAgent),
		gohttpcl.WithHeader("Content-Type", "application/json"),
		gohttpcl.WithHeader("Accept", "application/json"),
		gohttpcl.WithHeader("Accept-Encoding", "gzip, deflate"),
	}

	resp, err := c.httpClient.Post(ctx, url, bytes.NewReader(jsonBody), c.httpTimeout, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := c.readResponseBody(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var errResp irErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil && errResp.ErrorCode != "" {
			return nil, fmt.Errorf("IR API error [%s]: %s", errResp.ErrorCode, errResp.Message)
		}
		return nil, common.NewExchangeHTTPError(resp.StatusCode, respBody, string(respBody))
	}

	return respBody, nil
}

// =============================================================================
// Public API Methods
// =============================================================================

// GetValidPrimaryCurrencyCodes returns the list of valid primary (crypto) currency codes.
func (c *IndependentReserveClient) GetValidPrimaryCurrencyCodes(ctx context.Context) ([]string, error) {
	body, err := c.doPublicRequest(ctx, "GetValidPrimaryCurrencyCodes")
	if err != nil {
		return nil, err
	}

	var codes []string
	if err := json.Unmarshal(body, &codes); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return codes, nil
}

// GetValidSecondaryCurrencyCodes returns the list of valid secondary (fiat) currency codes.
func (c *IndependentReserveClient) GetValidSecondaryCurrencyCodes(ctx context.Context) ([]string, error) {
	body, err := c.doPublicRequest(ctx, "GetValidSecondaryCurrencyCodes")
	if err != nil {
		return nil, err
	}

	var codes []string
	if err := json.Unmarshal(body, &codes); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return codes, nil
}

// GetPrimaryCurrencyConfig2 returns the configuration of all primary currencies.
func (c *IndependentReserveClient) GetPrimaryCurrencyConfig2(ctx context.Context) ([]irPrimaryCurrencyConfig, error) {
	body, err := c.doPublicRequest(ctx, "GetPrimaryCurrencyConfig2")
	if err != nil {
		return nil, err
	}

	var configs []irPrimaryCurrencyConfig
	if err := json.Unmarshal(body, &configs); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return configs, nil
}

// GetMarketSummaryRaw returns the raw market summary for a trading pair.
func (c *IndependentReserveClient) GetMarketSummaryRaw(ctx context.Context, primaryCurrency, secondaryCurrency string) (*irMarketSummary, error) {
	path := fmt.Sprintf("GetMarketSummary?primaryCurrencyCode=%s&secondaryCurrencyCode=%s",
		strings.ToLower(primaryCurrency), strings.ToLower(secondaryCurrency))
	body, err := c.doPublicRequest(ctx, path)
	if err != nil {
		return nil, err
	}

	var summary irMarketSummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &summary, nil
}

// GetOrderBookRaw returns the raw order book for a trading pair.
func (c *IndependentReserveClient) GetOrderBookRaw(ctx context.Context, primaryCurrency, secondaryCurrency string) (*irOrderBook, error) {
	path := fmt.Sprintf("GetOrderBook?primaryCurrencyCode=%s&secondaryCurrencyCode=%s",
		strings.ToLower(primaryCurrency), strings.ToLower(secondaryCurrency))
	body, err := c.doPublicRequest(ctx, path)
	if err != nil {
		return nil, err
	}

	var orderBook irOrderBook
	if err := json.Unmarshal(body, &orderBook); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &orderBook, nil
}

// GetRecentTradesRaw returns the raw recent trades for a trading pair.
func (c *IndependentReserveClient) GetRecentTradesRaw(ctx context.Context, primaryCurrency, secondaryCurrency string, limit int) (*irRecentTrades, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	path := fmt.Sprintf("GetRecentTrades?primaryCurrencyCode=%s&secondaryCurrencyCode=%s&numberOfRecentTradesToRetrieve=%d",
		strings.ToLower(primaryCurrency), strings.ToLower(secondaryCurrency), limit)
	body, err := c.doPublicRequest(ctx, path)
	if err != nil {
		return nil, err
	}

	var trades irRecentTrades
	if err := json.Unmarshal(body, &trades); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &trades, nil
}

// GetTradeHistorySummaryRaw returns the raw trade history summary.
func (c *IndependentReserveClient) GetTradeHistorySummaryRaw(ctx context.Context, primaryCurrency, secondaryCurrency string, hours int) (*irHistorySummary, error) {
	if hours <= 0 || hours > 240 {
		hours = 24
	}
	path := fmt.Sprintf("GetTradeHistorySummary?primaryCurrencyCode=%s&secondaryCurrencyCode=%s&numberOfHoursInThePastToRetrieve=%d",
		strings.ToLower(primaryCurrency), strings.ToLower(secondaryCurrency), hours)
	body, err := c.doPublicRequest(ctx, path)
	if err != nil {
		return nil, err
	}

	var summary irHistorySummary
	if err := json.Unmarshal(body, &summary); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return &summary, nil
}

// GetOrderMinimumVolumes returns the minimum order volumes for each currency.
func (c *IndependentReserveClient) GetOrderMinimumVolumes(ctx context.Context) (map[string]float64, error) {
	body, err := c.doPublicRequest(ctx, "GetOrderMinimumVolumes")
	if err != nil {
		return nil, err
	}

	var volumes map[string]float64
	if err := json.Unmarshal(body, &volumes); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return volumes, nil
}

// GetFxRates returns the exchange rates used for deposits and withdrawals.
func (c *IndependentReserveClient) GetFxRates(ctx context.Context) ([]map[string]interface{}, error) {
	body, err := c.doPublicRequest(ctx, "GetFxRates")
	if err != nil {
		return nil, err
	}

	var rates []map[string]interface{}
	if err := json.Unmarshal(body, &rates); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return rates, nil
}

// =============================================================================
// ExchangeClient Interface Implementation
// =============================================================================

// GetTradingPairs returns all available trading pairs.
func (c *IndependentReserveClient) GetTradingPairs() ([]common.TradingPair, error) {
	c.pairsMu.RLock()
	if time.Since(c.pairsFetched) < irAssetCacheTTL && len(c.pairs) > 0 {
		pairs := c.pairs
		c.pairsMu.RUnlock()
		return pairs, nil
	}
	c.pairsMu.RUnlock()

	ctx := context.Background()

	// Get primary and secondary currency codes
	primaryCodes, err := c.GetValidPrimaryCurrencyCodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary currencies: %w", err)
	}

	secondaryCodes, err := c.GetValidSecondaryCurrencyCodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get secondary currencies: %w", err)
	}

	// Get currency configs to check which are tradeable
	configs, err := c.GetPrimaryCurrencyConfig2(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get currency configs: %w", err)
	}

	// Build a map of tradeable currencies
	tradeable := make(map[string]bool)
	for _, cfg := range configs {
		if cfg.IsTradeEnabled {
			tradeable[strings.ToUpper(cfg.Currency)] = true
		}
	}

	// Build trading pairs
	var pairs []common.TradingPair
	for _, primary := range primaryCodes {
		if !tradeable[strings.ToUpper(primary)] {
			continue
		}
		for _, secondary := range secondaryCodes {
			// Normalize currency codes (XBT -> BTC)
			baseAsset := normalizeIRCurrencyCode(primary)
			quoteAsset := strings.ToUpper(secondary)
			pairs = append(pairs, common.TradingPair{
				Symbol:     baseAsset + "/" + quoteAsset,
				BaseAsset:  baseAsset,
				QuoteAsset: quoteAsset,
			})
		}
	}

	c.pairsMu.Lock()
	c.pairs = pairs
	c.primaryCodes = primaryCodes
	c.secondaryCodes = secondaryCodes
	c.pairsFetched = time.Now()
	c.pairsMu.Unlock()

	return pairs, nil
}

// GetTicker returns the ticker for a trading pair.
func (c *IndependentReserveClient) GetTicker(symbol string) (*models.Ticker, error) {
	primary, secondary := parseIRSymbol(symbol)
	if primary == "" || secondary == "" {
		return nil, fmt.Errorf("invalid symbol format: %s (expected BASE/QUOTE)", symbol)
	}

	summary, err := c.GetMarketSummaryRaw(context.Background(), primary, secondary)
	if err != nil {
		return nil, err
	}

	ts, _ := time.Parse(time.RFC3339, summary.CreatedTimestampUtc)

	return &models.Ticker{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		LastPrice: summary.LastPrice,
		Timestamp: ts,
		Volume:    summary.DayVolumeXbt,
		Bid:       summary.CurrentHighestBidPrice,
		Ask:       summary.CurrentLowestOfferPrice,
		Open:      summary.DayAvgPrice, // IR doesn't provide exact open price
		High:      summary.DayHighestPrice,
		Low:       summary.DayLowestPrice,
		Close:     summary.LastPrice,
	}, nil
}

// GetOrderBook returns the order book for a trading pair.
func (c *IndependentReserveClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	primary, secondary := parseIRSymbol(symbol)
	if primary == "" || secondary == "" {
		return nil, fmt.Errorf("invalid symbol format: %s (expected BASE/QUOTE)", symbol)
	}

	rawBook, err := c.GetOrderBookRaw(context.Background(), primary, secondary)
	if err != nil {
		return nil, err
	}

	ts, _ := time.Parse(time.RFC3339, rawBook.CreatedTimestampUtc)

	// Convert bids
	bids := make([]models.OrderBookEntry, 0, len(rawBook.BuyOrders))
	for i, order := range rawBook.BuyOrders {
		if depth > 0 && i >= depth {
			break
		}
		bids = append(bids, models.OrderBookEntry{
			Price:  order.Price,
			Amount: order.Volume,
		})
	}

	// Convert asks
	asks := make([]models.OrderBookEntry, 0, len(rawBook.SellOrders))
	for i, order := range rawBook.SellOrders {
		if depth > 0 && i >= depth {
			break
		}
		asks = append(asks, models.OrderBookEntry{
			Price:  order.Price,
			Amount: order.Volume,
		})
	}

	return &models.OrderBook{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: ts,
	}, nil
}

// GetCandles returns candlestick data for a trading pair.
// Independent Reserve provides hourly candle data through GetTradeHistorySummary.
// Supported intervals: "1h" (hourly). For other intervals, data is aggregated from hourly.
func (c *IndependentReserveClient) GetCandles(symbol string, interval string, since time.Time, limit int) ([]models.Candle, error) {
	primary, secondary := parseIRSymbol(symbol)
	if primary == "" || secondary == "" {
		return nil, fmt.Errorf("invalid symbol format: %s (expected BASE/QUOTE)", symbol)
	}

	// Calculate hours to fetch based on since time
	hours := int(time.Since(since).Hours())
	if hours <= 0 {
		hours = 24
	}
	if hours > 240 {
		hours = 240 // Maximum allowed by IR API
	}
	if limit > 0 && limit < hours {
		hours = limit
	}

	summary, err := c.GetTradeHistorySummaryRaw(context.Background(), primary, secondary, hours)
	if err != nil {
		return nil, err
	}

	candles := make([]models.Candle, 0, len(summary.HistorySummaryItems))
	for _, item := range summary.HistorySummaryItems {
		openTime, _ := time.Parse(time.RFC3339, item.StartTimestampUtc)
		closeTime, _ := time.Parse(time.RFC3339, item.EndTimestampUtc)

		// Skip candles with no trades
		if item.NumberOfTrades == 0 {
			continue
		}

		candles = append(candles, models.Candle{
			Exchange:  c.GetName(),
			Symbol:    symbol,
			Interval:  "1h",
			OpenTime:  openTime,
			CloseTime: closeTime,
			Open:      item.OpeningSecondaryCurrencyPrice,
			High:      item.HighestSecondaryCurrencyPrice,
			Low:       item.LowestSecondaryCurrencyPrice,
			Close:     item.ClosingSecondaryCurrencyPrice,
			Volume:    item.PrimaryCurrencyVolume,
		})
	}

	return candles, nil
}

// GetTrades returns recent trades for a trading pair.
func (c *IndependentReserveClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	primary, secondary := parseIRSymbol(symbol)
	if primary == "" || secondary == "" {
		return nil, fmt.Errorf("invalid symbol format: %s (expected BASE/QUOTE)", symbol)
	}

	rawTrades, err := c.GetRecentTradesRaw(context.Background(), primary, secondary, limit)
	if err != nil {
		return nil, err
	}

	trades := make([]models.Trade, 0, len(rawTrades.Trades))
	for _, t := range rawTrades.Trades {
		ts, _ := time.Parse(time.RFC3339, t.TradeTimestampUtc)
		if !since.IsZero() && ts.Before(since) {
			continue
		}

		tradeType := "buy"
		if t.Taker == "Offer" {
			tradeType = "sell"
		}

		trades = append(trades, models.Trade{
			ID:            t.TradeGuid,
			Symbol:        symbol,
			Exchange:      c.GetName(),
			Type:          tradeType,
			Quantity:      t.PrimaryCurrencyAmount,
			Price:         t.SecondaryCurrencyTradePrice,
			Total:         t.PrimaryCurrencyAmount * t.SecondaryCurrencyTradePrice,
			ExecutionTime: ts,
		})
	}

	return trades, nil
}

// =============================================================================
// Private API Methods - Account
// =============================================================================

// GetBalance returns the balance for a specific currency.
func (c *IndependentReserveClient) GetBalance(currency string) (*common.Balance, error) {
	balances, err := c.GetBalances()
	if err != nil {
		return nil, err
	}

	upperCurrency := strings.ToUpper(currency)
	if balance, ok := balances[upperCurrency]; ok {
		return balance, nil
	}

	return &common.Balance{
		Asset:  upperCurrency,
		Free:   "0",
		Locked: "0",
	}, nil
}

// GetBalances returns all account balances.
func (c *IndependentReserveClient) GetBalances() (map[string]*common.Balance, error) {
	body, err := c.doPrivateRequest(context.Background(), "GetAccounts", nil, nil)
	if err != nil {
		return nil, err
	}

	var accounts []irAccount
	if err := json.Unmarshal(body, &accounts); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	balances := make(map[string]*common.Balance)
	for _, acc := range accounts {
		// Calculate locked balance (total - available)
		locked := acc.TotalBalance - acc.AvailableBalance
		if locked < 0 {
			locked = 0
		}

		balances[strings.ToUpper(acc.CurrencyCode)] = &common.Balance{
			Asset:  strings.ToUpper(acc.CurrencyCode),
			Free:   strconv.FormatFloat(acc.AvailableBalance, 'f', -1, 64),
			Locked: strconv.FormatFloat(locked, 'f', -1, 64),
		}
	}

	return balances, nil
}

// =============================================================================
// Private API Methods - Orders
// =============================================================================

// CreateOrder creates a new order on the exchange.
func (c *IndependentReserveClient) CreateOrder(symbol string, side common.OrderSide, orderType common.OrderType, amount float64, price float64) (*common.Order, error) {
	primary, secondary := parseIRSymbol(symbol)
	if primary == "" || secondary == "" {
		return nil, fmt.Errorf("invalid symbol format: %s (expected BASE/QUOTE)", symbol)
	}

	ctx := context.Background()

	if orderType == common.OrderTypeMarket {
		return c.createMarketOrder(ctx, primary, secondary, side, amount)
	}
	return c.createLimitOrder(ctx, primary, secondary, side, amount, price)
}

func (c *IndependentReserveClient) createLimitOrder(ctx context.Context, primary, secondary string, side common.OrderSide, volume, price float64) (*common.Order, error) {
	orderType := "LimitBid"
	if side == common.OrderSideSell {
		orderType = "LimitOffer"
	}

	// Build signature params in correct order
	params := []string{
		fmt.Sprintf("primaryCurrencyCode=%s", primary),
		fmt.Sprintf("secondaryCurrencyCode=%s", secondary),
		fmt.Sprintf("orderType=%s", orderType),
		fmt.Sprintf("price=%s", strconv.FormatFloat(price, 'f', -1, 64)),
		fmt.Sprintf("volume=%s", strconv.FormatFloat(volume, 'f', -1, 64)),
	}

	// Build request body
	body := map[string]interface{}{
		"primaryCurrencyCode":   primary,
		"secondaryCurrencyCode": secondary,
		"orderType":             orderType,
		"price":                 price,
		"volume":                volume,
	}

	respBody, err := c.doPrivateRequest(ctx, "PlaceLimitOrder", params, body)
	if err != nil {
		return nil, err
	}

	var resp irOrderResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return c.convertOrderResponse(&resp, side)
}

func (c *IndependentReserveClient) createMarketOrder(ctx context.Context, primary, secondary string, side common.OrderSide, volume float64) (*common.Order, error) {
	orderType := "MarketBid"
	if side == common.OrderSideSell {
		orderType = "MarketOffer"
	}

	// Build signature params in correct order
	params := []string{
		fmt.Sprintf("primaryCurrencyCode=%s", primary),
		fmt.Sprintf("secondaryCurrencyCode=%s", secondary),
		fmt.Sprintf("orderType=%s", orderType),
		fmt.Sprintf("volume=%s", strconv.FormatFloat(volume, 'f', -1, 64)),
	}

	// Build request body
	body := map[string]interface{}{
		"primaryCurrencyCode":   primary,
		"secondaryCurrencyCode": secondary,
		"orderType":             orderType,
		"volume":                volume,
	}

	respBody, err := c.doPrivateRequest(ctx, "PlaceMarketOrder", params, body)
	if err != nil {
		return nil, err
	}

	var resp irOrderResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return c.convertOrderResponse(&resp, side)
}

func (c *IndependentReserveClient) convertOrderResponse(resp *irOrderResponse, side common.OrderSide) (*common.Order, error) {
	ts, _ := time.Parse(time.RFC3339, resp.CreatedTimestampUtc)

	status := convertIROrderStatus(resp.Status)
	orderType := common.OrderTypeLimit
	if strings.HasPrefix(resp.Type, "Market") {
		orderType = common.OrderTypeMarket
	}

	symbol := convertToIRSymbol(resp.PrimaryCurrencyCode, resp.SecondaryCurrencyCode)

	return &common.Order{
		ID:              resp.OrderGuid,
		Symbol:          symbol,
		Side:            side,
		Type:            orderType,
		Status:          status,
		Price:           resp.Price,
		Amount:          resp.VolumeOrdered,
		FilledAmount:    resp.VolumeFilled,
		RemainingAmount: resp.VolumeOrdered - resp.VolumeFilled,
		CreatedAt:       ts,
		UpdatedAt:       ts,
		Quantity:        resp.VolumeOrdered,
		Timestamp:       ts,
	}, nil
}

// GetOrder retrieves order details by ID.
func (c *IndependentReserveClient) GetOrder(symbol string, orderID string) (*common.Order, error) {
	params := []string{
		fmt.Sprintf("orderGuid=%s", orderID),
	}

	body := map[string]interface{}{
		"orderGuid": orderID,
	}

	respBody, err := c.doPrivateRequest(context.Background(), "GetOrderDetails", params, body)
	if err != nil {
		return nil, err
	}

	var details irOrderDetails
	if err := json.Unmarshal(respBody, &details); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	ts, _ := time.Parse(time.RFC3339, details.CreatedTimestampUtc)

	side := common.OrderSideBuy
	if strings.Contains(details.Type, "Offer") {
		side = common.OrderSideSell
	}

	orderType := common.OrderTypeLimit
	if strings.HasPrefix(details.Type, "Market") {
		orderType = common.OrderTypeMarket
	}

	irSymbol := convertToIRSymbol(details.PrimaryCurrencyCode, details.SecondaryCurrencyCode)

	return &common.Order{
		ID:              details.OrderGuid,
		ClientOrderID:   details.ClientId,
		Symbol:          irSymbol,
		Side:            side,
		Type:            orderType,
		Status:          convertIROrderStatus(details.Status),
		Price:           details.Price,
		Amount:          details.VolumeOrdered,
		FilledAmount:    details.VolumeFilled,
		RemainingAmount: details.VolumeOrdered - details.VolumeFilled,
		CreatedAt:       ts,
		UpdatedAt:       ts,
		Quantity:        details.VolumeOrdered,
		Timestamp:       ts,
	}, nil
}

// GetOrders retrieves orders for a symbol.
func (c *IndependentReserveClient) GetOrders(symbol string, since time.Time, limit int) ([]common.Order, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}

	primary, secondary := parseIRSymbol(symbol)

	// Build params for signature
	params := []string{
		fmt.Sprintf("pageIndex=%d", 1),
		fmt.Sprintf("pageSize=%d", limit),
	}

	body := map[string]interface{}{
		"pageIndex": 1,
		"pageSize":  limit,
	}

	if primary != "" && secondary != "" {
		params = append([]string{
			fmt.Sprintf("primaryCurrencyCode=%s", primary),
			fmt.Sprintf("secondaryCurrencyCode=%s", secondary),
		}, params...)
		body["primaryCurrencyCode"] = primary
		body["secondaryCurrencyCode"] = secondary
	}

	respBody, err := c.doPrivateRequest(context.Background(), "GetOpenOrders", params, body)
	if err != nil {
		return nil, err
	}

	var paged irPagedOrders
	if err := json.Unmarshal(respBody, &paged); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	orders := make([]common.Order, 0, len(paged.Data))
	for _, item := range paged.Data {
		ts, _ := time.Parse(time.RFC3339, item.CreatedTimestampUtc)
		if !since.IsZero() && ts.Before(since) {
			continue
		}

		side := common.OrderSideBuy
		if strings.Contains(item.OrderType, "Offer") {
			side = common.OrderSideSell
		}

		orderType := common.OrderTypeLimit
		if strings.HasPrefix(item.OrderType, "Market") {
			orderType = common.OrderTypeMarket
		}

		irSymbol := convertToIRSymbol(item.PrimaryCurrencyCode, item.SecondaryCurrencyCode)

		orders = append(orders, common.Order{
			ID:              item.OrderGuid,
			ClientOrderID:   item.ClientId,
			Symbol:          irSymbol,
			Side:            side,
			Type:            orderType,
			Status:          convertIROrderStatus(item.Status),
			Price:           item.Price,
			Amount:          item.Volume,
			FilledAmount:    item.Volume - item.Outstanding,
			RemainingAmount: item.Outstanding,
			Fee:             item.FeePercent,
			CreatedAt:       ts,
			UpdatedAt:       ts,
			Quantity:        item.Volume,
			Timestamp:       ts,
		})
	}

	return orders, nil
}

// GetClosedOrders retrieves closed orders.
func (c *IndependentReserveClient) GetClosedOrders(ctx context.Context, symbol string, since time.Time, limit int) ([]common.Order, error) {
	if limit <= 0 || limit > 5000 {
		limit = 100
	}

	primary, secondary := parseIRSymbol(symbol)

	// Build params for signature
	var params []string
	body := map[string]interface{}{
		"pageIndex":     1,
		"pageSize":      limit,
		"includeTotals": false,
	}

	if primary != "" && secondary != "" {
		params = append(params, fmt.Sprintf("primaryCurrencyCode=%s", primary))
		params = append(params, fmt.Sprintf("secondaryCurrencyCode=%s", secondary))
		body["primaryCurrencyCode"] = primary
		body["secondaryCurrencyCode"] = secondary
	}

	if !since.IsZero() {
		fromStr := since.UTC().Format(time.RFC3339)
		params = append(params, fmt.Sprintf("fromTimestampUtc=%s", fromStr))
		body["fromTimestampUtc"] = fromStr
	}

	params = append(params,
		fmt.Sprintf("includeTotals=%v", false),
		fmt.Sprintf("pageIndex=%d", 1),
		fmt.Sprintf("pageSize=%d", limit),
	)

	respBody, err := c.doPrivateRequest(ctx, "GetClosedOrders", params, body)
	if err != nil {
		return nil, err
	}

	var paged irPagedOrders
	if err := json.Unmarshal(respBody, &paged); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	orders := make([]common.Order, 0, len(paged.Data))
	for _, item := range paged.Data {
		ts, _ := time.Parse(time.RFC3339, item.CreatedTimestampUtc)

		side := common.OrderSideBuy
		if strings.Contains(item.OrderType, "Offer") {
			side = common.OrderSideSell
		}

		orderType := common.OrderTypeLimit
		if strings.HasPrefix(item.OrderType, "Market") {
			orderType = common.OrderTypeMarket
		}

		irSymbol := convertToIRSymbol(item.PrimaryCurrencyCode, item.SecondaryCurrencyCode)

		orders = append(orders, common.Order{
			ID:              item.OrderGuid,
			ClientOrderID:   item.ClientId,
			Symbol:          irSymbol,
			Side:            side,
			Type:            orderType,
			Status:          convertIROrderStatus(item.Status),
			Price:           item.Price,
			Amount:          item.Volume,
			FilledAmount:    item.Volume - item.Outstanding,
			RemainingAmount: item.Outstanding,
			Fee:             item.FeePercent,
			CreatedAt:       ts,
			UpdatedAt:       ts,
			Quantity:        item.Volume,
			Timestamp:       ts,
		})
	}

	return orders, nil
}

// CancelOrder cancels an order by ID.
func (c *IndependentReserveClient) CancelOrder(symbol string, orderID string) error {
	params := []string{
		fmt.Sprintf("orderGuid=%s", orderID),
	}

	body := map[string]interface{}{
		"orderGuid": orderID,
	}

	_, err := c.doPrivateRequest(context.Background(), "CancelOrder", params, body)
	return err
}

// CancelAllOrders cancels all orders for a symbol.
// Note: Independent Reserve doesn't have a native "cancel all" endpoint,
// so this retrieves open orders and cancels them individually.
func (c *IndependentReserveClient) CancelAllOrders(symbol string) error {
	orders, err := c.GetOrders(symbol, time.Time{}, 100)
	if err != nil {
		return fmt.Errorf("failed to get open orders: %w", err)
	}

	// Collect order GUIDs
	var guids []string
	for _, order := range orders {
		guids = append(guids, order.ID)
	}

	if len(guids) == 0 {
		return nil
	}

	// Use bulk cancel if multiple orders
	if len(guids) > 1 {
		return c.cancelOrders(context.Background(), guids)
	}

	// Cancel single order
	return c.CancelOrder(symbol, guids[0])
}

func (c *IndependentReserveClient) cancelOrders(ctx context.Context, orderGuids []string) error {
	// For signature, orderGuids needs to be comma-separated
	params := []string{
		fmt.Sprintf("orderGuids=%s", strings.Join(orderGuids, ",")),
	}

	body := map[string]interface{}{
		"orderGuids": orderGuids,
	}

	_, err := c.doPrivateRequest(ctx, "CancelOrders", params, body)
	return err
}

// =============================================================================
// Private API Methods - Trades
// =============================================================================

// GetMyTrades retrieves the user's trade history.
func (c *IndependentReserveClient) GetMyTrades(ctx context.Context, since, until time.Time, limit int) ([]irTradeItem, error) {
	if limit <= 0 || limit > 5000 {
		limit = 100
	}

	var params []string
	body := map[string]interface{}{
		"pageIndex":     1,
		"pageSize":      limit,
		"includeTotals": false,
	}

	if !since.IsZero() {
		fromStr := since.UTC().Format(time.RFC3339)
		params = append(params, fmt.Sprintf("fromTimestampUtc=%s", fromStr))
		body["fromTimestampUtc"] = fromStr
	}

	if !until.IsZero() {
		toStr := until.UTC().Format(time.RFC3339)
		params = append(params, fmt.Sprintf("toTimestampUtc=%s", toStr))
		body["toTimestampUtc"] = toStr
	}

	params = append(params,
		fmt.Sprintf("includeTotals=%v", false),
		fmt.Sprintf("pageIndex=%d", 1),
		fmt.Sprintf("pageSize=%d", limit),
	)

	respBody, err := c.doPrivateRequest(ctx, "GetTrades", params, body)
	if err != nil {
		return nil, err
	}

	var paged irPagedTrades
	if err := json.Unmarshal(respBody, &paged); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return paged.Data, nil
}

// =============================================================================
// Private API Methods - Brokerage Fees
// =============================================================================

// GetBrokerageFees retrieves the trading fees for all currencies.
func (c *IndependentReserveClient) GetBrokerageFees(ctx context.Context) (map[string]float64, error) {
	respBody, err := c.doPrivateRequest(ctx, "GetBrokerageFees", nil, nil)
	if err != nil {
		return nil, err
	}

	var fees []struct {
		CurrencyCode string  `json:"CurrencyCode"`
		Fee          float64 `json:"Fee"`
	}

	if err := json.Unmarshal(respBody, &fees); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	feeMap := make(map[string]float64)
	for _, f := range fees {
		feeMap[strings.ToUpper(f.CurrencyCode)] = f.Fee
	}

	return feeMap, nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// convertToIRSymbol converts currency codes to a standard symbol format.
// e.g., "Xbt", "Aud" -> "BTC/AUD"
func convertToIRSymbol(primary, secondary string) string {
	return normalizeIRCurrencyCode(primary) + "/" + strings.ToUpper(secondary)
}

// normalizeIRCurrencyCode converts IR currency codes to standard codes.
// e.g., "Xbt" -> "BTC", "Eth" -> "ETH"
func normalizeIRCurrencyCode(code string) string {
	upper := strings.ToUpper(code)
	// Independent Reserve uses XBT for Bitcoin instead of BTC
	if upper == "XBT" {
		return "BTC"
	}
	return upper
}

// parseIRSymbol parses a standard symbol format into IR currency codes.
// e.g., "BTC/AUD" -> "Xbt", "Aud"
func parseIRSymbol(symbol string) (primary, secondary string) {
	parts := strings.Split(symbol, "/")
	if len(parts) != 2 {
		// Try without separator
		parts = strings.Split(symbol, "-")
		if len(parts) != 2 {
			return "", ""
		}
	}

	primary = parts[0]
	secondary = parts[1]

	// Convert BTC to XBT for IR API
	if strings.ToUpper(primary) == "BTC" {
		primary = "Xbt"
	}

	return primary, secondary
}

// convertIROrderStatus converts IR order status to common.OrderStatus.
func convertIROrderStatus(status string) common.OrderStatus {
	switch strings.ToLower(status) {
	case "open":
		return common.OrderStatusNew
	case "partiallyfilled":
		return common.OrderStatusPartiallyFilled
	case "filled":
		return common.OrderStatusFilled
	case "cancelled", "canceled":
		return common.OrderStatusCancelled
	case "rejected":
		return common.OrderStatusRejected
	case "expired":
		return common.OrderStatusExpired
	default:
		return common.OrderStatusNew
	}
}
