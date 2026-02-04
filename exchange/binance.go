package exchange

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	common "github.com/evdnx/goexchange/common"
	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/gohttpcl"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
	"github.com/evdnx/gowscl"
	"golang.org/x/net/html"
)

// BinanceClient implements the ExchangeClient interface for Binance spot and futures trading
type BinanceClient struct {
	*common.BaseClient
	httpClient     *gohttpcl.Client
	httpTimeout    time.Duration
	wsClient       *BinanceWebSocketClient
	baseURL        string
	futuresBaseURL string
	wsURL          string
	isTestnet      bool
	metrics        *metrics.Metrics
}

// BinanceResponse represents a generic Binance API response
type BinanceResponse struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"msg,omitempty"`
}

// BinanceOrderResponse represents the payload returned by the cancel order endpoints.
// https://developers.binance.com/docs/binance-spot-api-docs/rest-api#cancel-order-trade
type BinanceOrderResponse struct {
	Symbol                  string `json:"symbol"`
	OrigClientOrderID       string `json:"origClientOrderId"`
	OrderID                 int64  `json:"orderId"`
	OrderListID             int64  `json:"orderListId"`
	ClientOrderID           string `json:"clientOrderId"`
	Price                   string `json:"price"`
	OrigQuantity            string `json:"origQty"`
	ExecutedQuantity        string `json:"executedQty"`
	CumulativeQuoteQuantity string `json:"cummulativeQuoteQty"`
	Status                  string `json:"status"`
	TimeInForce             string `json:"timeInForce"`
	Type                    string `json:"type"`
	Side                    string `json:"side"`
	SelfTradePreventionMode string `json:"selfTradePreventionMode,omitempty"`
	PreventedMatchID        int64  `json:"preventedMatchId,omitempty"`
	PreventedQuantity       string `json:"preventedQuantity,omitempty"`
	WorkingTime             int64  `json:"workingTime,omitempty"`
	UpdateTime              int64  `json:"updateTime,omitempty"`
	IcebergQuantity         string `json:"icebergQty,omitempty"`
	StopPrice               string `json:"stopPrice,omitempty"`
	IsWorking               bool   `json:"isWorking,omitempty"`
	Time                    int64  `json:"time,omitempty"`
}

// BinanceStreamType represents WebSocket stream types
type BinanceStreamType string

// FundingRateInfo represents funding rate information
type FundingRateInfo struct {
	Symbol          string
	FundingRate     float64
	NextFundingTime time.Time
	LastFundingTime time.Time
	MarkPrice       float64
	IndexPrice      float64
}

// FuturesOrder extends Order with futures-specific fields
type FuturesOrder struct {
	common.Order
	ReduceOnly   bool   // Whether the order should only reduce position
	PositionSide string // "BOTH", "LONG", or "SHORT" for hedge mode
	MarginType   string // "isolated" or "cross"
	Leverage     int    // Leverage level
}

// FuturesPosition represents a futures position
type FuturesPosition struct {
	Symbol           string
	Side             string // "long" or "short"
	Size             float64
	EntryPrice       float64
	MarkPrice        float64
	LiquidationPrice float64
	Margin           float64
	UnrealizedPnL    float64
	Leverage         int
	MarginType       string // "isolated" or "cross"
	PositionSide     string // "BOTH", "LONG", or "SHORT" for hedge mode
	UpdateTime       time.Time
}

// SpotPosition represents a spot trading position (non-zero balance)
type SpotPosition struct {
	Asset       string  // Asset symbol (e.g., "BTC", "ETH")
	Free        float64 // Available balance for trading
	Locked      float64 // Locked balance (in orders, etc.)
	Total       float64 // Total balance (Free + Locked)
	IsDust      bool    // Whether this is considered dust (very small amount)
	IsTradeable bool    // Whether this asset can be traded
}

// BinanceDustConvertibleAsset represents a single dust-eligible asset entry.
type BinanceDustConvertibleAsset struct {
	Asset                    string `json:"asset"`
	AssetFullName            string `json:"assetFullName"`
	AmountFree               string `json:"amountFree"`
	Exchange                 string `json:"exchange"`
	ToQuotaAssetAmount       string `json:"toQuotaAssetAmount"`
	ToTargetAssetAmount      string `json:"toTargetAssetAmount"`
	ToTargetAssetOffExchange string `json:"toTargetAssetOffExchange"`
}

// BinanceDustConvertibleAssetsResponse represents the response from the dust-convertible assets endpoint.
type BinanceDustConvertibleAssetsResponse struct {
	DribbletPercentage             string                        `json:"dribbletPercentage"`
	TotalTransferQuotaAssetAmount  string                        `json:"totalTransferQuotaAssetAmount"`
	TotalTransferTargetAssetAmount string                        `json:"totalTransferTargetAssetAmount"`
	DribbletBase                   string                        `json:"dribbletBase"`
	Details                        []BinanceDustConvertibleAsset `json:"details"`
	BinanceResponse
}

// BinanceDustTransfer represents a single dust conversion result entry.
type BinanceDustTransfer struct {
	TranID              int64  `json:"tranId"`
	FromAsset           string `json:"fromAsset"`
	Amount              string `json:"amount"`
	TransferedAmount    string `json:"transferedAmount"`
	ServiceChargeAmount string `json:"serviceChargeAmount"`
	OperateTime         int64  `json:"operateTime"`
}

// BinanceDustConvertResult represents the response from the dust convert endpoint.
type BinanceDustConvertResult struct {
	TotalTransfered    string                `json:"totalTransfered"`
	TotalServiceCharge string                `json:"totalServiceCharge"`
	TransferResult     []BinanceDustTransfer `json:"transferResult"`
	BinanceResponse
}

const (
	StreamTicker     BinanceStreamType = "ticker"
	StreamKline      BinanceStreamType = "kline"
	StreamTrade      BinanceStreamType = "trade"
	StreamDepth      BinanceStreamType = "depth"
	StreamUserData   BinanceStreamType = "userData"
	StreamAggTrade   BinanceStreamType = "aggTrade"
	StreamBookTicker BinanceStreamType = "bookTicker"
)

const binanceHTTPTimeout = 10 * time.Second

// taggedCoinsCache caches the list of Seed and Monitoring tagged coins
// to avoid excessive scraping. Cache expires after 24 hours.
type taggedCoinsCache struct {
	mu              sync.RWMutex
	monitoringCoins map[string]bool // base asset -> true
	seedCoins       map[string]bool // base asset -> true
	lastUpdate      time.Time
	cacheDuration   time.Duration
}

var globalTaggedCoinsCache = &taggedCoinsCache{
	monitoringCoins: make(map[string]bool),
	seedCoins:       make(map[string]bool),
	cacheDuration:   24 * time.Hour,
}

// NewBinanceClient creates a new Binance client for spot and futures trading.
// If apiKey or apiSecret are empty strings, they will be read from environment variables
// BINANCE_API_KEY and BINANCE_API_SECRET respectively.
func NewBinanceClient(apiKey, apiSecret string, testnet bool, metrics *metrics.Metrics) *BinanceClient {
	// Read from environment variables if not provided
	if apiKey == "" {
		apiKey = os.Getenv("BINANCE_API_KEY")
	}
	if apiSecret == "" {
		apiSecret = os.Getenv("BINANCE_API_SECRET")
	}

	baseURL := "https://api.binance.com"
	futuresBaseURL := "https://fapi.binance.com"
	// Use combined stream endpoint for proper message format with {"stream": "...", "data": {...}}
	wsURL := "wss://stream.binance.com:9443/stream"

	if testnet {
		// For demo accounts, use demo-api.binance.com
		// Note: Demo environment may not support all endpoints (e.g., /order/test)
		baseURL = "https://demo-api.binance.com/api"
		futuresBaseURL = "https://testnet.binancefuture.com" // Prepend "/fapi" for USDT-M or "/dapi" for Coin-M in endpoints, e.g., futuresBaseURL + "/fapi/v1/ticker/price"
		wsURL = "wss://demo-api.binance.com/stream"          // Combined stream endpoint for user data and market data
	}

	client := &BinanceClient{
		BaseClient:     common.NewBaseClient("Binance", apiKey, apiSecret, testnet),
		baseURL:        baseURL,
		futuresBaseURL: futuresBaseURL,
		wsURL:          wsURL,
		isTestnet:      testnet,
		metrics:        metrics,
		httpTimeout:    binanceHTTPTimeout,
	}

	client.httpClient = createBinanceHTTPClient(apiKey, metrics)
	client.wsClient = NewBinanceWebSocketClient(wsURL, baseURL, apiKey, apiSecret, client.httpClient)
	return client
}

// createBinanceHTTPClient creates a configured HTTP client for Binance API
func createBinanceHTTPClient(apiKey string, metrics *metrics.Metrics) *gohttpcl.Client {
	opts := []gohttpcl.Option{
		gohttpcl.WithMaxRetries(4),
		gohttpcl.WithMinBackoff(150 * time.Millisecond),
		gohttpcl.WithMaxBackoff(15 * time.Second),
		gohttpcl.WithBackoffFactor(2.0),
		gohttpcl.WithBackoffStrategy(gohttpcl.BackoffExponential),
		gohttpcl.WithRetryBudget(0.2, time.Minute),
		gohttpcl.WithTimeout(binanceHTTPTimeout),
		gohttpcl.WithDefaultHeader("X-MBX-APIKEY", apiKey),
	}
	if collector := common.NewHTTPMetricsCollector(metrics, "Binance"); collector != nil {
		opts = append(opts, gohttpcl.WithMetrics(collector))
	}
	return gohttpcl.New(opts...)
}

// getHeaders returns standard API request headers
func (c *BinanceClient) getHeaders() map[string]string {
	return map[string]string{
		"X-MBX-APIKEY": c.APIKey(),
	}
}

// addSignature adds timestamp, recvWindow, and HMAC SHA256 signature to request parameters.
// Uses the default recvWindow of 5000 milliseconds.
func (c *BinanceClient) addSignature(params url.Values) url.Values {
	return c.addSignatureWithRecvWindow(params, 5000)
}

// addSignatureWithRecvWindow adds timestamp, recvWindow, and HMAC SHA256 signature to request parameters.
//
// According to Binance API documentation (https://developers.binance.com/docs/binance-spot-api-docs/testnet/rest-api/request-security):
//   - timestamp: Current timestamp in milliseconds (or microseconds)
//   - recvWindow: Optional receive window in milliseconds (default: 5000, max: 60000)
//     Supports up to 3 decimal places of precision (e.g., 6000.346) for microsecond precision
//   - signature: HMAC SHA256 signature of the payload
//
// Signature computation:
//   - For GET/DELETE requests: signature payload = query string (all parameters URL-encoded)
//   - For POST requests: signature payload = query string + HTTP body (concatenated without separator)
//     Note: Current implementation sends all params in body, so signature = encoded body content
//
// Important:
// - Non-ASCII characters must be percent-encoded before signing (handled by url.Values.Encode())
// - HMAC signatures are case-insensitive (RSA and Ed25519 are case-sensitive)
// - Signature is computed from params WITHOUT the signature field, then added to params
func (c *BinanceClient) addSignatureWithRecvWindow(params url.Values, recvWindow int) url.Values {
	// Add timestamp in milliseconds
	timestamp := fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
	params.Add("timestamp", timestamp)

	// Add recvWindow if specified (defaults to 5000ms if not provided)
	// recvWindow supports up to 3 decimal places for microsecond precision
	if recvWindow > 0 {
		params.Add("recvWindow", strconv.Itoa(recvWindow))
	}

	// Encode params to create the signature payload
	// url.Values.Encode() handles percent-encoding of non-ASCII characters
	payload := params.Encode()

	// Compute HMAC SHA256 signature (case-insensitive for HMAC)
	signature := createHMACSHA256Signature(payload, c.APISecret())

	// Add signature to params (will be included in final request)
	params.Add("signature", signature)
	return params
}

func (c *BinanceClient) doGet(url string) ([]byte, error) {
	return c.doRequest(context.Background(), http.MethodGet, url, nil, c.getHeaders())
}

func (c *BinanceClient) doPost(url string, body []byte, extraHeaders map[string]string) ([]byte, error) {
	headers := c.getHeaders()
	for k, v := range extraHeaders {
		headers[k] = v
	}
	return c.doRequest(context.Background(), http.MethodPost, url, body, headers)
}

func (c *BinanceClient) doDelete(url string) ([]byte, error) {
	return c.doRequest(context.Background(), http.MethodDelete, url, nil, c.getHeaders())
}

func (c *BinanceClient) doRequest(ctx context.Context, method, target string, body []byte, headers map[string]string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := c.httpTimeout
	if timeout <= 0 {
		timeout = binanceHTTPTimeout
	}
	options := headerOptions(headers)
	var (
		resp *http.Response
		err  error
	)
	switch method {
	case http.MethodGet:
		resp, err = c.httpClient.Get(ctx, target, timeout, nil, options...)
	case http.MethodPost:
		resp, err = c.httpClient.Post(ctx, target, bytes.NewReader(body), timeout, nil, options...)
	case http.MethodDelete:
		resp, err = c.httpClient.Delete(ctx, target, timeout, nil, options...)
	default:
		return nil, fmt.Errorf("unsupported HTTP method %s", method)
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	payload, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, common.NewExchangeHTTPError(resp.StatusCode, payload, string(payload))
	}
	return payload, nil
}

func headerOptions(headers map[string]string) []gohttpcl.ReqOption {
	if len(headers) == 0 {
		return nil
	}
	options := make([]gohttpcl.ReqOption, 0, len(headers))
	for k, v := range headers {
		options = append(options, gohttpcl.WithHeader(k, v))
	}
	return options
}

// createHMACSHA256Signature generates an HMAC SHA256 signature.
// According to Binance API documentation, HMAC signatures are case-insensitive
// (unlike RSA and Ed25519 signatures which are case-sensitive).
// Returns the signature as a hexadecimal string.
func createHMACSHA256Signature(payload, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

// apiPath constructs the API path correctly based on whether baseURL already includes "/api"
func (c *BinanceClient) apiPath(version string) string {
	return constructAPIPath(c.baseURL, version)
}

// sapiPath constructs the SAPI path correctly based on whether baseURL already includes "/api".
func (c *BinanceClient) sapiPath(version string) string {
	return constructServicePath(c.baseURL, "sapi", version)
}

// constructAPIPath is a helper function that constructs the API path correctly
// based on whether baseURL already includes "/api"
func constructAPIPath(baseURL, version string) string {
	if strings.HasSuffix(baseURL, "/api") {
		return fmt.Sprintf("%s/%s", baseURL, version)
	}
	return fmt.Sprintf("%s/api/%s", baseURL, version)
}

// constructServicePath builds service paths (api, sapi, etc.) using a base URL that may already include "/api" or "/sapi".
func constructServicePath(baseURL, service, version string) string {
	base := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(base, "/api") {
		base = strings.TrimSuffix(base, "/api")
	} else if strings.HasSuffix(base, "/sapi") {
		base = strings.TrimSuffix(base, "/sapi")
	}
	return fmt.Sprintf("%s/%s/%s", base, service, version)
}

// convertToBinanceSymbol converts symbol format (e.g., "BTC/USDT" to "BTCUSDT")
func convertToBinanceSymbol(symbol string) string {
	return strings.ReplaceAll(strings.ReplaceAll(symbol, "/", ""), "-", "")
}

// convertFromBinanceSymbol converts Binance symbol to standard format
func convertFromBinanceSymbol(binanceSymbol string) string {
	quoteCurrencies := []string{"USDT", "BUSD", "USDC", "BTC", "ETH", "BNB"}
	for _, quote := range quoteCurrencies {
		if strings.HasSuffix(binanceSymbol, quote) {
			base := strings.TrimSuffix(binanceSymbol, quote)
			return base + "/" + quote
		}
	}
	return binanceSymbol
}

// FetchMarketData fetches latest market data for a symbol
func (c *BinanceClient) FetchMarketData(symbol string) (models.MarketData, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/klines", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("interval", "1m")
	params.Add("limit", "1")

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return models.MarketData{}, fmt.Errorf("failed to fetch market data: %w", err)
	}

	var klines [][]interface{}
	if err := json.Unmarshal(response, &klines); err != nil {
		return models.MarketData{}, fmt.Errorf("failed to parse market data: %w", err)
	}

	if len(klines) == 0 {
		return models.MarketData{}, errors.New("no market data available")
	}

	kline := klines[0]
	if len(kline) < 7 {
		return models.MarketData{}, fmt.Errorf("invalid kline data format: expected at least 7 elements, got %d", len(kline))
	}

	// Safely extract openTime
	var openTime int64
	switch v := kline[0].(type) {
	case float64:
		openTime = int64(v)
	case int64:
		openTime = v
	default:
		return models.MarketData{}, fmt.Errorf("invalid kline openTime type: %T", kline[0])
	}

	// Safely extract string values
	openStr, ok := kline[1].(string)
	if !ok {
		return models.MarketData{}, fmt.Errorf("invalid kline open type: %T", kline[1])
	}
	highStr, ok := kline[2].(string)
	if !ok {
		return models.MarketData{}, fmt.Errorf("invalid kline high type: %T", kline[2])
	}
	lowStr, ok := kline[3].(string)
	if !ok {
		return models.MarketData{}, fmt.Errorf("invalid kline low type: %T", kline[3])
	}
	closeStr, ok := kline[4].(string)
	if !ok {
		return models.MarketData{}, fmt.Errorf("invalid kline close type: %T", kline[4])
	}
	volumeStr, ok := kline[5].(string)
	if !ok {
		return models.MarketData{}, fmt.Errorf("invalid kline volume type: %T", kline[5])
	}

	open, _ := strconv.ParseFloat(openStr, 64)
	high, _ := strconv.ParseFloat(highStr, 64)
	low, _ := strconv.ParseFloat(lowStr, 64)
	close, _ := strconv.ParseFloat(closeStr, 64)
	volume, _ := strconv.ParseFloat(volumeStr, 64)

	return models.MarketData{
		Symbol:    symbol,
		Timestamp: time.Unix(openTime/1000, 0),
		Open:      open,
		High:      high,
		Low:       low,
		Close:     close,
		Volume:    volume,
	}, nil
}

// GetTicker returns ticker information for a symbol
func (c *BinanceClient) GetTicker(symbol string) (*models.Ticker, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/ticker/24hr", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ticker: %w", err)
	}

	var tickerResponse struct {
		Symbol    string `json:"symbol"`
		LastPrice string `json:"lastPrice"`
		Volume    string `json:"volume"`
		BidPrice  string `json:"bidPrice"`
		AskPrice  string `json:"askPrice"`
		CloseTime int64  `json:"closeTime"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &tickerResponse); err != nil {
		return nil, fmt.Errorf("failed to parse ticker: %w", err)
	}

	if tickerResponse.Code != 0 {
		return nil, fmt.Errorf("ticker error: %s", tickerResponse.Message)
	}

	lastPrice, _ := strconv.ParseFloat(tickerResponse.LastPrice, 64)
	volume, _ := strconv.ParseFloat(tickerResponse.Volume, 64)
	bidPrice, _ := strconv.ParseFloat(tickerResponse.BidPrice, 64)
	askPrice, _ := strconv.ParseFloat(tickerResponse.AskPrice, 64)

	return &models.Ticker{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		LastPrice: lastPrice,
		Volume:    volume,
		Bid:       bidPrice,
		Ask:       askPrice,
		Timestamp: time.Unix(tickerResponse.CloseTime/1000, 0),
	}, nil
}

// GetCandles returns candlestick data for a symbol
func (c *BinanceClient) GetCandles(symbol, interval string, since time.Time, limit int) ([]models.Candle, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/klines", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("interval", interval)

	// Only add startTime if since is not zero
	if !since.IsZero() {
		params.Add("startTime", strconv.FormatInt(since.UnixNano()/int64(time.Millisecond), 10))
	}

	// Validate and limit the limit parameter (Binance max is 1000)
	if limit <= 0 {
		limit = 500 // Default limit
	}
	if limit > 1000 {
		limit = 1000 // Binance maximum
	}
	params.Add("limit", strconv.Itoa(limit))

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch candles: %w", err)
	}

	var klines [][]interface{}
	if err := json.Unmarshal(response, &klines); err != nil {
		return nil, fmt.Errorf("failed to parse candles: %w", err)
	}

	candles := make([]models.Candle, len(klines))
	for i, kline := range klines {
		if len(kline) < 7 {
			return nil, fmt.Errorf("invalid kline data format: expected at least 7 elements, got %d", len(kline))
		}

		// Safely extract openTime (can be float64 or int64 from JSON)
		var openTime int64
		switch v := kline[0].(type) {
		case float64:
			openTime = int64(v)
		case int64:
			openTime = v
		default:
			return nil, fmt.Errorf("invalid kline openTime type: %T", kline[0])
		}

		// Safely extract string values
		openStr, ok := kline[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid kline open type: %T", kline[1])
		}
		highStr, ok := kline[2].(string)
		if !ok {
			return nil, fmt.Errorf("invalid kline high type: %T", kline[2])
		}
		lowStr, ok := kline[3].(string)
		if !ok {
			return nil, fmt.Errorf("invalid kline low type: %T", kline[3])
		}
		closeStr, ok := kline[4].(string)
		if !ok {
			return nil, fmt.Errorf("invalid kline close type: %T", kline[4])
		}
		volumeStr, ok := kline[5].(string)
		if !ok {
			return nil, fmt.Errorf("invalid kline volume type: %T", kline[5])
		}

		open, _ := strconv.ParseFloat(openStr, 64)
		high, _ := strconv.ParseFloat(highStr, 64)
		low, _ := strconv.ParseFloat(lowStr, 64)
		close, _ := strconv.ParseFloat(closeStr, 64)
		volume, _ := strconv.ParseFloat(volumeStr, 64)

		// Safely extract closeTime
		var closeTime int64
		switch v := kline[6].(type) {
		case float64:
			closeTime = int64(v)
		case int64:
			closeTime = v
		default:
			return nil, fmt.Errorf("invalid kline closeTime type: %T", kline[6])
		}

		candles[i] = models.Candle{
			Exchange:  c.GetName(),
			Symbol:    symbol,
			Interval:  interval,
			OpenTime:  time.Unix(openTime/1000, 0),
			CloseTime: time.Unix(closeTime/1000, 0),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
		}
	}

	return candles, nil
}

// GetTrades returns trade history for a symbol
func (c *BinanceClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/trades", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("limit", strconv.Itoa(limit))

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trades: %w", err)
	}

	var tradesResponse []struct {
		ID           int64  `json:"id"`
		Price        string `json:"price"`
		Qty          string `json:"qty"`
		Time         int64  `json:"time"`
		IsBuyerMaker bool   `json:"isBuyerMaker"`
	}

	if err := json.Unmarshal(response, &tradesResponse); err != nil {
		return nil, fmt.Errorf("failed to parse trades: %w", err)
	}

	trades := make([]models.Trade, len(tradesResponse))
	for i, trade := range tradesResponse {
		price, _ := strconv.ParseFloat(trade.Price, 64)
		quantity, _ := strconv.ParseFloat(trade.Qty, 64)
		// isBuyerMaker indicates the buy side was the maker (limit order).
		// When true, the trade was initiated by a market sell order (taker is seller).
		// When false, the trade was initiated by a market buy order (taker is buyer).
		side := "buy"
		if trade.IsBuyerMaker {
			side = "sell"
		}

		trades[i] = models.Trade{
			Exchange:      c.GetName(),
			Symbol:        symbol,
			ID:            strconv.FormatInt(trade.ID, 10),
			Type:          side,
			Price:         price,
			Quantity:      quantity,
			ExecutionTime: time.Unix(trade.Time/1000, 0),
		}
	}

	return trades, nil
}

// GetOrderBook returns the order book for a symbol
// Binance supports depths: 5, 10, 20, 50, 100, 500, 1000, 5000
func (c *BinanceClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/depth", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	// Validate and normalize depth to Binance-supported values
	if depth <= 0 {
		depth = 20 // Default depth
	}
	// Binance supports: 5, 10, 20, 50, 100, 500, 1000, 5000
	// Round to nearest supported value
	validDepths := []int{5, 10, 20, 50, 100, 500, 1000, 5000}
	closestDepth := validDepths[0]
	for _, validDepth := range validDepths {
		if depth >= validDepth {
			closestDepth = validDepth
		} else {
			break
		}
	}
	if depth > 5000 {
		closestDepth = 5000
	}
	params.Add("limit", strconv.Itoa(closestDepth))

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to get order book: %w", err)
	}

	var orderBookResp struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}

	if err := json.Unmarshal(response, &orderBookResp); err != nil {
		return nil, fmt.Errorf("failed to parse order book: %w", err)
	}

	orderBook := &models.OrderBook{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		Timestamp: time.Now(),
		Bids:      make([]models.OrderBookEntry, len(orderBookResp.Bids)),
		Asks:      make([]models.OrderBookEntry, len(orderBookResp.Asks)),
	}

	for i, bid := range orderBookResp.Bids {
		if len(bid) >= 2 {
			price, _ := strconv.ParseFloat(bid[0], 64)
			quantity, _ := strconv.ParseFloat(bid[1], 64)
			orderBook.Bids[i] = models.OrderBookEntry{
				Price:  price,
				Amount: quantity,
			}
		}
	}

	for i, ask := range orderBookResp.Asks {
		if len(ask) >= 2 {
			price, _ := strconv.ParseFloat(ask[0], 64)
			quantity, _ := strconv.ParseFloat(ask[1], 64)
			orderBook.Asks[i] = models.OrderBookEntry{
				Price:  price,
				Amount: quantity,
			}
		}
	}

	return orderBook, nil
}

// CreateOrder places a spot market order.
// Use test=true to validate the order without placing it (uses /api/v3/order/test endpoint).
// recvWindow specifies the receive window in milliseconds (default: 5000).
func (c *BinanceClient) CreateOrder(symbol string, side common.OrderSide, orderType common.OrderType, amount, price float64) (*common.Order, error) {
	return c.CreateOrderWithOptions(symbol, side, orderType, amount, price, "", false, 5000)
}

// CreateOrderWithOptions places a spot market order with additional options.
// test: if true, uses /api/v3/order/test endpoint to validate without placing a real order.
// clientOrderID: optional custom client order ID (newClientOrderId parameter).
// recvWindow: receive window in milliseconds (default: 5000, set to 0 to use default, max 60000).
func (c *BinanceClient) CreateOrderWithOptions(symbol string, side common.OrderSide, orderType common.OrderType, amount, price float64, clientOrderID string, test bool, recvWindow int) (*common.Order, error) {
	return c.CreateOrderAdvanced(symbol, side, orderType, amount, price, 0, clientOrderID, test, recvWindow)
}

// CreateOrderAdvanced places a spot market order with full control over all parameters.
// For MARKET buy orders, you can use quoteOrderQty instead of quantity to specify
// how much quote currency to spend (e.g., buy $100 worth of BTC).
// Parameters:
//   - symbol: trading pair (e.g., "BTC/USDT")
//   - side: BUY or SELL
//   - orderType: MARKET or LIMIT
//   - amount: quantity of base asset (ignored if quoteOrderQty > 0 for MARKET buy)
//   - price: limit price (required for LIMIT orders, ignored for MARKET orders)
//   - quoteOrderQty: quote quantity for MARKET buy orders (e.g., 100.0 to buy $100 worth)
//   - clientOrderID: optional custom client order ID
//   - test: if true, uses /api/v3/order/test endpoint to validate without placing a real order
//   - recvWindow: receive window in milliseconds (default: 5000, max: 60000)
func (c *BinanceClient) CreateOrderAdvanced(symbol string, side common.OrderSide, orderType common.OrderType, amount, price, quoteOrderQty float64, clientOrderID string, test bool, recvWindow int) (*common.Order, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpointPath := "order"
	if test {
		endpointPath = "order/test"
	}
	endpoint := fmt.Sprintf("%s/%s", c.apiPath("v3"), endpointPath)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("side", strings.ToUpper(side.String()))
	params.Add("type", strings.ToUpper(orderType.String()))

	// For LIMIT orders, timeInForce is required
	if strings.EqualFold(orderType.String(), common.OrderTypeLimit.String()) {
		params.Add("timeInForce", string(common.TimeInForceGTC))
		if price <= 0 {
			return nil, fmt.Errorf("price is required for limit orders")
		}
		params.Add("price", strconv.FormatFloat(price, 'f', -1, 64))
	}

	// For MARKET orders:
	// - BUY: can use either quantity (base asset) or quoteOrderQty (quote currency)
	// - SELL: must use quantity (base asset)
	isMarketBuy := strings.EqualFold(orderType.String(), common.OrderTypeMarket.String()) && side == common.OrderSideBuy

	if isMarketBuy && quoteOrderQty > 0 {
		// Market buy with fixed quote amount (e.g., buy $100 worth of BTC)
		params.Add("quoteOrderQty", strconv.FormatFloat(quoteOrderQty, 'f', -1, 64))
	} else {
		// Use quantity (base asset amount)
		quantity := amount
		if quantity <= 0 {
			return nil, fmt.Errorf("order quantity must be greater than 0")
		}
		params.Add("quantity", strconv.FormatFloat(quantity, 'f', -1, 64))
	}

	// Add custom client order ID if provided
	if clientOrderID != "" {
		params.Add("newClientOrderId", clientOrderID)
	}

	// Validate and set recvWindow (default: 5000, max: 60000)
	if recvWindow <= 0 {
		recvWindow = 5000
	} else if recvWindow > 60000 {
		recvWindow = 60000
	}
	params = c.addSignatureWithRecvWindow(params, recvWindow)
	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		// Check if this is a 404 error on the test endpoint
		if test {
			var httpErr *common.ExchangeError
			if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
				return nil, fmt.Errorf("the /order/test endpoint is not available in the demo environment. "+
					"Demo accounts may not support order validation. "+
					"Try using test=false to place an actual order in the demo account, "+
					"or use testnet.binance.vision for full test endpoint support. Original error: %w", err)
			}
		}
		return nil, fmt.Errorf("failed to place order: %w", err)
	}

	var orderResponse struct {
		Symbol                  string `json:"symbol"`
		OrderID                 int64  `json:"orderId"`
		ClientOrderID           string `json:"clientOrderId"`
		Price                   string `json:"price"`
		OrigQty                 string `json:"origQty"`
		ExecutedQty             string `json:"executedQty"`
		CumulativeQuoteQuantity string `json:"cummulativeQuoteQty"`
		Status                  string `json:"status"`
		TimeInForce             string `json:"timeInForce"`
		Type                    string `json:"type"`
		Side                    string `json:"side"`
		Time                    int64  `json:"transactTime"`
		UpdateTime              int64  `json:"updateTime"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &orderResponse); err != nil {
		return nil, fmt.Errorf("failed to parse order response: %w", err)
	}

	if orderResponse.Code != 0 {
		return nil, fmt.Errorf("order error: %s", orderResponse.Message)
	}

	// For test orders, don't try to fetch the order since it doesn't actually exist
	// The test endpoint only validates the order parameters
	if test {
		// Test endpoint may return minimal data, so construct order from what we have
		// Use a default status if not provided
		statusStr := orderResponse.Status
		if statusStr == "" {
			statusStr = "NEW" // Default status for test orders
		}
		status, err := binanceStatusToCommon(statusStr)
		if err != nil {
			// If status conversion fails, use NEW as default
			status = common.OrderStatusNew
		}

		orderPrice, _ := strconv.ParseFloat(orderResponse.Price, 64)
		orderAmount, _ := strconv.ParseFloat(orderResponse.OrigQty, 64)
		filled, _ := strconv.ParseFloat(orderResponse.ExecutedQty, 64)
		symbolFormatted := symbol
		if orderResponse.Symbol != "" {
			symbolFormatted = convertFromBinanceSymbol(orderResponse.Symbol)
		}

		orderTime := orderResponse.Time
		if orderTime == 0 {
			orderTime = time.Now().UnixNano() / int64(time.Millisecond)
		}
		updateTime := orderResponse.UpdateTime
		if updateTime == 0 {
			updateTime = orderTime
		}

		orderID := ""
		if orderResponse.OrderID > 0 {
			orderID = strconv.FormatInt(orderResponse.OrderID, 10)
		}

		return &common.Order{
			ID:              orderID,
			ClientOrderID:   orderResponse.ClientOrderID,
			Symbol:          symbolFormatted,
			Side:            side,
			Type:            orderType,
			Status:          status,
			Price:           orderPrice,
			Amount:          orderAmount,
			FilledAmount:    filled,
			RemainingAmount: orderAmount - filled,
			CreatedAt:       time.Unix(orderTime/1000, 0),
			UpdatedAt:       time.Unix(updateTime/1000, 0),
			Quantity:        orderAmount,
			Timestamp:       time.Unix(orderTime/1000, 0),
		}, nil
	}

	// If the response doesn't have all fields, fetch the complete order
	if orderResponse.Status == "" {
		orderID := strconv.FormatInt(orderResponse.OrderID, 10)
		return c.GetOrder(symbol, orderID)
	}

	status, err := binanceStatusToCommon(orderResponse.Status)
	if err != nil {
		return nil, err
	}

	orderPrice, _ := strconv.ParseFloat(orderResponse.Price, 64)
	orderAmount, _ := strconv.ParseFloat(orderResponse.OrigQty, 64)
	filled, _ := strconv.ParseFloat(orderResponse.ExecutedQty, 64)
	symbolFormatted := symbol
	if orderResponse.Symbol != "" {
		symbolFormatted = convertFromBinanceSymbol(orderResponse.Symbol)
	}

	orderTime := orderResponse.Time
	if orderTime == 0 {
		orderTime = time.Now().UnixNano() / int64(time.Millisecond)
	}
	updateTime := orderResponse.UpdateTime
	if updateTime == 0 {
		updateTime = orderTime
	}

	return &common.Order{
		ID:              strconv.FormatInt(orderResponse.OrderID, 10),
		ClientOrderID:   orderResponse.ClientOrderID,
		Symbol:          symbolFormatted,
		Side:            side,
		Type:            orderType,
		Status:          status,
		Price:           orderPrice,
		Amount:          orderAmount,
		FilledAmount:    filled,
		RemainingAmount: orderAmount - filled,
		CreatedAt:       time.Unix(orderTime/1000, 0),
		UpdatedAt:       time.Unix(updateTime/1000, 0),
		Quantity:        orderAmount,
		Timestamp:       time.Unix(orderTime/1000, 0),
	}, nil
}

// PlaceFuturesOrder places a futures order
func (c *BinanceClient) PlaceFuturesOrder(order FuturesOrder) (string, error) {
	binanceSymbol := convertToBinanceSymbol(order.Symbol)
	endpoint := fmt.Sprintf("%s/fapi/v1/order", c.futuresBaseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("side", strings.ToUpper(order.Side.String()))
	params.Add("type", strings.ToUpper(order.Type.String()))

	if strings.EqualFold(order.Type.String(), common.OrderTypeLimit.String()) {
		params.Add("timeInForce", string(common.TimeInForceGTC))
	}

	quantity := order.Amount
	if quantity == 0 {
		quantity = order.Quantity
	}
	if quantity <= 0 {
		return "", fmt.Errorf("order quantity must be greater than 0")
	}
	params.Add("quantity", strconv.FormatFloat(quantity, 'f', -1, 64))

	if order.Price > 0 {
		params.Add("price", strconv.FormatFloat(order.Price, 'f', -1, 64))
	}

	params.Add("reduceOnly", strconv.FormatBool(order.ReduceOnly))
	if order.PositionSide != "" {
		params.Add("positionSide", order.PositionSide)
	}

	params = c.addSignature(params)
	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		return "", fmt.Errorf("failed to place futures order: %w", err)
	}

	var orderResponse struct {
		OrderID int64 `json:"orderId"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &orderResponse); err != nil {
		return "", fmt.Errorf("failed to parse futures order response: %w", err)
	}

	if orderResponse.Code != 0 {
		return "", fmt.Errorf("futures order error: %s", orderResponse.Message)
	}

	return strconv.FormatInt(orderResponse.OrderID, 10), nil
}

// CancelOrder cancels a spot market order
func (c *BinanceClient) CancelOrder(symbol, orderID string) error {
	endpoint := fmt.Sprintf("%s/order", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", convertToBinanceSymbol(symbol))
	params.Add("orderId", orderID)

	params = c.addSignature(params)
	response, err := c.doDelete(endpoint + "?" + params.Encode())
	if err != nil {
		return fmt.Errorf("failed to cancel order: %w", err)
	}

	var orderResponse BinanceResponse
	if err := json.Unmarshal(response, &orderResponse); err != nil {
		return fmt.Errorf("failed to parse cancel response: %w", err)
	}

	if orderResponse.Code != 0 {
		return fmt.Errorf("cancel error: %s", orderResponse.Message)
	}

	return nil
}

func binanceStatusToCommon(status string) (common.OrderStatus, error) {
	switch strings.ToUpper(status) {
	case "NEW":
		return common.OrderStatusNew, nil
	case "PARTIALLY_FILLED":
		return common.OrderStatusPartiallyFilled, nil
	case "FILLED":
		return common.OrderStatusFilled, nil
	case "CANCELED", "PENDING_CANCEL", "REJECTED":
		return common.OrderStatusCancelled, nil
	case "EXPIRED", "EXPIRED_IN_MATCH":
		return common.OrderStatusExpired, nil
	default:
		return "", fmt.Errorf("unknown order status: %s", status)
	}
}

// GetOrder fetches a specific order by symbol and ID.
func (c *BinanceClient) GetOrder(symbol, orderID string) (*common.Order, error) {
	if strings.TrimSpace(symbol) == "" {
		return nil, fmt.Errorf("symbol is required to query an order")
	}
	if strings.TrimSpace(orderID) == "" {
		return nil, fmt.Errorf("orderID is required to query an order")
	}
	endpoint := fmt.Sprintf("%s/order", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", convertToBinanceSymbol(symbol))
	params.Add("orderId", orderID)
	params = c.addSignature(params)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	var orderResp struct {
		Symbol                  string `json:"symbol"`
		OrderID                 int64  `json:"orderId"`
		ClientOrderID           string `json:"clientOrderId"`
		Price                   string `json:"price"`
		OrigQty                 string `json:"origQty"`
		ExecutedQty             string `json:"executedQty"`
		CumulativeQuoteQuantity string `json:"cummulativeQuoteQty"`
		Status                  string `json:"status"`
		TimeInForce             string `json:"timeInForce"`
		Type                    string `json:"type"`
		Side                    string `json:"side"`
		StopPrice               string `json:"stopPrice"`
		IcebergQuantity         string `json:"icebergQty"`
		Time                    int64  `json:"time"`
		UpdateTime              int64  `json:"updateTime"`
		IsWorking               bool   `json:"isWorking"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &orderResp); err != nil {
		return nil, fmt.Errorf("failed to parse order response: %w", err)
	}

	if orderResp.Code != 0 {
		return nil, fmt.Errorf("order error: %s", orderResp.Message)
	}

	status, err := binanceStatusToCommon(orderResp.Status)
	if err != nil {
		return nil, err
	}

	price, _ := strconv.ParseFloat(orderResp.Price, 64)
	amount, _ := strconv.ParseFloat(orderResp.OrigQty, 64)
	filled, _ := strconv.ParseFloat(orderResp.ExecutedQty, 64)
	symbolFormatted := symbol
	if orderResp.Symbol != "" {
		symbolFormatted = convertFromBinanceSymbol(orderResp.Symbol)
	}

	return &common.Order{
		ID:              strconv.FormatInt(orderResp.OrderID, 10),
		ClientOrderID:   orderResp.ClientOrderID,
		Symbol:          symbolFormatted,
		Side:            common.OrderSideFromString(strings.ToLower(orderResp.Side)),
		Type:            common.OrderTypeFromString(strings.ToLower(orderResp.Type)),
		Status:          status,
		Price:           price,
		Amount:          amount,
		FilledAmount:    filled,
		RemainingAmount: amount - filled,
		CreatedAt:       time.Unix(orderResp.Time/1000, 0),
		UpdatedAt:       time.Unix(orderResp.UpdateTime/1000, 0),
		Quantity:        amount,
		Timestamp:       time.Unix(orderResp.Time/1000, 0),
	}, nil
}

// GetOrderStatus retrieves the status of an order
func (c *BinanceClient) GetOrderStatus(symbol, orderID string) (common.OrderStatus, error) {
	order, err := c.GetOrder(symbol, orderID)
	if err != nil {
		return "", err
	}
	return order.Status, nil
}

// GetBalance returns the balance for a specific asset
func (c *BinanceClient) GetBalance(asset string) (*common.Balance, error) {
	balances, err := c.GetBalances()
	if err != nil {
		return nil, err
	}
	return balances[asset], nil
}

// GetBalances returns all account balances
func (c *BinanceClient) GetBalances() (map[string]*common.Balance, error) {
	endpoint := fmt.Sprintf("%s/account", c.apiPath("v3"))
	params := url.Values{}
	params = c.addSignature(params)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to get account info: %w", err)
	}

	var accountInfo struct {
		Balances []common.Balance `json:"balances"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &accountInfo); err != nil {
		return nil, fmt.Errorf("failed to parse account info: %w", err)
	}

	if accountInfo.Code != 0 {
		return nil, fmt.Errorf("account error: %s", accountInfo.Message)
	}

	balances := make(map[string]*common.Balance)
	for _, balance := range accountInfo.Balances {
		balances[balance.Asset] = &balance
	}

	return balances, nil
}

// GetSpotPositions returns all open spot positions, excluding dust and non-tradeable positions
func (c *BinanceClient) GetSpotPositions() ([]SpotPosition, error) {
	// Get all balances
	balances, err := c.GetBalances()
	if err != nil {
		return nil, fmt.Errorf("failed to get balances: %w", err)
	}

	// Get tradeable assets from exchange info
	tradingPairs, err := c.GetTradingPairs()
	if err != nil {
		return nil, fmt.Errorf("failed to get trading pairs: %w", err)
	}

	// Create set of tradeable assets
	tradeableAssets := make(map[string]bool)
	for _, pair := range tradingPairs {
		tradeableAssets[pair.BaseAsset] = true
		tradeableAssets[pair.QuoteAsset] = true
	}

	var positions []SpotPosition

	for asset, balance := range balances {
		// Parse free and locked amounts
		free, err := strconv.ParseFloat(balance.Free, 64)
		if err != nil {
			continue // Skip invalid balances
		}

		locked, err := strconv.ParseFloat(balance.Locked, 64)
		if err != nil {
			continue // Skip invalid balances
		}

		total := free + locked

		// Skip zero balances
		if total == 0 {
			continue
		}

		// Check if this is dust (very small amount)
		isDust := c.isDustPosition(asset, total)

		// Check if asset is tradeable
		isTradeable := tradeableAssets[asset]

		position := SpotPosition{
			Asset:       asset,
			Free:        free,
			Locked:      locked,
			Total:       total,
			IsDust:      isDust,
			IsTradeable: isTradeable,
		}

		// Only include non-dust positions (user wants to exclude dust and non-tradeable)
		if !isDust {
			positions = append(positions, position)
		}
	}

	return positions, nil
}

// isDustPosition determines if a position is considered dust based on asset type and amount
func (c *BinanceClient) isDustPosition(asset string, amount float64) bool {
	// Define dust thresholds for different asset types
	// These are approximate minimum tradeable amounts for Binance
	dustThresholds := map[string]float64{
		"BTC":  0.00001, // ~$0.50 at $50k BTC
		"ETH":  0.0001,  // ~$0.03 at $300 ETH
		"BNB":  0.001,   // ~$0.30 at $300 BNB
		"USDT": 1.0,     // $1 minimum
		"USDC": 1.0,     // $1 minimum
		"BUSD": 1.0,     // $1 minimum
	}

	// For assets not in the map, use a very small threshold
	threshold, exists := dustThresholds[asset]
	if !exists {
		threshold = 0.00000001 // Very small amount for unknown assets
	}

	return amount < threshold
}

// GetDustConvertibleAssets retrieves the list of small balances eligible for dust conversion.
// This uses Binance's dust-convert endpoint (SAPI). The target asset is typically "USDT" or "BNB".
func (c *BinanceClient) GetDustConvertibleAssets(targetAsset string) (*BinanceDustConvertibleAssetsResponse, error) {
	if c.IsTestnet() {
		return nil, fmt.Errorf("dust conversion is not available on Binance demo/testnet endpoints")
	}
	target := strings.ToUpper(strings.TrimSpace(targetAsset))
	if target == "" {
		return nil, fmt.Errorf("target asset is required")
	}
	endpoint := fmt.Sprintf("%s/asset/dust-convert/query-convertible-assets", c.sapiPath("v1"))
	params := url.Values{}
	params.Add("targetAsset", target)
	params = c.addSignature(params)

	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query dust convertible assets: %w", err)
	}

	var resp BinanceDustConvertibleAssetsResponse
	if err := json.Unmarshal(response, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse dust convertible assets response: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("dust convertible assets error: %s", resp.Message)
	}
	return &resp, nil
}

// ConvertDustToAsset converts the provided dust assets into the target asset (e.g., USDT).
func (c *BinanceClient) ConvertDustToAsset(assets []string, targetAsset string) (*BinanceDustConvertResult, error) {
	if c.IsTestnet() {
		return nil, fmt.Errorf("dust conversion is not available on Binance demo/testnet endpoints")
	}
	if len(assets) == 0 {
		return nil, fmt.Errorf("at least one asset is required for dust conversion")
	}
	target := strings.ToUpper(strings.TrimSpace(targetAsset))
	params := url.Values{}
	if target != "" {
		params.Add("targetAsset", target)
	}

	unique := make(map[string]struct{}, len(assets))
	deduped := make([]string, 0, len(assets))
	for _, asset := range assets {
		normalized := strings.ToUpper(strings.TrimSpace(asset))
		if normalized == "" {
			continue
		}
		if _, exists := unique[normalized]; exists {
			continue
		}
		unique[normalized] = struct{}{}
		deduped = append(deduped, normalized)
	}
	if len(deduped) == 0 {
		return nil, fmt.Errorf("no valid assets provided for dust conversion")
	}
	// Binance expects ARRAY parameters as multiple values with the same key
	// (e.g., asset=BTC&asset=ETH), not comma-separated (asset=BTC,ETH)
	for _, asset := range deduped {
		params.Add("asset", asset)
	}
	params = c.addSignature(params)

	endpoint := fmt.Sprintf("%s/asset/dust-convert/convert", c.sapiPath("v1"))
	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to convert dust assets: %w", err)
	}

	var resp BinanceDustConvertResult
	if err := json.Unmarshal(response, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse dust convert response: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("dust convert error: %s", resp.Message)
	}
	return &resp, nil
}

// ConvertAllDustToUSDT converts all eligible dust balances to USDT in one request.
func (c *BinanceClient) ConvertAllDustToUSDT() (*BinanceDustConvertResult, error) {
	eligible, err := c.GetDustConvertibleAssets("USDT")
	if err != nil {
		return nil, err
	}
	if len(eligible.Details) == 0 {
		return nil, fmt.Errorf("no dust assets eligible for conversion to USDT")
	}

	assets := make([]string, 0, len(eligible.Details))
	for _, detail := range eligible.Details {
		if detail.Asset == "" {
			continue
		}
		assets = append(assets, detail.Asset)
	}
	if len(assets) == 0 {
		return nil, fmt.Errorf("no dust assets eligible for conversion to USDT")
	}
	return c.ConvertDustToAsset(assets, "USDT")
}

// GetOpenOrders retrieves all open orders for a symbol
func (c *BinanceClient) GetOpenOrders(symbol string) ([]common.Order, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/openOrders", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params = c.addSignature(params)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to get open orders: %w", err)
	}

	var openOrders []struct {
		Symbol  string `json:"symbol"`
		OrderID int64  `json:"orderId"`
		Price   string `json:"price"`
		OrigQty string `json:"origQty"`
		Side    string `json:"side"`
		Type    string `json:"type"`
		Time    int64  `json:"time"`
	}

	if err := json.Unmarshal(response, &openOrders); err != nil {
		return nil, fmt.Errorf("failed to parse open orders: %w", err)
	}

	orders := make([]common.Order, len(openOrders))
	for i, o := range openOrders {
		price, _ := strconv.ParseFloat(o.Price, 64)
		quantity, _ := strconv.ParseFloat(o.OrigQty, 64)

		orders[i] = common.Order{
			ID:        strconv.FormatInt(o.OrderID, 10),
			Symbol:    symbol,
			Side:      common.OrderSideFromString(strings.ToLower(o.Side)),
			Type:      common.OrderTypeFromString(strings.ToLower(o.Type)),
			Amount:    quantity,
			Price:     price,
			CreatedAt: time.Unix(o.Time/1000, 0),
			Quantity:  quantity,
			Timestamp: time.Unix(o.Time/1000, 0),
		}
	}

	return orders, nil
}

// GetOrders retrieves order history for a symbol
func (c *BinanceClient) GetOrders(symbol string, since time.Time, limit int) ([]common.Order, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/allOrders", c.apiPath("v3"))
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	// Only add startTime if since is not zero
	if !since.IsZero() {
		params.Add("startTime", strconv.FormatInt(since.UnixNano()/int64(time.Millisecond), 10))
	}

	// Validate limit (Binance max is 1000)
	if limit <= 0 {
		limit = 500 // Default limit
	}
	if limit > 1000 {
		limit = 1000 // Binance maximum
	}
	params.Add("limit", strconv.Itoa(limit))
	params = c.addSignature(params)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to get orders: %w", err)
	}

	var ordersResponse []struct {
		Symbol  string `json:"symbol"`
		OrderID int64  `json:"orderId"`
		Price   string `json:"price"`
		OrigQty string `json:"origQty"`
		Side    string `json:"side"`
		Type    string `json:"type"`
		Time    int64  `json:"time"`
	}

	if err := json.Unmarshal(response, &ordersResponse); err != nil {
		return nil, fmt.Errorf("failed to parse orders: %w", err)
	}

	orders := make([]common.Order, len(ordersResponse))
	for i, order := range ordersResponse {
		price, _ := strconv.ParseFloat(order.Price, 64)
		quantity, _ := strconv.ParseFloat(order.OrigQty, 64)

		orders[i] = common.Order{
			ID:        strconv.FormatInt(order.OrderID, 10),
			Symbol:    symbol,
			Side:      common.OrderSideFromString(strings.ToLower(order.Side)),
			Type:      common.OrderTypeFromString(strings.ToLower(order.Type)),
			Amount:    quantity,
			Price:     price,
			CreatedAt: time.Unix(order.Time/1000, 0),
			Quantity:  quantity,
			Timestamp: time.Unix(order.Time/1000, 0),
		}
	}

	return orders, nil
}

// getTaggedCoinsFromWeb scrapes Binance web pages to get lists of coins tagged as Monitoring or Seed.
// Returns sets of base asset symbols (e.g., "BTC", "ETH") that should be excluded.
func getTaggedCoinsFromWeb(ctx context.Context, httpClient *gohttpcl.Client) (monitoringCoins, seedCoins map[string]bool, err error) {
	monitoringCoins = make(map[string]bool)
	seedCoins = make(map[string]bool)

	// Scrape Monitoring page
	monitoringURL := "https://www.binance.com/en/markets/coinInfo-Monitoring"
	monitoringSymbols, err := scrapeBinanceTagPage(ctx, httpClient, monitoringURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scrape Monitoring page: %w", err)
	}
	for _, symbol := range monitoringSymbols {
		monitoringCoins[strings.ToUpper(symbol)] = true
	}

	// Scrape Seed page
	seedURL := "https://www.binance.com/en/markets/coinInfo-Seed"
	seedSymbols, err := scrapeBinanceTagPage(ctx, httpClient, seedURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scrape Seed page: %w", err)
	}
	for _, symbol := range seedSymbols {
		seedCoins[strings.ToUpper(symbol)] = true
	}

	return monitoringCoins, seedCoins, nil
}

// scrapeBinanceTagPage scrapes a Binance tag page and extracts coin symbols.
// The page structure may vary, so this function tries multiple parsing strategies.
func scrapeBinanceTagPage(ctx context.Context, httpClient *gohttpcl.Client, url string) ([]string, error) {
	timeout := binanceHTTPTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	// Set User-Agent to avoid being blocked
	headers := map[string]string{
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
		"Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	}

	options := headerOptions(headers)
	resp, err := httpClient.Get(ctx, url, timeout, nil, options...)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("HTTP error: status code %d", resp.StatusCode)
	}

	// Parse HTML
	doc, err := html.Parse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	// Extract symbols from the page
	// Binance pages typically have coin symbols in various formats.
	// We'll look for common patterns like data attributes, class names, or text content.
	var symbols []string
	symbolSet := make(map[string]bool)

	// Strategy 1: Look for data-symbol or similar attributes
	var extractFromNode func(*html.Node)
	extractFromNode = func(n *html.Node) {
		if n.Type == html.ElementNode {
			// Check for data-symbol attribute
			for _, attr := range n.Attr {
				if attr.Key == "data-symbol" || attr.Key == "data-base-asset" {
					symbol := strings.TrimSpace(attr.Val)
					if symbol != "" && !symbolSet[symbol] {
						symbols = append(symbols, symbol)
						symbolSet[symbol] = true
					}
				}
			}

			// Check for text content that looks like a coin symbol (2-10 uppercase letters/numbers)
			if n.FirstChild != nil && n.FirstChild.Type == html.TextNode {
				text := strings.TrimSpace(n.FirstChild.Data)
				// Match patterns like "BTC", "ETH", "USDT", etc. (2-10 alphanumeric uppercase)
				if len(text) >= 2 && len(text) <= 10 {
					upperText := strings.ToUpper(text)
					if isCoinSymbol(upperText) && !symbolSet[upperText] {
						// Only add if it's in a likely context (e.g., within a table cell, link, or span)
						if isSymbolContext(n) {
							symbols = append(symbols, upperText)
							symbolSet[upperText] = true
						}
					}
				}
			}
		}

		// Recursively process children
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			extractFromNode(child)
		}
	}

	extractFromNode(doc)

	// Strategy 2: Look for JSON data embedded in script tags
	// Many modern web pages load data via JavaScript, which might be in script tags
	var extractFromScripts func(*html.Node)
	extractFromScripts = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "script" {
			if n.FirstChild != nil && n.FirstChild.Type == html.TextNode {
				scriptContent := n.FirstChild.Data
				// Try to find JSON arrays or objects containing symbol data
				// Look for patterns like ["BTC","ETH","USDT"] or {"symbol":"BTC"}
				symbolsFromScript := extractSymbolsFromScript(scriptContent)
				for _, sym := range symbolsFromScript {
					if !symbolSet[sym] {
						symbols = append(symbols, sym)
						symbolSet[sym] = true
					}
				}
			}
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			extractFromScripts(child)
		}
	}

	extractFromScripts(doc)

	if len(symbols) == 0 {
		// Fallback: try to find any text that looks like a coin symbol in the page
		// This is a last resort and may include false positives
		var fallbackExtract func(*html.Node)
		fallbackExtract = func(n *html.Node) {
			if n.Type == html.TextNode {
				text := strings.TrimSpace(n.Data)
				words := strings.Fields(text)
				for _, word := range words {
					upperWord := strings.ToUpper(word)
					if isCoinSymbol(upperWord) && len(upperWord) >= 2 && len(upperWord) <= 10 {
						if !symbolSet[upperWord] {
							symbols = append(symbols, upperWord)
							symbolSet[upperWord] = true
						}
					}
				}
			}
			for child := n.FirstChild; child != nil; child = child.NextSibling {
				fallbackExtract(child)
			}
		}
		fallbackExtract(doc)
	}

	return symbols, nil
}

// isCoinSymbol checks if a string looks like a valid coin symbol
func isCoinSymbol(s string) bool {
	if len(s) < 2 || len(s) > 10 {
		return false
	}
	// Coin symbols are typically uppercase alphanumeric
	for _, r := range s {
		if !((r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

// isSymbolContext checks if a node is in a context where a coin symbol is likely to appear
func isSymbolContext(n *html.Node) bool {
	tagName := strings.ToLower(n.Data)
	// Common tags where symbols appear
	symbolTags := map[string]bool{
		"td": true, "th": true, "span": true, "div": true,
		"a": true, "p": true, "li": true, "strong": true,
	}
	return symbolTags[tagName]
}

// extractSymbolsFromScript tries to extract coin symbols from JavaScript/JSON in script tags
func extractSymbolsFromScript(scriptContent string) []string {
	var symbols []string
	symbolSet := make(map[string]bool)

	// Strategy 1: Look for JSON arrays like ["BTC","ETH","USDT"]
	// Find patterns like ["SYMBOL","SYMBOL"] or ['SYMBOL','SYMBOL']
	start := 0
	for {
		// Find array start
		arrayStart := strings.Index(scriptContent[start:], `["`)
		if arrayStart == -1 {
			arrayStart = strings.Index(scriptContent[start:], `['`)
		}
		if arrayStart == -1 {
			break
		}
		arrayStart += start

		// Find array end
		arrayEnd := strings.Index(scriptContent[arrayStart:], `]`)
		if arrayEnd == -1 {
			start = arrayStart + 1
			continue
		}
		arrayEnd += arrayStart + 1

		// Extract array content
		arrayContent := scriptContent[arrayStart:arrayEnd]

		// Parse quoted strings from the array
		// Look for patterns like "SYMBOL" or 'SYMBOL'
		quoteStart := 0
		for quoteStart < len(arrayContent) {
			quoteIdx := strings.IndexAny(arrayContent[quoteStart:], `"'`)
			if quoteIdx == -1 {
				break
			}
			quoteIdx += quoteStart
			quoteChar := arrayContent[quoteIdx]

			// Find closing quote
			closeQuote := strings.Index(arrayContent[quoteIdx+1:], string(quoteChar))
			if closeQuote == -1 {
				break
			}
			closeQuote += quoteIdx + 1

			// Extract symbol
			symbol := arrayContent[quoteIdx+1 : closeQuote]
			symbol = strings.TrimSpace(symbol)
			if isCoinSymbol(symbol) && !symbolSet[symbol] {
				symbols = append(symbols, symbol)
				symbolSet[symbol] = true
			}

			quoteStart = closeQuote + 1
		}
		start = arrayEnd
	}

	// Strategy 2: Look for JSON objects with symbol fields
	// Find patterns like {"symbol":"BTC"} or {symbol:'ETH'}
	objStart := 0
	for {
		objIdx := strings.Index(scriptContent[objStart:], `"symbol"`)
		if objIdx == -1 {
			objIdx = strings.Index(scriptContent[objStart:], `'symbol'`)
		}
		if objIdx == -1 {
			break
		}
		objIdx += objStart

		// Find the value after the colon
		colonIdx := strings.Index(scriptContent[objIdx:], `:`)
		if colonIdx == -1 {
			break
		}
		colonIdx += objIdx + 1

		// Skip whitespace
		for colonIdx < len(scriptContent) && (scriptContent[colonIdx] == ' ' || scriptContent[colonIdx] == '\t') {
			colonIdx++
		}

		// Extract quoted value
		if colonIdx < len(scriptContent) {
			quoteChar := scriptContent[colonIdx]
			if quoteChar == '"' || quoteChar == '\'' {
				closeQuote := strings.Index(scriptContent[colonIdx+1:], string(quoteChar))
				if closeQuote != -1 {
					symbol := scriptContent[colonIdx+1 : colonIdx+1+closeQuote]
					symbol = strings.TrimSpace(symbol)
					if isCoinSymbol(symbol) && !symbolSet[symbol] {
						symbols = append(symbols, symbol)
						symbolSet[symbol] = true
					}
				}
			}
		}

		objStart = objIdx + 1
	}

	// Strategy 3: Simple word-based extraction (fallback)
	words := strings.Fields(scriptContent)
	for _, word := range words {
		// Remove quotes, brackets, and common punctuation
		cleaned := strings.Trim(word, `"'[]{}:,;`)
		if isCoinSymbol(cleaned) && !symbolSet[cleaned] {
			symbols = append(symbols, cleaned)
			symbolSet[cleaned] = true
		}
	}

	return symbols
}

// getCachedTaggedCoins returns cached tagged coins, or fetches and caches them if expired
func (c *BinanceClient) getCachedTaggedCoins(ctx context.Context) (monitoringCoins, seedCoins map[string]bool, err error) {
	globalTaggedCoinsCache.mu.RLock()
	needsUpdate := time.Since(globalTaggedCoinsCache.lastUpdate) > globalTaggedCoinsCache.cacheDuration
	globalTaggedCoinsCache.mu.RUnlock()

	if !needsUpdate {
		globalTaggedCoinsCache.mu.RLock()
		defer globalTaggedCoinsCache.mu.RUnlock()
		// Return copies of the maps
		monitoringCopy := make(map[string]bool, len(globalTaggedCoinsCache.monitoringCoins))
		seedCopy := make(map[string]bool, len(globalTaggedCoinsCache.seedCoins))
		for k, v := range globalTaggedCoinsCache.monitoringCoins {
			monitoringCopy[k] = v
		}
		for k, v := range globalTaggedCoinsCache.seedCoins {
			seedCopy[k] = v
		}
		return monitoringCopy, seedCopy, nil
	}

	// Cache expired, fetch fresh data
	monitoringCoins, seedCoins, err = getTaggedCoinsFromWeb(ctx, c.httpClient)
	if err != nil {
		// On error, return cached data if available (even if expired)
		globalTaggedCoinsCache.mu.RLock()
		defer globalTaggedCoinsCache.mu.RUnlock()
		if len(globalTaggedCoinsCache.monitoringCoins) > 0 || len(globalTaggedCoinsCache.seedCoins) > 0 {
			monitoringCopy := make(map[string]bool, len(globalTaggedCoinsCache.monitoringCoins))
			seedCopy := make(map[string]bool, len(globalTaggedCoinsCache.seedCoins))
			for k, v := range globalTaggedCoinsCache.monitoringCoins {
				monitoringCopy[k] = v
			}
			for k, v := range globalTaggedCoinsCache.seedCoins {
				seedCopy[k] = v
			}
			return monitoringCopy, seedCopy, nil
		}
		return nil, nil, err
	}

	// Only update cache if we got results (don't cache empty results)
	// This prevents caching empty maps which would cause filtering to fail
	if len(monitoringCoins) > 0 || len(seedCoins) > 0 {
		globalTaggedCoinsCache.mu.Lock()
		globalTaggedCoinsCache.monitoringCoins = monitoringCoins
		globalTaggedCoinsCache.seedCoins = seedCoins
		globalTaggedCoinsCache.lastUpdate = time.Now()
		globalTaggedCoinsCache.mu.Unlock()
	} else {
		// If scraping returned empty results, log a warning but don't cache
		// This allows API tags to be used as fallback
		logger := common.DefaultLogger()
		logger.Warnf("Scraping returned empty tagged coins list - using API tags as fallback")
	}

	return monitoringCoins, seedCoins, nil
}

// ClearTaggedCoinsCache clears the cached tagged coins, forcing a fresh scrape on next request
func (c *BinanceClient) ClearTaggedCoinsCache() {
	globalTaggedCoinsCache.mu.Lock()
	defer globalTaggedCoinsCache.mu.Unlock()
	globalTaggedCoinsCache.monitoringCoins = make(map[string]bool)
	globalTaggedCoinsCache.seedCoins = make(map[string]bool)
	globalTaggedCoinsCache.lastUpdate = time.Time{} // Zero time forces refresh
}

// GetTradingPairs returns all available trading pairs
func (c *BinanceClient) GetTradingPairs() ([]common.TradingPair, error) {
	return c.GetTradingPairsWithFilter(false)
}

// GetTradingPairsWithFilter returns trading pairs with optional filtering of Seed tokens.
// Always excludes tokens marked with "Monitoring" tag (tokens under observation, not suitable for trading).
// Always excludes tokens marked with "Seed" tag (high-risk tokens with potential total loss).
// Uses web scraping to get the most up-to-date list of tagged coins, as Binance APIs don't include tag data.
// filterSeedTokens: deprecated parameter, kept for backward compatibility. Seed tokens are always filtered.
func (c *BinanceClient) GetTradingPairsWithFilter(filterSeedTokens bool) ([]common.TradingPair, error) {
	endpoint := fmt.Sprintf("%s/exchangeInfo", c.apiPath("v3"))
	response, err := c.doGet(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange info: %w", err)
	}

	var exchangeInfo struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &exchangeInfo); err != nil {
		return nil, fmt.Errorf("failed to parse exchange info: %w", err)
	}

	// Get tagged coins from web scraping (primary source)
	ctx := context.Background()
	monitoringCoins, seedCoins, scrapeErr := c.getCachedTaggedCoins(ctx)

	logger := common.DefaultLogger()
	if scrapeErr != nil {
		logger.Warnf("Failed to scrape tagged coins from web - Monitoring and Seed coins may not be filtered: %v", scrapeErr)
	} else {
		logger.Debugf("Scraped tagged coins: Monitoring=%d, Seed=%d", len(monitoringCoins), len(seedCoins))
		// Log a few examples for debugging
		if len(monitoringCoins) > 0 {
			count := 0
			for coin := range monitoringCoins {
				if count < 5 {
					logger.Debugf("  Monitoring coin: %s", coin)
					count++
				}
			}
		}
	}

	var tradingPairs []common.TradingPair
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.Status != "TRADING" {
			continue
		}

		baseAsset := strings.ToUpper(symbol.BaseAsset)

		// Filter out Monitoring coins (always excluded)
		// Only use scraped data since API doesn't include tags
		if scrapeErr == nil && len(monitoringCoins) > 0 {
			if monitoringCoins[baseAsset] {
				continue
			}
		}

		// Filter out Seed coins (always excluded)
		// Only use scraped data since API doesn't include tags
		if scrapeErr == nil && len(seedCoins) > 0 {
			if seedCoins[baseAsset] {
				continue
			}
		}

		tradingPairs = append(tradingPairs, common.TradingPair{
			Symbol:     symbol.Symbol,
			BaseAsset:  symbol.BaseAsset,
			QuoteAsset: symbol.QuoteAsset,
		})
	}

	return tradingPairs, nil
}

// ScalpingConfig holds configuration for the scalping coin selection algorithm.
type ScalpingConfig struct {
	// Quote asset to filter pairs (default: "USDT")
	QuoteAsset string
	// Minimum 24h volume in USD (default: 20,000,000)
	MinVolume float64
	// Minimum trades per minute over the last 24h to ensure active markets (default: 10)
	MinTradesPerMin float64
	// Minimum 24h price change percent to avoid coins in steep downtrends (default: -3.0%)
	Min24hPriceChange float64
	// Maximum spread percentage (default: 0.08%)
	MaxSpread float64
	// Minimum profitability ratio (default: 3.0)
	MinProfitabilityRatio float64
	// Minimum ATR percentage on 5-min candles (default: 0.3%)
	MinATR5Min float64
	// Minimum recent trend (6h of 5m candles) to avoid coins with fresh downward momentum (default: -0.75%)
	MinRecentTrendPct float64
	// Minimum short-term trend over TrendLookbackMinutes (default: +0.10%)
	MinShortTermTrendPct float64
	// Minimum relative strength vs. benchmarks (BTC/ETH) over the same window (default: -0.25%)
	MinRelativeStrengthPct float64
	// Lookback window in minutes for short-term trend and relative strength (default: 60 minutes)
	TrendLookbackMinutes int
	// Minimum order book depth within 0.05% (default: $5,000)
	MinOrderBookDepth float64
	// Taker fee percentage (default: 0.1% for Binance)
	TakerFeePercent float64
	// Number of top coins to return (default: 5)
	TopN int
	// Rate limit delay between API calls (default: 100ms)
	RateLimitDelay time.Duration
	// VolatilityWeight controls how much emphasis is placed on volatility (ATR) in scoring.
	// Higher values favor more volatile coins. (default: 1.5 for high-volatility targeting)
	// Use 1.0 for balanced scoring, 2.0+ for aggressive volatility preference.
	VolatilityWeight float64
	// TrendBiasWeight controls how strongly short-term trend/relative strength boosts the score (default: 1.15)
	TrendBiasWeight float64
	// UptrendWeight boosts scores for coins with multi-timeframe upside (24h + 6h + 1h).
	// 0.0 disables the boost, 1.0 applies the default emphasis.
	UptrendWeight float64
	// StrongUptrendOnly tightens trend filters (24h, 6h, 1h, RS) to require clear upside.
	StrongUptrendOnly bool
}

// DefaultScalpingConfig returns the recommended configuration for scalping coin selection.
// By default, volatility weight is set to 1.5 to favor higher-volatility coins.
func DefaultScalpingConfig() ScalpingConfig {
	return ScalpingConfig{
		QuoteAsset:             "USDT",
		MinVolume:              20_000_000, // $20M USD minimum
		MinTradesPerMin:        10,         // At least 10 trades/min for fast exits
		Min24hPriceChange:      -3.0,       // Filter out coins down more than 3% in 24h
		MaxSpread:              0.08,       // 0.08% maximum spread
		MinProfitabilityRatio:  3.0,        // Profitability ratio > 3.0
		MinATR5Min:             0.3,        // ATR > 0.3% on 5-min candles
		MinRecentTrendPct:      -0.75,      // Avoid coins with steep intraday downtrends
		MinShortTermTrendPct:   0.10,       // Require mild positive 1h trend bias
		MinRelativeStrengthPct: -0.25,      // Allow slight underperformance vs BTC/ETH but avoid weak laggards
		TrendLookbackMinutes:   60,         // 1h short-term trend window
		MinOrderBookDepth:      5000,       // $5k fillable within 0.05%
		TakerFeePercent:        0.1,        // 0.1% Binance taker fee
		TopN:                   5,          // Top 5 coins for diversification
		RateLimitDelay:         100 * time.Millisecond,
		VolatilityWeight:       1.5,  // Favor high-volatility coins (1.0 = balanced, 2.0+ = aggressive)
		TrendBiasWeight:        1.15, // Reward short-term upside/relative strength in scoring
		UptrendWeight:          1.0,  // Moderate boost for aligned multi-timeframe uptrends
		StrongUptrendOnly:      false,
	}
}

// FindScalpingCoins analyzes all active trading pairs to find the most suitable coins for scalping.
// Uses an advanced multi-factor algorithm prioritizing spread over volume.
//
// Algorithm Priority:
//  1. Spread filter: < 0.08% (primary filter - spread is more important than volume)
//  2. Trade frequency filter: > 10 trades/min (ensures continuous flow to exit quickly)
//  3. Volume filter: > $20M USD (secondary filter for basic liquidity)
//  4. Downtrend filter: 24h price change above -3% (avoid steep daily losers)
//  5. ATR filter: > 0.3% on 5-minute candles (tradeable volatility)
//  6. Intraday trend filter: 6h net change above -0.75% (avoid coins still sliding intraday)
//  7. Short-term trend filter: 60m net change above +0.10% (favor current upside)
//  8. Relative strength filter: outperform BTC/ETH over 60m by at least -0.25% (avoid laggards)
//  9. Profitability ratio: Volatility / (Spread + 2×Fee) > 3.0
//  10. Order book depth: $5k fillable within 0.05% of mid-price
//
// Scoring Formula:
//
//	Score = (ATR_5min / Spread) × sqrt(Volume_24h / 10M) × DirectionalityFactor × ExitSpeed × TrendBias
//
// Where DirectionalityFactor = Σ|returns| / (sign_changes + 1) rewards trending movement.
// TrendBias itself is composed of short-term trend/relative strength and an optional multi-timeframe uptrend boost.
//
// Parameters:
//   - quoteAsset: The quote currency to use (default: "USDT")
//   - minVolume: Minimum 24h volume threshold in USD (default: 20,000,000)
//   - topN: Number of top coins to return (default: 5)
//   - rateLimitDelay: Delay between API calls to respect rate limits (default: 100ms)
//   - maxSpread: Maximum allowed bid-ask spread percentage (default: 0.08%)
//   - trade frequency: Controlled via config.MinTradesPerMin (default: 10 trades/min)
//
// Returns a sorted list of ScalpingCoin structs, ranked by the advanced scoring algorithm.
func (c *BinanceClient) FindScalpingCoins(quoteAsset string, minVolume float64, topN int, rateLimitDelay time.Duration, maxSpread float64) ([]ScalpingCoin, error) {
	// Build config from parameters with smart defaults
	config := DefaultScalpingConfig()

	if quoteAsset != "" {
		config.QuoteAsset = strings.ToUpper(quoteAsset)
	}
	if minVolume > 0 {
		config.MinVolume = minVolume
	}
	if topN > 0 {
		config.TopN = topN
	}
	if rateLimitDelay > 0 {
		config.RateLimitDelay = rateLimitDelay
	}
	if maxSpread > 0 {
		config.MaxSpread = maxSpread
	}

	return c.FindScalpingCoinsWithConfig(config)
}

// FindScalpingCoinsWithConfig performs scalping coin selection with full configuration control.
// This method allows fine-tuning of all algorithm parameters.
func (c *BinanceClient) FindScalpingCoinsWithConfig(config ScalpingConfig) ([]ScalpingCoin, error) {
	// Use a timeout context to prevent indefinite hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	logger := common.DefaultLogger()

	trendLookback := config.TrendLookbackMinutes
	if trendLookback <= 0 {
		trendLookback = 60
	}

	// Normalize tunables to safe ranges
	if config.UptrendWeight < 0 {
		config.UptrendWeight = 0
	}

	// Optional "uptrend only" mode tightens trend gates without changing caller thresholds.
	if config.StrongUptrendOnly {
		if config.Min24hPriceChange < 0.5 {
			config.Min24hPriceChange = 0.5 // Require green daily tape
		}
		if config.MinRecentTrendPct < 0.5 {
			config.MinRecentTrendPct = 0.5 // Require upside over the last ~6h
		}
		if config.MinShortTermTrendPct < 0.35 {
			config.MinShortTermTrendPct = 0.35 // Bias to actively rising 1h slope
		}
		if config.MinRelativeStrengthPct < 0 {
			config.MinRelativeStrengthPct = 0 // Must at least keep up with BTC/ETH
		}
	}

	// Get all trading pairs, filtering out Seed tokens (high-risk tokens)
	tradingPairs, err := c.GetTradingPairsWithFilter(true)
	if err != nil {
		return nil, fmt.Errorf("failed to get trading pairs: %w", err)
	}

	// Filter pairs by quote asset and build symbol map
	candidatePairs := make([]common.TradingPair, 0)
	symbolMap := make(map[string]common.TradingPair)
	for _, pair := range tradingPairs {
		if strings.EqualFold(pair.QuoteAsset, config.QuoteAsset) {
			candidatePairs = append(candidatePairs, pair)
			binanceSymbol := convertToBinanceSymbol(pair.Symbol)
			symbolMap[binanceSymbol] = pair
		}
	}

	if len(candidatePairs) == 0 {
		return nil, fmt.Errorf("no trading pairs found for quote asset %s", config.QuoteAsset)
	}

	logger.Debugf("FindScalpingCoins: Found %d candidate pairs for %s", len(candidatePairs), config.QuoteAsset)

	// PHASE 1: Batch fetch all 24h tickers in one API call
	allTickers, err := c.getAllTickers24hr(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch all tickers: %w", err)
	}

	// Phase 1 filtering: Spread (primary), Trade frequency (secondary), Volume (tertiary)
	type candidateData struct {
		pair         common.TradingPair
		volume       float64
		tradesPerMin float64
		priceChange  float64
		spread       float64
		lastPrice    float64
		bidPrice     float64
		askPrice     float64
	}

	candidates := make([]candidateData, 0)
	var spreadFiltered, volumeFiltered, tradeFiltered, priceChangeFiltered int

	for _, ticker := range allTickers {
		pair, exists := symbolMap[ticker.Symbol]
		if !exists {
			continue
		}

		// Parse ticker values
		volume := ticker.parseVolume()
		tradesPerMin := ticker.tradesPerMinute()
		lastPrice := ticker.parseLastPrice()
		bidPrice := ticker.parseBidPrice()
		askPrice := ticker.parseAskPrice()
		priceChange := ticker.parsePriceChangePercent()

		// Validate basic price data
		if lastPrice <= 0 || bidPrice <= 0 || askPrice <= 0 {
			continue
		}
		if askPrice < bidPrice {
			continue // Invalid spread
		}

		// Calculate spread
		midPrice := (bidPrice + askPrice) / 2
		spread := ((askPrice - bidPrice) / midPrice) * 100

		// PRIORITY 1: Filter by spread FIRST (most important for scalping)
		// Spread directly impacts profitability
		if spread > config.MaxSpread {
			spreadFiltered++
			continue
		}

		// PRIORITY 2: Filter by trade frequency (fast fills)
		if tradesPerMin < config.MinTradesPerMin {
			tradeFiltered++
			continue
		}

		// PRIORITY 3: Filter by volume (ensures basic liquidity)
		if volume < config.MinVolume {
			volumeFiltered++
			continue
		}

		// DOWN-TREND FILTER: skip steeply negative 24h movers
		if priceChange < config.Min24hPriceChange {
			priceChangeFiltered++
			continue
		}

		candidates = append(candidates, candidateData{
			pair:         pair,
			volume:       volume,
			tradesPerMin: tradesPerMin,
			priceChange:  priceChange,
			spread:       spread,
			lastPrice:    lastPrice,
			bidPrice:     bidPrice,
			askPrice:     askPrice,
		})
	}

	logger.Debugf("Phase 1: spreadFiltered=%d, tradeFiltered=%d, volumeFiltered=%d, priceChangeFiltered=%d, remaining=%d",
		spreadFiltered, tradeFiltered, volumeFiltered, priceChangeFiltered, len(candidates))

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidates passed initial filters: spreadFiltered=%d, tradeFiltered=%d, volumeFiltered=%d, priceChangeFiltered=%d",
			spreadFiltered, tradeFiltered, volumeFiltered, priceChangeFiltered)
	}

	// Sort by spread (tightest spreads first) for priority in detailed analysis
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].spread < candidates[j].spread
	})

	// Limit candidates for detailed analysis
	maxDetailedCandidates := config.TopN * 5
	if len(candidates) > maxDetailedCandidates {
		candidates = candidates[:maxDetailedCandidates]
	}

	// Prepare trend baselines for short-term momentum and relative strength (BTC/ETH against the quote asset)
	since5min := time.Now().Add(-6 * time.Hour) // 6 hours of 5-min candles
	benchmarkTrends := c.loadBenchmarkTrends(ctx, config.QuoteAsset, since5min, trendLookback)
	bestBenchmarkTrend, hasBenchmarkTrend := maxBenchmarkTrend(benchmarkTrends)

	// PHASE 2: Detailed analysis with candles and order books
	type detailedResult struct {
		coin ScalpingCoin
		err  error
	}

	resultChan := make(chan detailedResult, len(candidates))
	semaphore := make(chan struct{}, 10)

	var wg sync.WaitGroup

	for i, cand := range candidates {
		wg.Add(1)
		go func(cand candidateData, index int) {
			defer wg.Done()

			// Rate limiting
			if index > 0 {
				delay := config.RateLimitDelay * time.Duration(index%10)
				select {
				case <-ctx.Done():
					resultChan <- detailedResult{err: ctx.Err()}
					return
				case <-time.After(delay):
				}
			}

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			symbol := cand.pair.Symbol

			// Fetch 5-minute candles for ATR and directionality calculation
			candles5min, err := c.GetCandles(symbol, "5m", since5min, 72) // 6 hours of 5-min candles
			if err != nil || len(candles5min) < 10 {
				resultChan <- detailedResult{err: fmt.Errorf("insufficient candle data")}
				return
			}

			// Calculate ATR on 5-minute candles
			atr5min := calculateATR5Min(candles5min)
			if atr5min < config.MinATR5Min {
				resultChan <- detailedResult{err: fmt.Errorf("ATR too low: %.4f%% < %.4f%%", atr5min, config.MinATR5Min)}
				return
			}

			recentTrendPct := calculateRecentTrendPercent(candles5min)
			if recentTrendPct < config.MinRecentTrendPct {
				resultChan <- detailedResult{err: fmt.Errorf("recent trend too negative: %.2f%% < %.2f%%", recentTrendPct, config.MinRecentTrendPct)}
				return
			}

			shortTermTrendPct := calculateTrendPercentWindow(candles5min, trendLookback)
			if shortTermTrendPct < config.MinShortTermTrendPct {
				resultChan <- detailedResult{err: fmt.Errorf("short-term trend too weak: %.2f%% < %.2f%%", shortTermTrendPct, config.MinShortTermTrendPct)}
				return
			}

			relativeStrengthPct := 0.0
			if hasBenchmarkTrend {
				relativeStrengthPct = shortTermTrendPct - bestBenchmarkTrend
				if relativeStrengthPct < config.MinRelativeStrengthPct {
					resultChan <- detailedResult{err: fmt.Errorf("relative strength too weak: %.2f%% < %.2f%%", relativeStrengthPct, config.MinRelativeStrengthPct)}
					return
				}
			}

			// Calculate directionality factor
			directionalityFactor := calculateDirectionalityFactor(candles5min)

			// Calculate profitability ratio
			profitabilityRatio := calculateProfitabilityRatio(atr5min, cand.spread, config.TakerFeePercent)
			if profitabilityRatio < config.MinProfitabilityRatio {
				resultChan <- detailedResult{err: fmt.Errorf("profitability ratio too low: %.2f < %.2f",
					profitabilityRatio, config.MinProfitabilityRatio)}
				return
			}

			// Fetch order book for depth check
			orderBook, err := c.GetOrderBook(symbol, 100)
			var orderBookDepth float64
			if err == nil && orderBook != nil {
				midPrice := (cand.bidPrice + cand.askPrice) / 2
				// Check depth within 0.05% of mid-price
				orderBookDepth = calculateOrderBookDepthWithinSpread(orderBook, midPrice, 0.05)
			}

			// Filter by order book depth
			if orderBookDepth < config.MinOrderBookDepth {
				resultChan <- detailedResult{err: fmt.Errorf("order book depth too low: $%.2f < $%.2f",
					orderBookDepth, config.MinOrderBookDepth)}
				return
			}

			exitSpeed := calculateExitSpeedFactor(orderBookDepth, cand.tradesPerMin, config.MinOrderBookDepth)

			trendBias := calculateTrendBias(shortTermTrendPct, relativeStrengthPct, config.TrendBiasWeight)
			uptrendFactor := calculateUptrendFactor(recentTrendPct, shortTermTrendPct, cand.priceChange, config.UptrendWeight)
			totalTrendBias := trendBias * uptrendFactor

			// Calculate advanced score using the new formula with volatility weighting and trend bias
			score := calculateAdvancedScalpingScore(atr5min, cand.spread, cand.volume, directionalityFactor, config.VolatilityWeight, exitSpeed, totalTrendBias)

			// Also calculate legacy volatility for backward compatibility
			volatility := calculateBinanceVolatility(candles5min)

			coin := ScalpingCoin{
				Code:                 cand.pair.BaseAsset,
				Name:                 cand.pair.BaseAsset,
				Symbol:               symbol,
				Volume:               cand.volume,
				Volatility:           volatility,
				Spread:               cand.spread,
				Score:                score,
				ATR5Min:              atr5min,
				ProfitabilityRatio:   profitabilityRatio,
				DirectionalityFactor: directionalityFactor,
				OrderBookDepth:       orderBookDepth,
				TradesPerMinute:      cand.tradesPerMin,
				ExitLiquidityScore:   exitSpeed,
				DailyChangePercent:   cand.priceChange,
				RecentTrendPct:       recentTrendPct,
				ShortTermTrendPct:    shortTermTrendPct,
				RelativeStrengthPct:  relativeStrengthPct,
				TrendBias:            totalTrendBias,
				UptrendFactor:        uptrendFactor,
			}

			resultChan <- detailedResult{coin: coin}
		}(cand, i)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	rankedCoins := make([]ScalpingCoin, 0)
	var atrFiltered, profitabilityFiltered, depthFiltered, trendFiltered, shortTrendFiltered, relativeStrengthFiltered, otherFiltered int

	for result := range resultChan {
		if result.err != nil {
			errStr := result.err.Error()
			if strings.Contains(errStr, "ATR too low") {
				atrFiltered++
			} else if strings.Contains(errStr, "recent trend") {
				trendFiltered++
			} else if strings.Contains(errStr, "short-term trend") {
				shortTrendFiltered++
			} else if strings.Contains(errStr, "relative strength") {
				relativeStrengthFiltered++
			} else if strings.Contains(errStr, "profitability ratio") {
				profitabilityFiltered++
			} else if strings.Contains(errStr, "order book depth") {
				depthFiltered++
			} else {
				otherFiltered++
			}
			continue
		}
		rankedCoins = append(rankedCoins, result.coin)
	}

	// Sort by score (highest first)
	sort.Slice(rankedCoins, func(i, j int) bool {
		return rankedCoins[i].Score > rankedCoins[j].Score
	})

	// Debug logging
	logger.Debugf("FindScalpingCoins: candidates=%d, spreadFiltered=%d, tradeFiltered=%d, volumeFiltered=%d, priceChangeFiltered=%d, atrFiltered=%d, trendFiltered=%d, shortTrendFiltered=%d, relativeStrengthFiltered=%d, profitabilityFiltered=%d, depthFiltered=%d, otherFiltered=%d, ranked=%d",
		len(candidatePairs), spreadFiltered, tradeFiltered, volumeFiltered, priceChangeFiltered, atrFiltered, trendFiltered, shortTrendFiltered, relativeStrengthFiltered, profitabilityFiltered, depthFiltered, otherFiltered, len(rankedCoins))

	if len(rankedCoins) == 0 {
		return nil, fmt.Errorf("no coins passed all filters (spread: %d, trades: %d, volume: %d, priceChange: %d, ATR: %d, trend: %d, shortTrend: %d, relativeStrength: %d, profitability: %d, depth: %d, other: %d)",
			spreadFiltered, tradeFiltered, volumeFiltered, priceChangeFiltered, atrFiltered, trendFiltered, shortTrendFiltered, relativeStrengthFiltered, profitabilityFiltered, depthFiltered, otherFiltered)
	}

	// Return top N
	if len(rankedCoins) > config.TopN {
		rankedCoins = rankedCoins[:config.TopN]
	}

	return rankedCoins, nil
}

// loadBenchmarkTrends fetches short-term trend data for benchmark assets (BTC/ETH) against the provided quote.
// Used to compute relative strength for scalping candidates without per-symbol overhead.
func (c *BinanceClient) loadBenchmarkTrends(ctx context.Context, quoteAsset string, since time.Time, lookbackMinutes int) map[string]float64 {
	benchmarks := []string{"BTC", "ETH"}
	trends := make(map[string]float64, len(benchmarks))

	if lookbackMinutes <= 0 {
		lookbackMinutes = 60
	}

	quoteAsset = strings.ToUpper(quoteAsset)

	for _, base := range benchmarks {
		select {
		case <-ctx.Done():
			return trends
		default:
		}

		symbol := fmt.Sprintf("%s/%s", base, quoteAsset)
		candles, err := c.GetCandles(symbol, "5m", since, 72)
		if err != nil || len(candles) < 5 {
			continue
		}

		trends[base] = calculateTrendPercentWindow(candles, lookbackMinutes)
	}

	return trends
}

func maxBenchmarkTrend(trends map[string]float64) (float64, bool) {
	var maxTrend float64
	var hasValue bool

	for _, v := range trends {
		if !hasValue || v > maxTrend {
			maxTrend = v
			hasValue = true
		}
	}

	return maxTrend, hasValue
}

// getAllTickers24hr fetches all 24h ticker statistics in one batch API call.
// This is much more efficient than fetching individual tickers.
func (c *BinanceClient) getAllTickers24hr(ctx context.Context) ([]binanceTicker24hr, error) {
	endpoint := fmt.Sprintf("%s/ticker/24hr", c.apiPath("v3"))
	// No symbol parameter = get all tickers

	response, err := c.doRequest(ctx, http.MethodGet, endpoint, nil, c.getHeaders())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch all tickers: %w", err)
	}

	var tickers []binanceTicker24hr
	if err := json.Unmarshal(response, &tickers); err != nil {
		return nil, fmt.Errorf("failed to parse tickers: %w", err)
	}

	return tickers, nil
}

// binanceTicker24hr represents the 24h ticker statistics from Binance API
type binanceTicker24hr struct {
	Symbol             string `json:"symbol"`
	LastPrice          string `json:"lastPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"` // Volume in quote currency
	BidPrice           string `json:"bidPrice"`
	AskPrice           string `json:"askPrice"`
	PriceChangePercent string `json:"priceChangePercent"`
	Count              int64  `json:"count"` // Number of trades in the last 24h
	CloseTime          int64  `json:"closeTime"`
}

// parseVolume parses volume, preferring quoteVolume if available
func (t *binanceTicker24hr) parseVolume() float64 {
	quoteVol, _ := strconv.ParseFloat(t.QuoteVolume, 64)
	if quoteVol > 0 {
		return quoteVol
	}
	vol, _ := strconv.ParseFloat(t.Volume, 64)
	return vol
}

// parseLastPrice parses last price
func (t *binanceTicker24hr) parseLastPrice() float64 {
	price, _ := strconv.ParseFloat(t.LastPrice, 64)
	return price
}

// parseBidPrice parses bid price
func (t *binanceTicker24hr) parseBidPrice() float64 {
	price, _ := strconv.ParseFloat(t.BidPrice, 64)
	return price
}

// parseAskPrice parses ask price
func (t *binanceTicker24hr) parseAskPrice() float64 {
	price, _ := strconv.ParseFloat(t.AskPrice, 64)
	return price
}

// parsePriceChangePercent parses price change percent
func (t *binanceTicker24hr) parsePriceChangePercent() float64 {
	percent, _ := strconv.ParseFloat(t.PriceChangePercent, 64)
	return percent
}

// tradesPerMinute returns the average trades per minute over the last 24h.
func (t *binanceTicker24hr) tradesPerMinute() float64 {
	if t.Count <= 0 {
		return 0
	}
	return float64(t.Count) / 1440.0
}

// calculateBinanceVolatility calculates the daily volatility as the standard deviation of log returns.
// Returns volatility as a percentage.
func calculateBinanceVolatility(candles []models.Candle) float64 {
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

// calculateOrderBookLiquidity calculates a weighted liquidity score based on order book depth.
// Orders closer to the mid price are weighted more heavily, as they're more relevant for scalping.
// Returns a score typically between 0.1 and 10.0, where higher is better.
func calculateOrderBookLiquidity(orderBook *models.OrderBook, midPrice float64) float64 {
	if orderBook == nil || midPrice <= 0 {
		return 1.0
	}

	var weightedBidDepth, weightedAskDepth float64

	// Calculate weighted bid depth (orders closer to mid price weighted more)
	for i, bid := range orderBook.Bids {
		if bid.Price <= 0 || bid.Amount <= 0 {
			continue
		}
		// Weight decreases with distance from mid price
		// Use exponential decay: weight = exp(-distance_pct * decay_factor)
		distancePct := (midPrice - bid.Price) / midPrice
		if distancePct < 0 {
			distancePct = 0
		}
		// Decay factor: orders within 0.5% get full weight, beyond that decay quickly
		weight := math.Exp(-distancePct * 200) // 200 = decay factor
		// Also weight by position (first few levels more important)
		positionWeight := math.Exp(-float64(i) * 0.3)
		weightedBidDepth += bid.Amount * bid.Price * weight * positionWeight
	}

	// Calculate weighted ask depth
	for i, ask := range orderBook.Asks {
		if ask.Price <= 0 || ask.Amount <= 0 {
			continue
		}
		distancePct := (ask.Price - midPrice) / midPrice
		if distancePct < 0 {
			distancePct = 0
		}
		weight := math.Exp(-distancePct * 200)
		positionWeight := math.Exp(-float64(i) * 0.3)
		weightedAskDepth += ask.Amount * ask.Price * weight * positionWeight
	}

	// Normalize by mid price to get a liquidity score
	// Higher liquidity = better for scalping
	totalWeightedDepth := (weightedBidDepth + weightedAskDepth) / 2.0
	if totalWeightedDepth <= 0 {
		return 1.0
	}

	// Normalize to a reasonable range (0.1 to 10.0)
	// Typical values: 1000-1000000 in quote currency
	liquidityScore := math.Log10(totalWeightedDepth+1) / 2.0
	if liquidityScore < 0.1 {
		liquidityScore = 0.1
	}
	if liquidityScore > 10.0 {
		liquidityScore = 10.0
	}

	return liquidityScore
}

// calculateOrderBookImbalance calculates the bid/ask imbalance ratio.
// Returns a value between 0.5 and 2.0, where:
// - 1.0 = perfectly balanced
// - > 1.0 = more bid liquidity (bullish pressure)
// - < 1.0 = more ask liquidity (bearish pressure)
// For scalping, we slightly prefer balanced books (closer to 1.0).
func calculateOrderBookImbalance(orderBook *models.OrderBook) float64 {
	if orderBook == nil {
		return 1.0
	}

	var totalBidValue, totalAskValue float64

	// Sum bid liquidity (first 10 levels)
	for i, bid := range orderBook.Bids {
		if i >= 10 {
			break
		}
		if bid.Price > 0 && bid.Amount > 0 {
			totalBidValue += bid.Price * bid.Amount
		}
	}

	// Sum ask liquidity (first 10 levels)
	for i, ask := range orderBook.Asks {
		if i >= 10 {
			break
		}
		if ask.Price > 0 && ask.Amount > 0 {
			totalAskValue += ask.Price * ask.Amount
		}
	}

	if totalAskValue <= 0 {
		return 1.0
	}

	imbalance := totalBidValue / totalAskValue

	// Clamp to reasonable range
	if imbalance < 0.5 {
		imbalance = 0.5
	}
	if imbalance > 2.0 {
		imbalance = 2.0
	}

	return imbalance
}

// calculatePriceImpactFactor estimates the price impact of a typical scalping trade.
// Lower values = better (less slippage). Returns a factor typically between 0.5 and 5.0.
// Uses a fixed trade size of $1000 USDT (or equivalent in quote currency) which represents
// a typical scalping trade size, independent of coin price.
func calculatePriceImpactFactor(orderBook *models.OrderBook, midPrice float64) float64 {
	if orderBook == nil || midPrice <= 0 {
		return 1.0
	}

	// Use a fixed trade size of $1000 in quote currency for scalping
	// This is independent of coin price and represents a typical scalping trade size
	// For scalping, coin cost does not matter - we use a fixed position size
	typicalTradeSize := 1000.0 // Fixed $1000 trade size in quote currency

	var bidImpact, askImpact float64

	// Calculate bid side impact (buying)
	remaining := typicalTradeSize
	for i, bid := range orderBook.Bids {
		if i >= 10 || remaining <= 0 {
			break
		}
		if bid.Price <= 0 || bid.Amount <= 0 {
			continue
		}
		available := bid.Price * bid.Amount
		if available >= remaining {
			// Can fill entirely at this level
			bidImpact = float64(i+1) * 0.1 // Each level adds 0.1% impact
			break
		}
		remaining -= available
		bidImpact = float64(i+1) * 0.1
	}

	// Calculate ask side impact (selling)
	remaining = typicalTradeSize
	for i, ask := range orderBook.Asks {
		if i >= 10 || remaining <= 0 {
			break
		}
		if ask.Price <= 0 || ask.Amount <= 0 {
			continue
		}
		available := ask.Price * ask.Amount
		if available >= remaining {
			askImpact = float64(i+1) * 0.1
			break
		}
		remaining -= available
		askImpact = float64(i+1) * 0.1
	}

	// Average impact and normalize
	avgImpact := (bidImpact + askImpact) / 2.0
	if avgImpact <= 0 {
		return 1.0
	}

	// Convert to factor (lower is better, so invert)
	// Impact of 0.1% = factor of 1.0, impact of 0.5% = factor of 2.0
	impactFactor := 1.0 + avgImpact*10.0

	// Clamp to reasonable range
	if impactFactor < 0.5 {
		impactFactor = 0.5
	}
	if impactFactor > 5.0 {
		impactFactor = 5.0
	}

	return impactFactor
}

// calculateScalpingScore computes an enhanced scoring algorithm for scalping suitability.
// Uses multiple factors with appropriate weights and normalization.
func calculateScalpingScore(volume, volatility, liquidityScore, orderBookImbalance, spread, priceImpactFactor float64) float64 {
	// Normalize volume (log scale to handle wide range)
	// Typical range: 100K to 100M, normalize to 0.1-10.0
	normalizedVolume := math.Log10(volume+1) / 7.0 // Divide by 7 to normalize
	if normalizedVolume < 0.1 {
		normalizedVolume = 0.1
	}
	if normalizedVolume > 10.0 {
		normalizedVolume = 10.0
	}

	// Normalize volatility (already a percentage, typical range 0.5-10%)
	// Normalize to 0.1-10.0
	normalizedVolatility := volatility / 2.0
	if normalizedVolatility < 0.1 {
		normalizedVolatility = 0.1
	}
	if normalizedVolatility > 10.0 {
		normalizedVolatility = 10.0
	}

	// Normalize order book imbalance (prefer values closer to 1.0)
	// Convert to penalty: |imbalance - 1.0| = penalty
	imbalancePenalty := 1.0 + math.Abs(orderBookImbalance-1.0)*0.5

	// Spread penalty (wider spreads = worse)
	spreadPenalty := 1.0 + spread*2.0 // Multiply by 2 for stronger penalty

	// Enhanced scoring formula with weighted factors:
	// score = (volume^0.7 * volatility^0.8 * liquidity^0.6) / (spread^1.2 * impact^0.5 * imbalance^0.3)
	//
	// Exponents chosen to balance factors:
	// - Volume: 0.7 (important but not dominant)
	// - Volatility: 0.8 (very important for scalping)
	// - Liquidity: 0.6 (important for execution quality)
	// - Spread: 1.2 (strong penalty, critical for profitability)
	// - Price impact: 0.5 (moderate penalty)
	// - Imbalance: 0.3 (minor penalty, prefer balanced)

	numerator := math.Pow(normalizedVolume, 0.7) *
		math.Pow(normalizedVolatility, 0.8) *
		math.Pow(liquidityScore, 0.6)

	denominator := math.Pow(spreadPenalty, 1.2) *
		math.Pow(priceImpactFactor, 0.5) *
		math.Pow(imbalancePenalty, 0.3)

	if denominator <= 0 {
		return 0
	}

	score := numerator / denominator

	// Scale to a reasonable range (0.1 to 1000)
	// This ensures scores are comparable across different market conditions
	return score
}

// calculateATR5Min calculates the Average True Range on 5-minute candles as a percentage.
// ATR measures volatility by considering the full price range within each period.
// Returns ATR as a percentage of the current price.
func calculateATR5Min(candles []models.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	var trueRanges []float64

	for i := 1; i < len(candles); i++ {
		high := candles[i].High
		low := candles[i].Low
		prevClose := candles[i-1].Close

		if high <= 0 || low <= 0 || prevClose <= 0 {
			continue
		}

		// True Range = max(High - Low, |High - PrevClose|, |Low - PrevClose|)
		tr1 := high - low
		tr2 := math.Abs(high - prevClose)
		tr3 := math.Abs(low - prevClose)

		tr := tr1
		if tr2 > tr {
			tr = tr2
		}
		if tr3 > tr {
			tr = tr3
		}

		trueRanges = append(trueRanges, tr)
	}

	if len(trueRanges) == 0 {
		return 0
	}

	// Calculate average true range
	var sum float64
	for _, tr := range trueRanges {
		sum += tr
	}
	atr := sum / float64(len(trueRanges))

	// Convert to percentage based on the latest close price
	lastClose := candles[len(candles)-1].Close
	if lastClose <= 0 {
		return 0
	}

	atrPercent := (atr / lastClose) * 100
	return atrPercent
}

// calculateDirectionalityFactor measures how "trendy" vs "choppy" the price action is.
// Formula: Σ|returns| / (count of sign changes + 1)
// Higher values indicate more sustained directional movement (better for scalping).
func calculateDirectionalityFactor(candles []models.Candle) float64 {
	if len(candles) < 3 {
		return 1.0 // Neutral factor
	}

	// Calculate returns
	var returns []float64
	var sumAbsReturns float64

	for i := 1; i < len(candles); i++ {
		if candles[i-1].Close <= 0 || candles[i].Close <= 0 {
			continue
		}
		ret := (candles[i].Close - candles[i-1].Close) / candles[i-1].Close
		returns = append(returns, ret)
		sumAbsReturns += math.Abs(ret)
	}

	if len(returns) < 2 {
		return 1.0
	}

	// Count sign changes (direction reversals)
	var signChanges int
	for i := 1; i < len(returns); i++ {
		// Sign change if returns have opposite signs
		if (returns[i] > 0 && returns[i-1] < 0) || (returns[i] < 0 && returns[i-1] > 0) {
			signChanges++
		}
	}

	// Directionality = sum of absolute returns / (sign changes + 1)
	// +1 to avoid division by zero and to scale appropriately
	directionality := sumAbsReturns / float64(signChanges+1)

	// Scale to a reasonable range (typically 0.001 to 0.1)
	// Multiply by 100 to get a more usable range
	return directionality * 100
}

// calculateRecentTrendPercent returns the net percentage change over the provided candles.
// Used to avoid coins that are persistently trending down intraday.
func calculateRecentTrendPercent(candles []models.Candle) float64 {
	if len(candles) < 2 {
		return 0
	}

	first := candles[0].Close
	last := candles[len(candles)-1].Close

	if first <= 0 || last <= 0 {
		return 0
	}

	return ((last - first) / first) * 100
}

// calculateTrendPercentWindow returns the net percentage change over the trailing windowMinutes of candles.
// Uses 5m candles, so the window is rounded up to the nearest candle count.
func calculateTrendPercentWindow(candles []models.Candle, windowMinutes int) float64 {
	if len(candles) < 2 || windowMinutes <= 0 {
		return 0
	}

	windowCandles := windowMinutes / 5
	if windowMinutes%5 != 0 {
		windowCandles++
	}
	if windowCandles < 2 {
		windowCandles = 2
	}
	if windowCandles > len(candles) {
		windowCandles = len(candles)
	}

	startIdx := len(candles) - windowCandles
	first := candles[startIdx].Close
	last := candles[len(candles)-1].Close

	if first <= 0 || last <= 0 {
		return 0
	}

	return ((last - first) / first) * 100
}

// calculateTrendBias creates a scoring multiplier that rewards positive short-term momentum
// and relative strength while clamping extremes to keep scores stable.
func calculateTrendBias(shortTermTrendPct, relativeStrengthPct, weight float64) float64 {
	trendFactor := 1.0 + (shortTermTrendPct / 5.0) // 1% move ~20% boost
	if trendFactor < 0.5 {
		trendFactor = 0.5
	}
	if trendFactor > 1.8 {
		trendFactor = 1.8
	}

	rsFactor := 1.0 + (relativeStrengthPct / 4.0) // 1% RS outperformance ~25% boost
	if rsFactor < 0.6 {
		rsFactor = 0.6
	}
	if rsFactor > 1.6 {
		rsFactor = 1.6
	}

	if weight <= 0 {
		weight = 1.0
	}

	trendBias := math.Pow(trendFactor*rsFactor, weight)
	if trendBias < 0.5 {
		trendBias = 0.5
	}
	if trendBias > 3.0 {
		trendBias = 3.0
	}

	return trendBias
}

// calculateUptrendFactor boosts scoring for multi-timeframe upside alignment.
// It blends 24h change (20%), 6h intraday drift (30%), and short-term 1h momentum (50%).
// weight controls strength; 0 disables the boost.
func calculateUptrendFactor(recentTrendPct, shortTermTrendPct, dailyChangePct, weight float64) float64 {
	if weight <= 0 {
		return 1.0
	}

	// Composite favors current momentum while still respecting broader context.
	composite := (shortTermTrendPct * 0.5) + (recentTrendPct * 0.3) + (dailyChangePct * 0.2)

	// Each ~8% composite move adds roughly 1× weight to the multiplier.
	factor := 1.0 + (composite/8.0)*weight

	if factor < 0.5 {
		factor = 0.5
	}
	if factor > 3.0 {
		factor = 3.0
	}

	return factor
}

// calculateProfitabilityRatio calculates the profitability ratio for scalping.
// Formula: Volatility / (Spread + 2 × TakerFee)
// Minimum threshold should be > 3.0 for profitable scalping.
func calculateProfitabilityRatio(volatility, spread, takerFeePercent float64) float64 {
	if spread <= 0 {
		spread = 0.001 // Avoid division by zero
	}

	totalCost := spread + (2.0 * takerFeePercent)
	if totalCost <= 0 {
		return 0
	}

	return volatility / totalCost
}

// calculateOrderBookDepthWithinSpread calculates the total fillable USD value
// within a specified percentage of the mid-price on each side of the order book.
// Returns the minimum of bid-side and ask-side depth (to ensure both directions are tradeable).
func calculateOrderBookDepthWithinSpread(orderBook *models.OrderBook, midPrice float64, spreadPercent float64) float64 {
	if orderBook == nil || midPrice <= 0 || spreadPercent <= 0 {
		return 0
	}

	// Calculate price thresholds
	maxBidPrice := midPrice * (1 - spreadPercent/100)
	minAskPrice := midPrice * (1 + spreadPercent/100)

	var bidDepth, askDepth float64

	// Sum bid-side depth within threshold
	for _, bid := range orderBook.Bids {
		if bid.Price >= maxBidPrice && bid.Price > 0 && bid.Amount > 0 {
			bidDepth += bid.Price * bid.Amount
		}
	}

	// Sum ask-side depth within threshold
	for _, ask := range orderBook.Asks {
		if ask.Price <= minAskPrice && ask.Price > 0 && ask.Amount > 0 {
			askDepth += ask.Price * ask.Amount
		}
	}

	// Return minimum of both sides (both must have sufficient depth)
	if bidDepth < askDepth {
		return bidDepth
	}
	return askDepth
}

// calculateExitSpeedFactor favors markets that can be exited quickly after entry.
// Combines near-spread depth with trade frequency so we prefer books that are both
// deep at the top and actively trading.
func calculateExitSpeedFactor(orderBookDepth, tradesPerMinute, depthBaseline float64) float64 {
	if depthBaseline <= 0 {
		depthBaseline = 1000
	}

	// Depth factor: sqrt to soften extremes, normalized to the configured minimum depth
	depthFactor := math.Sqrt(orderBookDepth / depthBaseline)
	if depthFactor < 0.6 {
		depthFactor = 0.6
	}
	if depthFactor > 4.0 {
		depthFactor = 4.0
	}

	// Trade frequency factor: log scaling to reward steady flow without letting
	// a few hyperactive pairs dominate. 120 trades/min ~ 2 trades/sec as an upper anchor.
	tradeFactor := math.Log1p(tradesPerMinute) / math.Log1p(120)
	if tradeFactor < 0.4 {
		tradeFactor = 0.4
	}
	if tradeFactor > 1.6 {
		tradeFactor = 1.6
	}

	exitSpeed := depthFactor * tradeFactor
	if exitSpeed < 0.25 {
		exitSpeed = 0.25
	}
	if exitSpeed > 6.0 {
		exitSpeed = 6.0
	}

	return exitSpeed
}

// calculateAdvancedScalpingScore computes the advanced scoring algorithm for scalping suitability.
// Formula: Score = (ATR_5min^volatilityWeight / Spread) × sqrt(Volume_24h / 10M) × DirectionalityFactor × ExitSpeedFactor × TrendBias
// This scoring method prioritizes:
//   - ATR/Spread ratio: How much price moves relative to cost of trading
//   - Volatility weight: Raises ATR to a power to favor higher-volatility coins
//   - Volume factor: Ensures sufficient liquidity (sqrt to reduce impact of very high volume)
//   - Directionality: Rewards trending movement over choppy action
//   - ExitSpeedFactor: Rewards books with active flow + depth for faster exits
//   - TrendBias: Rewards pairs with short-term upside and relative strength
func calculateAdvancedScalpingScore(atr5min, spread, volume24h, directionalityFactor, volatilityWeight, exitSpeedFactor, trendBias float64) float64 {
	if spread <= 0 {
		spread = 0.001 // Avoid division by zero
	}

	// Apply volatility weight: raise ATR to the power of volatilityWeight
	// volatilityWeight > 1.0 emphasizes high-volatility coins more strongly
	// volatilityWeight = 1.0 is the original balanced scoring
	// volatilityWeight = 1.5 gives 50% more emphasis to volatility
	// volatilityWeight = 2.0 squares the ATR effect, strongly favoring volatile coins
	if volatilityWeight <= 0 {
		volatilityWeight = 1.0
	}
	weightedATR := math.Pow(atr5min, volatilityWeight)

	// Weighted ATR/Spread ratio - core profitability metric with volatility emphasis
	atrSpreadRatio := weightedATR / spread

	// Volume factor: sqrt(Volume / 10M)
	// This normalizes volume impact and ensures basic liquidity
	volumeFactor := math.Sqrt(volume24h / 10_000_000)
	if volumeFactor < 0.1 {
		volumeFactor = 0.1 // Minimum volume factor
	}
	if volumeFactor > 10.0 {
		volumeFactor = 10.0 // Cap volume factor to prevent domination
	}

	// Directionality factor (already scaled appropriately)
	if directionalityFactor <= 0 {
		directionalityFactor = 1.0
	}

	if exitSpeedFactor <= 0 {
		exitSpeedFactor = 1.0
	}

	if trendBias <= 0 {
		trendBias = 1.0
	}

	score := atrSpreadRatio * volumeFactor * directionalityFactor * exitSpeedFactor * trendBias
	return score
}

// FetchFundingRate fetches the current funding rate for a futures symbol
func (c *BinanceClient) FetchFundingRate(symbol string) (FundingRateInfo, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/fapi/v1/premiumIndex", c.futuresBaseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return FundingRateInfo{}, fmt.Errorf("failed to fetch funding rate: %w", err)
	}

	var fundingRateResp struct {
		Symbol          string `json:"symbol"`
		MarkPrice       string `json:"markPrice"`
		IndexPrice      string `json:"indexPrice"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
		Time            int64  `json:"time"`
	}

	if err := json.Unmarshal(response, &fundingRateResp); err != nil {
		return FundingRateInfo{}, fmt.Errorf("failed to parse funding rate: %w", err)
	}

	fundingRate, _ := strconv.ParseFloat(fundingRateResp.LastFundingRate, 64)
	markPrice, _ := strconv.ParseFloat(fundingRateResp.MarkPrice, 64)
	indexPrice, _ := strconv.ParseFloat(fundingRateResp.IndexPrice, 64)

	return FundingRateInfo{
		Symbol:          symbol,
		FundingRate:     fundingRate,
		NextFundingTime: time.Unix(fundingRateResp.NextFundingTime/1000, 0),
		LastFundingTime: time.Unix(fundingRateResp.Time/1000, 0),
		MarkPrice:       markPrice,
		IndexPrice:      indexPrice,
	}, nil
}

// FetchFundingHistory fetches historical funding rates
func (c *BinanceClient) FetchFundingHistory(symbol string, startTime, endTime time.Time, limit int) ([]FundingRateInfo, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/fapi/v1/fundingRate", c.futuresBaseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	if !startTime.IsZero() {
		params.Add("startTime", strconv.FormatInt(startTime.UnixNano()/int64(time.Millisecond), 10))
	}
	if !endTime.IsZero() {
		params.Add("endTime", strconv.FormatInt(endTime.UnixNano()/int64(time.Millisecond), 10))
	}
	if limit > 0 {
		params.Add("limit", strconv.Itoa(limit))
	}

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch funding history: %w", err)
	}

	var historyResp []struct {
		Symbol      string `json:"symbol"`
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}

	if err := json.Unmarshal(response, &historyResp); err != nil {
		return nil, fmt.Errorf("failed to parse funding history: %w", err)
	}

	fundingHistory := make([]FundingRateInfo, len(historyResp))
	for i, item := range historyResp {
		fundingRate, _ := strconv.ParseFloat(item.FundingRate, 64)
		fundingHistory[i] = FundingRateInfo{
			Symbol:          symbol,
			FundingRate:     fundingRate,
			LastFundingTime: time.Unix(item.FundingTime/1000, 0),
		}
	}

	return fundingHistory, nil
}

// FetchOpenInterest fetches open interest for a futures symbol
func (c *BinanceClient) FetchOpenInterest(symbol string) (float64, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/fapi/v1/openInterest", c.futuresBaseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return 0, fmt.Errorf("failed to fetch open interest: %w", err)
	}

	var openInterestResp struct {
		OpenInterest string `json:"openInterest"`
	}

	if err := json.Unmarshal(response, &openInterestResp); err != nil {
		return 0, fmt.Errorf("failed to parse open interest: %w", err)
	}

	return strconv.ParseFloat(openInterestResp.OpenInterest, 64)
}

// FetchMarkPrice fetches the mark price for a futures symbol
func (c *BinanceClient) FetchMarkPrice(symbol string) (float64, error) {
	fundingRate, err := c.FetchFundingRate(symbol)
	if err != nil {
		return 0, err
	}
	return fundingRate.MarkPrice, nil
}

// FetchIndexPrice fetches the index price for a futures symbol
func (c *BinanceClient) FetchIndexPrice(symbol string) (float64, error) {
	fundingRate, err := c.FetchFundingRate(symbol)
	if err != nil {
		return 0, err
	}
	return fundingRate.IndexPrice, nil
}

// GetFuturesPosition retrieves the current futures position for a symbol
func (c *BinanceClient) GetFuturesPosition(symbol string) (FuturesPosition, error) {
	positions, err := c.GetAllFuturesPositions()
	if err != nil {
		return FuturesPosition{}, err
	}

	for _, pos := range positions {
		if pos.Symbol == symbol {
			return pos, nil
		}
	}

	return FuturesPosition{}, errors.New("position not found")
}

// GetAllFuturesPositions retrieves all open futures positions
func (c *BinanceClient) GetAllFuturesPositions() ([]FuturesPosition, error) {
	endpoint := fmt.Sprintf("%s/fapi/v2/positionRisk", c.futuresBaseURL)
	params := url.Values{}
	params = c.addSignature(params)

	response, err := c.doGet(endpoint + "?" + params.Encode())
	if err != nil {
		return nil, fmt.Errorf("failed to get futures positions: %w", err)
	}

	var positionsResp []struct {
		Symbol           string `json:"symbol"`
		PositionAmt      string `json:"positionAmt"`
		EntryPrice       string `json:"entryPrice"`
		MarkPrice        string `json:"markPrice"`
		UnrealizedProfit string `json:"unRealizedProfit"`
		LiquidationPrice string `json:"liquidationPrice"`
		Leverage         string `json:"leverage"`
		MarginType       string `json:"marginType"`
		IsolatedMargin   string `json:"isolatedMargin"`
		PositionSide     string `json:"positionSide"`
		UpdateTime       int64  `json:"updateTime"`
	}

	if err := json.Unmarshal(response, &positionsResp); err != nil {
		return nil, fmt.Errorf("failed to parse futures positions: %w", err)
	}

	positions := make([]FuturesPosition, 0)
	for _, pos := range positionsResp {
		positionAmt, _ := strconv.ParseFloat(pos.PositionAmt, 64)
		if positionAmt == 0 {
			continue
		}

		entryPrice, _ := strconv.ParseFloat(pos.EntryPrice, 64)
		markPrice, _ := strconv.ParseFloat(pos.MarkPrice, 64)
		liquidationPrice, _ := strconv.ParseFloat(pos.LiquidationPrice, 64)
		unrealizedPnL, _ := strconv.ParseFloat(pos.UnrealizedProfit, 64)
		leverage, _ := strconv.Atoi(pos.Leverage)
		isolatedMargin, _ := strconv.ParseFloat(pos.IsolatedMargin, 64)

		side := "long"
		if positionAmt < 0 {
			side = "short"
			positionAmt = -positionAmt
		}

		ourSymbol := convertFromBinanceSymbol(pos.Symbol)
		positions = append(positions, FuturesPosition{
			Symbol:           ourSymbol,
			Side:             side,
			Size:             positionAmt,
			EntryPrice:       entryPrice,
			MarkPrice:        markPrice,
			LiquidationPrice: liquidationPrice,
			Margin:           isolatedMargin,
			UnrealizedPnL:    unrealizedPnL,
			Leverage:         leverage,
			MarginType:       strings.ToLower(pos.MarginType),
			PositionSide:     pos.PositionSide,
			UpdateTime:       time.Unix(pos.UpdateTime/1000, 0),
		})
	}

	return positions, nil
}

// SetLeverage sets the leverage for a futures symbol
func (c *BinanceClient) SetLeverage(symbol string, leverage int) error {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/fapi/v1/leverage", c.futuresBaseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("leverage", strconv.Itoa(leverage))
	params = c.addSignature(params)

	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		return fmt.Errorf("failed to set leverage: %w", err)
	}

	var respMap map[string]interface{}
	if err := json.Unmarshal(response, &respMap); err != nil {
		return fmt.Errorf("failed to parse leverage response: %w", err)
	}

	if code, exists := respMap["code"]; exists {
		var codeValue float64
		switch v := code.(type) {
		case float64:
			codeValue = v
		case int:
			codeValue = float64(v)
		case int64:
			codeValue = float64(v)
		default:
			// If code exists but is not a number, check if it's non-zero
			return fmt.Errorf("leverage error: unexpected code type: %T", code)
		}
		if codeValue != 0 {
			msg, _ := respMap["msg"].(string)
			if msg == "" {
				msg = "unknown error"
			}
			return fmt.Errorf("leverage error: %s", msg)
		}
	}

	return nil
}

// SetMarginType sets the margin type for a futures symbol
func (c *BinanceClient) SetMarginType(symbol string, marginType string) error {
	binanceSymbol := convertToBinanceSymbol(symbol)
	marginType = strings.ToUpper(marginType)
	if marginType != "ISOLATED" && marginType != "CROSSED" {
		return errors.New("margin type must be either ISOLATED or CROSSED")
	}

	endpoint := fmt.Sprintf("%s/fapi/v1/marginType", c.futuresBaseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("marginType", marginType)
	params = c.addSignature(params)

	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		return fmt.Errorf("failed to set margin type: %w", err)
	}

	var respMap map[string]interface{}
	if err := json.Unmarshal(response, &respMap); err != nil {
		return fmt.Errorf("failed to parse margin type response: %w", err)
	}

	if code, exists := respMap["code"]; exists {
		var codeValue float64
		switch v := code.(type) {
		case float64:
			codeValue = v
		case int:
			codeValue = float64(v)
		case int64:
			codeValue = float64(v)
		default:
			// If code exists but is not a number, check if it's non-zero
			return fmt.Errorf("margin type error: unexpected code type: %T", code)
		}
		if codeValue != 0 && codeValue != 200 {
			msg, _ := respMap["msg"].(string)
			if msg == "" {
				msg = "unknown error"
			}
			return fmt.Errorf("margin type error: %s", msg)
		}
	}

	return nil
}

// CancelAllOrders implementation for BinanceClient
func (c *BinanceClient) CancelAllOrders(symbol string) error {
	// Convert symbol format
	binanceSymbol := convertToBinanceSymbol(symbol)

	// Construct the API URL
	endpoint := fmt.Sprintf("%s/openOrders", c.apiPath("v3"))

	// Prepare the request parameters
	params := url.Values{}
	params.Add("symbol", binanceSymbol)

	// Add timestamp and signature
	params = c.addSignature(params)

	// Make the API call
	response, err := c.doDelete(endpoint + "?" + params.Encode())
	if err != nil {
		return fmt.Errorf("failed to cancel all orders: %w", err)
	}

	// Parse the response
	var cancelResponse []BinanceOrderResponse
	if err := json.Unmarshal(response, &cancelResponse); err != nil {
		return fmt.Errorf("failed to parse cancel response: %w", err)
	}

	return nil
}

type wsMessageSender interface {
	SendMessage([]byte) error
}

type binanceSubscription interface {
	StreamName() string
	Subscribe(wsMessageSender) error
}

// BinanceWebSocketClient manages WebSocket connections for Binance
type BinanceWebSocketClient struct {
	apiKey    string
	apiSecret string

	callbacks     map[string]interface{}
	subscriptions map[string]binanceSubscription
	mu            sync.RWMutex

	// State for merging ticker data (miniTicker + bookTicker)
	tickerState map[string]*tickerMergeState // key: symbol (normalized)
	tickerMu    sync.RWMutex

	connMu     sync.RWMutex
	ws         *gowscl.Client
	baseURL    string
	currentURL string
	connected  bool
	logger     *golog.Logger

	// HTTP client and base URL for REST API calls (e.g., listen key)
	httpClient  *gohttpcl.Client
	restBaseURL string

	// Rate limiting for subscriptions (Binance allows 5 messages/second)
	subRateLimiter *time.Ticker
	subRateMu      sync.Mutex
}

// tickerMergeState holds the latest data from both streams for merging
type tickerMergeState struct {
	ohlcv     *models.Ticker // Latest OHLCV data from miniTicker
	bid       float64        // Latest bid price from bookTicker
	ask       float64        // Latest ask price from bookTicker
	hasOHLCV  bool           // Whether we have OHLCV data
	hasBidAsk bool           // Whether we have bid/ask data
	callback  func(models.Ticker)
}

const (
	binanceWSComponent = "binance_ws"
	// wsMessageText matches the coder/websocket constant but avoids importing it directly.
	wsMessageText gowscl.MessageType = 1
)

// BinanceTicker represents a WebSocket miniTicker update (provides OHLCV data)
// Format: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-mini-ticker-stream
type BinanceTicker struct {
	EventType string `json:"e"` // Event type (e.g., "24hrMiniTicker")
	EventTime int64  `json:"E"` // Event time
	Symbol    string `json:"s"` // Symbol
	Close     string `json:"c"` // Close price (last price)
	Open      string `json:"o"` // Open price
	High      string `json:"h"` // High price
	Low       string `json:"l"` // Low price
	Volume    string `json:"v"` // Total traded base asset volume
	QuoteVol  string `json:"q"` // Total traded quote asset volume
}

// BinanceBookTicker represents a WebSocket bookTicker update (provides best bid/ask)
// Format: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-book-ticker-streams
type BinanceBookTicker struct {
	UpdateID int64  `json:"u"` // Order book update ID
	Symbol   string `json:"s"` // Symbol
	BidPrice string `json:"b"` // Best bid price
	BidQty   string `json:"B"` // Best bid quantity
	AskPrice string `json:"a"` // Best ask price
	AskQty   string `json:"A"` // Best ask quantity
}

// BinanceKlineStream represents a WebSocket kline update
type BinanceKlineStream struct {
	Symbol    string `json:"s"`
	Interval  string `json:"i"`
	OpenTime  int64  `json:"t"`
	CloseTime int64  `json:"T"`
	Open      string `json:"o"`
	Close     string `json:"c"`
	High      string `json:"h"`
	Low       string `json:"l"`
	Volume    string `json:"v"`
}

// BinanceTrade represents a WebSocket trade update
type BinanceTrade struct {
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
}

// BinanceDepthUpdate represents a WebSocket order book update
type BinanceDepthUpdate struct {
	Symbol    string     `json:"s"`
	Bids      [][]string `json:"b"`
	Asks      [][]string `json:"a"`
	EventTime int64      `json:"E"`
}

// BinanceUserDataUpdate represents a WebSocket user data update
type BinanceUserDataUpdate struct {
	EventType string                 `json:"e"`
	EventTime int64                  `json:"E"`
	Data      map[string]interface{} `json:"data"`
}

// binanceExecutionReport represents the executionReport payload from the user data stream.
// Reference: https://developers.binance.com/docs/binance-spot-api-docs/websocket-api#event-order-update
type binanceExecutionReport struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`

	ClientOrderID string `json:"c"`
	OrderSide     string `json:"S"`
	OrderType     string `json:"o"`
	TimeInForce   string `json:"f"`
	Price         string `json:"p"`
	Quantity      string `json:"q"`
	StopPrice     string `json:"P"`
	IcebergQty    string `json:"F"`

	OrderListID  int64  `json:"g"`
	OrigClientID string `json:"C"`
	OrderID      int64  `json:"i"`

	ExecutionType            string `json:"x"`
	OrderStatus              string `json:"X"`
	RejectReason             string `json:"r"`
	LastExecutedQuantity     string `json:"l"`
	CumulativeFilledQuantity string `json:"z"`
	LastExecutedPrice        string `json:"L"`
	CommissionAmount         string `json:"n"`
	CommissionAsset          string `json:"N"`

	TradeTime int64 `json:"T"`
	TradeID   int64 `json:"t"`
	IsMaker   bool  `json:"m"`
}

// toCommonOrder converts the executionReport payload to the unified Order model.
func (er binanceExecutionReport) toCommonOrder() (common.Order, error) {
	status, err := binanceStatusToCommon(er.OrderStatus)
	if err != nil {
		return common.Order{}, err
	}

	price, _ := strconv.ParseFloat(er.Price, 64)
	quantity, _ := strconv.ParseFloat(er.Quantity, 64)
	filled, _ := strconv.ParseFloat(er.CumulativeFilledQuantity, 64)
	remaining := quantity - filled
	if remaining < 0 {
		remaining = 0
	}

	orderTime := er.EventTime
	if orderTime == 0 {
		orderTime = er.TradeTime
	}

	return common.Order{
		ID:              strconv.FormatInt(er.OrderID, 10),
		ClientOrderID:   er.ClientOrderID,
		Symbol:          convertFromBinanceSymbol(er.Symbol),
		Side:            common.OrderSideFromString(strings.ToLower(er.OrderSide)),
		Type:            common.OrderTypeFromString(strings.ToLower(er.OrderType)),
		Status:          status,
		Price:           price,
		Amount:          quantity,
		FilledAmount:    filled,
		RemainingAmount: remaining,
		Fee:             0,
		FeeCurrency:     er.CommissionAsset,
		CreatedAt:       time.Unix(orderTime/1000, 0),
		UpdatedAt:       time.Unix(orderTime/1000, 0),
		Quantity:        quantity,
		Timestamp:       time.Unix(orderTime/1000, 0),
	}, nil
}

type BinanceTickerSubscription struct {
	Symbol string
}

func (s *BinanceTickerSubscription) StreamName() string {
	return fmt.Sprintf("%s@miniTicker", strings.ToLower(convertToBinanceSymbol(s.Symbol)))
}

func (s *BinanceTickerSubscription) Subscribe(sender wsMessageSender) error {
	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s"], "id": %d}`, s.StreamName(), time.Now().Unix()))
	return sender.SendMessage(msg)
}

type BinanceBookTickerSubscription struct {
	Symbol string
}

func (s *BinanceBookTickerSubscription) StreamName() string {
	return fmt.Sprintf("%s@bookTicker", strings.ToLower(convertToBinanceSymbol(s.Symbol)))
}

func (s *BinanceBookTickerSubscription) Subscribe(sender wsMessageSender) error {
	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s"], "id": %d}`, s.StreamName(), time.Now().Unix()))
	return sender.SendMessage(msg)
}

type BinanceKlineSubscription struct {
	Symbol   string
	Interval string
}

func (s *BinanceKlineSubscription) StreamName() string {
	return fmt.Sprintf("%s@kline_%s", strings.ToLower(convertToBinanceSymbol(s.Symbol)), s.Interval)
}

func (s *BinanceKlineSubscription) Subscribe(sender wsMessageSender) error {
	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s"], "id": %d}`, s.StreamName(), time.Now().Unix()))
	return sender.SendMessage(msg)
}

type BinanceTradeSubscription struct {
	Symbol string
}

func (s *BinanceTradeSubscription) StreamName() string {
	return fmt.Sprintf("%s@trade", strings.ToLower(convertToBinanceSymbol(s.Symbol)))
}

func (s *BinanceTradeSubscription) Subscribe(sender wsMessageSender) error {
	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s"], "id": %d}`, s.StreamName(), time.Now().Unix()))
	return sender.SendMessage(msg)
}

type BinanceDepthSubscription struct {
	Symbol string
}

func (s *BinanceDepthSubscription) StreamName() string {
	return fmt.Sprintf("%s@depth", strings.ToLower(convertToBinanceSymbol(s.Symbol)))
}

func (s *BinanceDepthSubscription) Subscribe(sender wsMessageSender) error {
	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s"], "id": %d}`, s.StreamName(), time.Now().Unix()))
	return sender.SendMessage(msg)
}

type BinanceUserDataSubscription struct {
	ListenKey string
}

func (s *BinanceUserDataSubscription) StreamName() string {
	return s.ListenKey
}

func (s *BinanceUserDataSubscription) Subscribe(sender wsMessageSender) error {
	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": ["%s"], "id": %d}`, s.ListenKey, time.Now().Unix()))
	return sender.SendMessage(msg)
}

// NewBinanceWebSocketClient creates a new WebSocket client
func NewBinanceWebSocketClient(wsURL, restBaseURL, apiKey, apiSecret string, httpClient *gohttpcl.Client) *BinanceWebSocketClient {
	client := &BinanceWebSocketClient{
		apiKey:         apiKey,
		apiSecret:      apiSecret,
		callbacks:      make(map[string]interface{}),
		subscriptions:  make(map[string]binanceSubscription),
		tickerState:    make(map[string]*tickerMergeState),
		baseURL:        wsURL,
		currentURL:     wsURL,
		logger:         common.DefaultLogger(),
		httpClient:     httpClient,
		restBaseURL:    restBaseURL,
		subRateLimiter: time.NewTicker(200 * time.Millisecond), // 5 messages per second max
	}
	client.replaceClient(wsURL)
	return client
}

func (c *BinanceWebSocketClient) replaceClient(url string) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.ws != nil {
		c.ws.Close()
	}
	c.currentURL = url
	c.connected = false
	c.ws = gowscl.NewClient(
		url,
		gowscl.WithLogger(common.DefaultLogger()),
		gowscl.WithInitialReconnect(1*time.Second),
		gowscl.WithMaxReconnect(60*time.Second),
		gowscl.WithReconnectFactor(2.0),
		gowscl.WithReconnectJitter(0.1),
		gowscl.WithOnMessage(func(data []byte, typ gowscl.MessageType) {
			if err := c.HandleMessage(data); err != nil {
				c.logger.Warnf("[%s] websocket message handling failed: %v", binanceWSComponent, err)
			}
		}),
		gowscl.WithOnOpen(func() {
			c.setConnected(true)
			if err := c.restoreSubscriptions(); err != nil {
				c.logger.Warnf("[%s] failed to restore subscriptions: %v", binanceWSComponent, err)
			}
		}),
		gowscl.WithOnClose(func() {
			c.setConnected(false)
		}),
		gowscl.WithOnError(func(err error) {
			// Filter out expected errors that occur during connection teardown or timeouts
			errMsg := err.Error()
			if strings.Contains(errMsg, "failed to ping: use of closed network connection") ||
				strings.Contains(errMsg, "failed to get reader: context deadline exceeded") {
				// These are expected errors during connection teardown or timeouts
				// Only log at debug level if connection is already closed
				c.connMu.RLock()
				isConnected := c.connected
				c.connMu.RUnlock()
				if !isConnected {
					// Connection is already closed, this is expected
					return
				}
				// Connection is still marked as connected, log at debug level
				c.logger.Debugf("[%s] websocket error (expected): %v", binanceWSComponent, err)
				return
			}
			// Log unexpected errors
			c.logger.Warnf("[%s] websocket error: %v", binanceWSComponent, err)
		}),
	)
}

func (c *BinanceWebSocketClient) setConnected(state bool) {
	c.connMu.Lock()
	c.connected = state
	c.connMu.Unlock()
}

// Connect establishes the websocket connection if needed.
func (c *BinanceWebSocketClient) Connect() error {
	c.connMu.RLock()
	ws := c.ws
	c.connMu.RUnlock()
	if ws == nil {
		c.replaceClient(c.baseURL)
		c.connMu.RLock()
		ws = c.ws
		c.connMu.RUnlock()
	}
	return ws.Connect()
}

// IsConnected reports whether the underlying websocket is connected.
func (c *BinanceWebSocketClient) IsConnected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connected
}

// SendMessage sends a raw JSON payload to Binance.
func (c *BinanceWebSocketClient) SendMessage(message []byte) error {
	c.connMu.RLock()
	ws := c.ws
	c.connMu.RUnlock()
	if ws == nil {
		return errors.New("websocket client not initialized")
	}
	return ws.Send(message, wsMessageText)
}

// batchSubscribe subscribes to multiple streams in a single SUBSCRIBE message.
// This helps avoid rate limiting (Binance allows 5 messages/second).
func (c *BinanceWebSocketClient) batchSubscribe(streamNames []string) error {
	if len(streamNames) == 0 {
		return nil
	}

	// Rate limit: wait for ticker to ensure we don't exceed 5 messages/second
	c.subRateMu.Lock()
	<-c.subRateLimiter.C
	c.subRateMu.Unlock()

	// Build batch subscription message
	paramsJSON, err := json.Marshal(streamNames)
	if err != nil {
		return fmt.Errorf("failed to marshal stream names: %w", err)
	}

	msg := []byte(fmt.Sprintf(`{"method": "SUBSCRIBE", "params": %s, "id": %d}`, paramsJSON, time.Now().Unix()))
	return c.SendMessage(msg)
}

// URL returns the current websocket endpoint.
func (c *BinanceWebSocketClient) URL() string {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	if c.currentURL != "" {
		return c.currentURL
	}
	return c.baseURL
}

// HandleMessage processes incoming WebSocket messages
func (c *BinanceWebSocketClient) HandleMessage(message []byte) error {
	// First, check if this is a subscription response
	var subResponse struct {
		Result interface{} `json:"result"`
		ID     int64       `json:"id"`
	}
	if err := json.Unmarshal(message, &subResponse); err == nil && subResponse.ID > 0 {
		return nil
	}

	// Try to parse as combined stream format: {"stream": "...", "data": {...}}
	var streamData struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(message, &streamData); err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	// If no stream wrapper, try to handle as direct user data message (executionReport, etc.)
	if streamData.Stream == "" {
		return c.handleDirectMessage(message)
	}

	c.mu.RLock()
	callback, ok := c.callbacks[streamData.Stream]
	c.mu.RUnlock()
	if !ok {
		// Log unhandled streams for debugging (could be subscription confirmations or other events)
		c.logger.Debugf("[%s] no callback registered for stream: %s", binanceWSComponent, streamData.Stream)
		return nil
	}

	switch {
	case strings.Contains(streamData.Stream, "@miniTicker"):
		var ticker BinanceTicker
		if err := json.Unmarshal(streamData.Data, &ticker); err != nil {
			return err
		}

		normalizedSymbol := strings.ToLower(ticker.Symbol)
		closePrice, _ := strconv.ParseFloat(ticker.Close, 64)
		openPrice, _ := strconv.ParseFloat(ticker.Open, 64)
		highPrice, _ := strconv.ParseFloat(ticker.High, 64)
		lowPrice, _ := strconv.ParseFloat(ticker.Low, 64)
		volume, _ := strconv.ParseFloat(ticker.Volume, 64)

		// Use EventTime if available, otherwise fallback to current time
		timestamp := time.Now()
		if ticker.EventTime > 0 {
			timestamp = time.Unix(ticker.EventTime/1000, 0)
		}

		// Create ticker with OHLCV data
		ohlcvTicker := models.Ticker{
			Exchange:  "Binance",
			Symbol:    convertFromBinanceSymbol(ticker.Symbol),
			LastPrice: closePrice,
			Open:      openPrice,
			High:      highPrice,
			Low:       lowPrice,
			Close:     closePrice,
			Volume:    volume,
			Timestamp: timestamp,
		}

		// Update merge state and emit if we have both OHLCV and bid/ask
		c.tickerMu.Lock()
		state, exists := c.tickerState[normalizedSymbol]
		if !exists {
			// Not a merged subscription, use direct callback
			c.tickerMu.Unlock()
			if cb, ok := callback.(func(models.Ticker)); ok {
				cb(ohlcvTicker)
			}
		} else {
			// Update OHLCV data
			state.ohlcv = &ohlcvTicker
			state.hasOHLCV = true
			callback := state.callback
			hasBidAsk := state.hasBidAsk
			bid := state.bid
			ask := state.ask
			c.tickerMu.Unlock()

			// Merge and emit if we have both
			if hasBidAsk {
				mergedTicker := ohlcvTicker
				mergedTicker.Bid = bid
				mergedTicker.Ask = ask
				callback(mergedTicker)
			}
		}
	case strings.Contains(streamData.Stream, "@bookTicker"):
		var bookTicker BinanceBookTicker
		if err := json.Unmarshal(streamData.Data, &bookTicker); err != nil {
			return err
		}

		normalizedSymbol := strings.ToLower(bookTicker.Symbol)
		bidPrice, _ := strconv.ParseFloat(bookTicker.BidPrice, 64)
		askPrice, _ := strconv.ParseFloat(bookTicker.AskPrice, 64)

		// Update merge state and emit if we have both OHLCV and bid/ask
		c.tickerMu.Lock()
		state, exists := c.tickerState[normalizedSymbol]
		if !exists {
			// Not a merged subscription, ignore
			c.tickerMu.Unlock()
			return nil
		}

		// Update bid/ask data
		state.bid = bidPrice
		state.ask = askPrice
		state.hasBidAsk = true
		callback := state.callback
		hasOHLCV := state.hasOHLCV
		ohlcv := state.ohlcv
		c.tickerMu.Unlock()

		// Merge and emit if we have both
		if hasOHLCV && ohlcv != nil {
			mergedTicker := *ohlcv
			mergedTicker.Bid = bidPrice
			mergedTicker.Ask = askPrice
			callback(mergedTicker)
		}
	case strings.Contains(streamData.Stream, "@kline"):
		var klineData struct {
			Kline BinanceKlineStream `json:"k"`
		}
		if err := json.Unmarshal(streamData.Data, &klineData); err != nil {
			return err
		}
		if cb, ok := callback.(func(models.Candle)); ok {
			open, _ := strconv.ParseFloat(klineData.Kline.Open, 64)
			high, _ := strconv.ParseFloat(klineData.Kline.High, 64)
			low, _ := strconv.ParseFloat(klineData.Kline.Low, 64)
			close, _ := strconv.ParseFloat(klineData.Kline.Close, 64)
			volume, _ := strconv.ParseFloat(klineData.Kline.Volume, 64)
			cb(models.Candle{
				Exchange:  "Binance",
				Symbol:    convertFromBinanceSymbol(klineData.Kline.Symbol),
				Interval:  klineData.Kline.Interval,
				OpenTime:  time.Unix(klineData.Kline.OpenTime/1000, 0),
				CloseTime: time.Unix(klineData.Kline.CloseTime/1000, 0),
				Open:      open,
				High:      high,
				Low:       low,
				Close:     close,
				Volume:    volume,
			})
		}
	case strings.Contains(streamData.Stream, "@trade"):
		var trade BinanceTrade
		if err := json.Unmarshal(streamData.Data, &trade); err != nil {
			return err
		}
		if cb, ok := callback.(func(models.Trade)); ok {
			price, _ := strconv.ParseFloat(trade.Price, 64)
			quantity, _ := strconv.ParseFloat(trade.Quantity, 64)
			cb(models.Trade{
				Exchange:      "Binance",
				Symbol:        convertFromBinanceSymbol(trade.Symbol),
				ID:            fmt.Sprintf("%d", trade.TradeID),
				Price:         price,
				Quantity:      quantity,
				ExecutionTime: time.Unix(trade.TradeTime/1000, 0),
			})
		}
	case strings.Contains(streamData.Stream, "@depth"):
		var depth BinanceDepthUpdate
		if err := json.Unmarshal(streamData.Data, &depth); err != nil {
			return err
		}
		if cb, ok := callback.(func(models.OrderBook)); ok {
			orderBook := models.OrderBook{
				Exchange:  "Binance",
				Symbol:    convertFromBinanceSymbol(depth.Symbol),
				Timestamp: time.Unix(depth.EventTime/1000, 0),
				Bids:      make([]models.OrderBookEntry, len(depth.Bids)),
				Asks:      make([]models.OrderBookEntry, len(depth.Asks)),
			}
			for i, bid := range depth.Bids {
				price, _ := strconv.ParseFloat(bid[0], 64)
				quantity, _ := strconv.ParseFloat(bid[1], 64)
				orderBook.Bids[i] = models.OrderBookEntry{
					Price:  price,
					Amount: quantity,
				}
			}
			for i, ask := range depth.Asks {
				price, _ := strconv.ParseFloat(ask[0], 64)
				quantity, _ := strconv.ParseFloat(ask[1], 64)
				orderBook.Asks[i] = models.OrderBookEntry{
					Price:  price,
					Amount: quantity,
				}
			}
			cb(orderBook)
		}
	default:
		var userData BinanceUserDataUpdate
		if err := json.Unmarshal(streamData.Data, &userData); err != nil {
			return err
		}
		// First, try typed order callback for executionReport events
		if orderCb, ok := callback.(func(common.Order)); ok && strings.EqualFold(userData.EventType, "executionReport") {
			var er binanceExecutionReport
			if err := json.Unmarshal(streamData.Data, &er); err != nil {
				return err
			}
			order, err := er.toCommonOrder()
			if err != nil {
				return err
			}
			orderCb(order)
			return nil
		}

		// Fallback to generic user-data callback
		if cb, ok := callback.(func(models.UserData)); ok {
			cb(models.UserData{
				Exchange:  "Binance",
				EventType: userData.EventType,
				Data:      userData.Data,
			})
		}
	}
	return nil
}

// handleDirectMessage handles messages that arrive without the combined stream wrapper.
// This can happen with user data streams when connected to /ws/<listenKey> endpoint
// instead of /stream endpoint, or in certain edge cases.
func (c *BinanceWebSocketClient) handleDirectMessage(message []byte) error {
	// Try to parse as execution report (order update)
	var eventType struct {
		EventType string `json:"e"`
	}
	if err := json.Unmarshal(message, &eventType); err != nil {
		c.logger.Debugf("[%s] failed to parse direct message event type: %v", binanceWSComponent, err)
		return nil
	}

	if eventType.EventType == "" {
		return nil
	}

	c.logger.Debugf("[%s] received direct message with event type: %s", binanceWSComponent, eventType.EventType)

	// Find any registered order update callback (registered with listen key)
	c.mu.RLock()
	var orderCallback func(common.Order)
	var userDataCallback func(models.UserData)
	for _, cb := range c.callbacks {
		if orderCb, ok := cb.(func(common.Order)); ok {
			orderCallback = orderCb
		}
		if userCb, ok := cb.(func(models.UserData)); ok {
			userDataCallback = userCb
		}
	}
	c.mu.RUnlock()

	switch strings.ToLower(eventType.EventType) {
	case "executionreport":
		if orderCallback != nil {
			var er binanceExecutionReport
			if err := json.Unmarshal(message, &er); err != nil {
				return fmt.Errorf("failed to parse direct executionReport: %w", err)
			}
			order, err := er.toCommonOrder()
			if err != nil {
				return fmt.Errorf("failed to convert executionReport to order: %w", err)
			}
			c.logger.Debugf("[%s] processing direct executionReport for order %s, status: %s",
				binanceWSComponent, order.ID, order.Status)
			orderCallback(order)
			return nil
		}
		c.logger.Warnf("[%s] received executionReport but no order callback registered", binanceWSComponent)

	case "outboundaccountposition", "balanceupdate", "listenstatus":
		// Handle other user data events via the generic callback
		if userDataCallback != nil {
			var userData BinanceUserDataUpdate
			if err := json.Unmarshal(message, &userData); err != nil {
				return fmt.Errorf("failed to parse direct user data: %w", err)
			}
			userDataCallback(models.UserData{
				Exchange:  "Binance",
				EventType: userData.EventType,
				Data:      userData.Data,
			})
			return nil
		}

	default:
		c.logger.Debugf("[%s] unhandled direct message event type: %s", binanceWSComponent, eventType.EventType)
	}

	return nil
}

// SubscribeToTicker subscribes to ticker updates (OHLCV + Bid/Ask)
// This subscribes to both @miniTicker (for OHLCV) and @bookTicker (for bid/ask) streams
// and merges the data before calling the callback.
func (c *BinanceWebSocketClient) SubscribeToTicker(symbol string, callback func(models.Ticker)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}

	// Normalize symbol for state tracking
	normalizedSymbol := strings.ToLower(convertToBinanceSymbol(symbol))

	// Initialize merge state
	c.tickerMu.Lock()
	c.tickerState[normalizedSymbol] = &tickerMergeState{
		callback: callback,
	}
	c.tickerMu.Unlock()

	// Subscribe to miniTicker stream (OHLCV data)
	miniTickerSub := &BinanceTickerSubscription{Symbol: symbol}
	miniTickerStreamName := miniTickerSub.StreamName()

	// Subscribe to bookTicker stream (bid/ask data)
	bookTickerSub := &BinanceBookTickerSubscription{Symbol: symbol}
	bookTickerStreamName := bookTickerSub.StreamName()

	// Batch subscribe to both streams in a single message to avoid rate limiting
	if err := c.batchSubscribe([]string{miniTickerStreamName, bookTickerStreamName}); err != nil {
		return fmt.Errorf("failed to batch subscribe to ticker streams: %w", err)
	}

	// Register callbacks for both streams (they will merge the data)
	c.mu.Lock()
	c.callbacks[miniTickerStreamName] = callback
	c.subscriptions[miniTickerStreamName] = miniTickerSub
	c.callbacks[bookTickerStreamName] = callback
	c.subscriptions[bookTickerStreamName] = bookTickerSub
	c.mu.Unlock()

	return nil
}

// SubscribeToKline subscribes to kline updates
func (c *BinanceWebSocketClient) SubscribeToKline(symbol, interval string, callback func(models.Candle)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	sub := &BinanceKlineSubscription{Symbol: symbol, Interval: interval}
	streamName := sub.StreamName()
	if err := c.batchSubscribe([]string{streamName}); err != nil {
		return fmt.Errorf("failed to subscribe to kline: %w", err)
	}
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
	c.mu.Unlock()
	return nil
}

// SubscribeToTrades subscribes to trade updates
func (c *BinanceWebSocketClient) SubscribeToTrades(symbol string, callback func(models.Trade)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	sub := &BinanceTradeSubscription{Symbol: symbol}
	streamName := sub.StreamName()
	if err := c.batchSubscribe([]string{streamName}); err != nil {
		return fmt.Errorf("failed to subscribe to trades: %w", err)
	}
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
	c.mu.Unlock()
	return nil
}

// SubscribeToDepth subscribes to order book updates
func (c *BinanceWebSocketClient) SubscribeToDepth(symbol string, callback func(models.OrderBook)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	sub := &BinanceDepthSubscription{Symbol: symbol}
	streamName := sub.StreamName()
	if err := c.batchSubscribe([]string{streamName}); err != nil {
		return fmt.Errorf("failed to subscribe to depth: %w", err)
	}
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
	c.mu.Unlock()
	return nil
}

// SubscribeToUserData subscribes to user data updates
func (c *BinanceWebSocketClient) SubscribeToUserData(callback func(models.UserData)) error {
	listenKey, err := c.getListenKey()
	if err != nil {
		return err
	}
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	sub := &BinanceUserDataSubscription{ListenKey: listenKey}
	streamName := sub.StreamName()
	if err := c.batchSubscribe([]string{streamName}); err != nil {
		return fmt.Errorf("failed to subscribe to user data: %w", err)
	}
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
	c.mu.Unlock()
	c.logger.Debugf("[%s] subscribed to user data stream: %s", binanceWSComponent, streamName)
	return nil
}

// SubscribeToOrderUpdates subscribes specifically to executionReport user data events
// and delivers them as unified common.Order objects via the provided callback.
func (c *BinanceWebSocketClient) SubscribeToOrderUpdates(callback func(common.Order)) error {
	listenKey, err := c.getListenKey()
	if err != nil {
		return err
	}
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	sub := &BinanceUserDataSubscription{ListenKey: listenKey}
	streamName := sub.StreamName()
	if err := c.batchSubscribe([]string{streamName}); err != nil {
		return fmt.Errorf("failed to subscribe to order updates: %w", err)
	}
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
	c.mu.Unlock()
	c.logger.Debugf("[%s] subscribed to order updates stream: %s", binanceWSComponent, streamName)
	return nil
}

// WaitForOrderConfirmation waits for an order status update via WebSocket.
// It subscribes to order updates if not already subscribed, then waits for the specific order ID.
// Returns the confirmed order status or an error if the timeout is reached.
// Parameters:
//   - orderID: the order ID to wait for (as string)
//   - timeout: maximum time to wait for confirmation
//   - acceptedStatuses: list of statuses that count as "confirmed" (e.g., NEW, FILLED, PARTIALLY_FILLED)
//     If empty, any status update for the order will be accepted.
func (c *BinanceWebSocketClient) WaitForOrderConfirmation(orderID string, timeout time.Duration, acceptedStatuses ...common.OrderStatus) (*common.Order, error) {
	if orderID == "" {
		return nil, fmt.Errorf("order ID is required")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	// Create a channel to receive the order confirmation
	confirmChan := make(chan common.Order, 1)
	errChan := make(chan error, 1)

	// Build a set of accepted statuses for fast lookup
	acceptedSet := make(map[common.OrderStatus]bool)
	for _, status := range acceptedStatuses {
		acceptedSet[status] = true
	}

	// Get listen key and subscribe if not already subscribed
	listenKey, err := c.getListenKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get listen key for order confirmation: %w", err)
	}

	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return nil, fmt.Errorf("failed to connect WebSocket for order confirmation: %w", err)
		}
	}

	streamName := listenKey

	// Check if already subscribed
	c.mu.RLock()
	_, alreadySubscribed := c.subscriptions[streamName]
	c.mu.RUnlock()

	// Create the confirmation callback
	confirmCallback := func(order common.Order) {
		if order.ID == orderID {
			// Check if the status is acceptable (or accept any if no filter specified)
			if len(acceptedSet) == 0 || acceptedSet[order.Status] {
				select {
				case confirmChan <- order:
					c.logger.Debugf("[%s] order %s confirmed with status: %s", binanceWSComponent, orderID, order.Status)
				default:
					// Channel already has a value, ignore duplicate
				}
			} else {
				c.logger.Debugf("[%s] order %s received status %s, waiting for: %v", binanceWSComponent, orderID, order.Status, acceptedStatuses)
			}
		}
	}

	if !alreadySubscribed {
		// Subscribe with our confirmation callback
		sub := &BinanceUserDataSubscription{ListenKey: listenKey}
		if err := c.batchSubscribe([]string{streamName}); err != nil {
			return nil, fmt.Errorf("failed to subscribe for order confirmation: %w", err)
		}
		c.mu.Lock()
		c.callbacks[streamName] = confirmCallback
		c.subscriptions[streamName] = sub
		c.mu.Unlock()
		c.logger.Debugf("[%s] subscribed for order confirmation, waiting for order %s", binanceWSComponent, orderID)
	} else {
		// Already subscribed, wrap the existing callback to also check for our order
		c.mu.Lock()
		existingCallback := c.callbacks[streamName]
		wrappedCallback := func(order common.Order) {
			// Call the confirmation check
			confirmCallback(order)
			// Also call the existing callback if it's an order callback
			if existingCb, ok := existingCallback.(func(common.Order)); ok {
				existingCb(order)
			}
		}
		c.callbacks[streamName] = wrappedCallback
		c.mu.Unlock()
		c.logger.Debugf("[%s] added confirmation listener for order %s to existing subscription", binanceWSComponent, orderID)
	}

	// Wait for confirmation or timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case order := <-confirmChan:
		return &order, nil
	case err := <-errChan:
		return nil, err
	case <-ctx.Done():
		return nil, fmt.Errorf("order %s confirmation timeout after %s (no websocket status received)", orderID, timeout)
	}
}

// getListenKey retrieves a listen key for user data streams.
// Binance API: POST /api/v3/userDataStream (spot) or POST /fapi/v1/listenKey (futures)
// The listen key is valid for 60 minutes and should be kept alive with PUT requests.
func (c *BinanceWebSocketClient) getListenKey() (string, error) {
	if c.httpClient == nil {
		return "", fmt.Errorf("HTTP client not available for listen key request")
	}
	if c.restBaseURL == "" {
		return "", fmt.Errorf("REST base URL not configured")
	}
	if c.apiKey == "" {
		return "", fmt.Errorf("API key required for user data stream")
	}

	// Use spot API endpoint for user data stream
	// For futures, this would be /fapi/v1/listenKey, but we'll use spot for now
	endpoint := fmt.Sprintf("%s/userDataStream", constructAPIPath(c.restBaseURL, "v3"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create request with X-MBX-APIKEY header
	// Note: userDataStream endpoint does NOT require signature, only API key
	headers := map[string]string{
		"X-MBX-APIKEY": c.apiKey,
		"Content-Type": "application/json",
	}
	options := headerOptions(headers)

	// Make POST request (empty body)
	resp, err := c.httpClient.Post(ctx, endpoint, bytes.NewReader(nil), binanceHTTPTimeout, nil, options...)
	if err != nil {
		return "", fmt.Errorf("failed to request listen key: %w", err)
	}
	defer resp.Body.Close()

	payload, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", fmt.Errorf("failed to read listen key response: %w", readErr)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", common.NewExchangeHTTPError(resp.StatusCode, payload, string(payload))
	}

	// Parse response: {"listenKey": "..."}
	var listenKeyResp struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal(payload, &listenKeyResp); err != nil {
		return "", fmt.Errorf("failed to parse listen key response: %w", err)
	}

	if listenKeyResp.ListenKey == "" {
		return "", fmt.Errorf("empty listen key in response")
	}

	return listenKeyResp.ListenKey, nil
}

// keepAliveListenKey extends the validity of a listen key.
// Binance listen keys expire after 60 minutes. This should be called periodically (e.g., every 30 minutes).
// Binance API: PUT /api/v3/userDataStream?listenKey=...
func (c *BinanceWebSocketClient) keepAliveListenKey(listenKey string) error {
	if c.httpClient == nil {
		return fmt.Errorf("HTTP client not available for listen key keep-alive")
	}
	if c.restBaseURL == "" {
		return fmt.Errorf("REST base URL not configured")
	}
	if c.apiKey == "" {
		return fmt.Errorf("API key required")
	}
	if listenKey == "" {
		return fmt.Errorf("listen key required")
	}

	endpoint := fmt.Sprintf("%s/userDataStream?listenKey=%s", constructAPIPath(c.restBaseURL, "v3"), url.QueryEscape(listenKey))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	headers := map[string]string{
		"X-MBX-APIKEY": c.apiKey,
		"Content-Type": "application/json",
	}
	options := headerOptions(headers)

	// Make PUT request (empty body)
	resp, err := c.httpClient.Put(ctx, endpoint, bytes.NewReader(nil), binanceHTTPTimeout, nil, options...)
	if err != nil {
		return fmt.Errorf("failed to keep listen key alive: %w", err)
	}
	defer resp.Body.Close()

	payload, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("failed to read keep-alive response: %w", readErr)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return common.NewExchangeHTTPError(resp.StatusCode, payload, string(payload))
	}

	return nil
}

// closeListenKey closes and invalidates a listen key.
// Binance API: DELETE /api/v3/userDataStream?listenKey=...
func (c *BinanceWebSocketClient) closeListenKey(listenKey string) error {
	if c.httpClient == nil {
		return fmt.Errorf("HTTP client not available for listen key close")
	}
	if c.restBaseURL == "" {
		return fmt.Errorf("REST base URL not configured")
	}
	if c.apiKey == "" {
		return fmt.Errorf("API key required")
	}
	if listenKey == "" {
		return fmt.Errorf("listen key required")
	}

	endpoint := fmt.Sprintf("%s/userDataStream?listenKey=%s", constructAPIPath(c.restBaseURL, "v3"), url.QueryEscape(listenKey))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	headers := map[string]string{
		"X-MBX-APIKEY": c.apiKey,
		"Content-Type": "application/json",
	}
	options := headerOptions(headers)

	// Make DELETE request
	resp, err := c.httpClient.Delete(ctx, endpoint, binanceHTTPTimeout, nil, options...)
	if err != nil {
		return fmt.Errorf("failed to close listen key: %w", err)
	}
	defer resp.Body.Close()

	payload, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("failed to read close response: %w", readErr)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return common.NewExchangeHTTPError(resp.StatusCode, payload, string(payload))
	}

	return nil
}

// restoreSubscriptions restores WebSocket subscriptions after reconnection
func (c *BinanceWebSocketClient) restoreSubscriptions() error {
	c.mu.RLock()
	subs := make([]binanceSubscription, 0, len(c.subscriptions))
	streamNames := make([]string, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		subs = append(subs, sub)
		streamNames = append(streamNames, sub.StreamName())
	}
	c.mu.RUnlock()

	if len(streamNames) == 0 {
		return nil
	}

	// Batch all subscriptions to avoid rate limiting
	// Binance allows up to 1024 streams per connection, so we can batch them all
	// But we'll still respect the rate limiter
	if err := c.batchSubscribe(streamNames); err != nil {
		return fmt.Errorf("failed to batch restore subscriptions: %w", err)
	}

	return nil
}

// GetWebSocketClient returns the WebSocket client
func (c *BinanceClient) GetWebSocketClient() *BinanceWebSocketClient {
	return c.wsClient
}
