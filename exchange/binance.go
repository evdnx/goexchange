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
	wsURL := "wss://stream.binance.com:9443"

	if testnet {
		// For demo accounts, use demo-api.binance.com
		// Note: Demo environment may not support all endpoints (e.g., /order/test)
		baseURL = "https://demo-api.binance.com/api"
		futuresBaseURL = "https://testnet.binancefuture.com" // Prepend "/fapi" for USDT-M or "/dapi" for Coin-M in endpoints, e.g., futuresBaseURL + "/fapi/v1/ticker/price"
		wsURL = "wss://demo-api.binance.com"                 // Append "/ws" for user data or "/stream" for market data/combined streams, e.g., wsURL + "/ws" or wsURL + "/stream?streams=..."
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

// constructAPIPath is a helper function that constructs the API path correctly
// based on whether baseURL already includes "/api"
func constructAPIPath(baseURL, version string) string {
	if strings.HasSuffix(baseURL, "/api") {
		return fmt.Sprintf("%s/%s", baseURL, version)
	}
	return fmt.Sprintf("%s/api/%s", baseURL, version)
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
		side := "sell"
		if trade.IsBuyerMaker {
			side = "buy"
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
	endpoint := fmt.Sprintf("%s/api/v3/order", c.baseURL)
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

// FindScalpingCoins analyzes all active trading pairs to find the most suitable coins for scalping.
// OPTIMIZED VERSION: Uses batch ticker fetching, parallel processing, and two-phase analysis.
// It uses an advanced multi-factor scoring algorithm that considers:
//   - Volume: 24h trading volume (normalized, log scale)
//   - Volatility: Daily price volatility from 1-minute candles (standard deviation of log returns)
//   - Order book liquidity: Weighted depth analysis (orders closer to mid price weighted more)
//   - Order book imbalance: Bid/ask ratio (slight preference for balanced books)
//   - Price impact: Estimated slippage for typical scalping trade sizes
//   - Spread: Bid-ask spread (strong penalty for wide spreads)
//
// The scoring formula: score = (volume^0.7 * volatility^0.8 * liquidity^0.6) / (spread^1.2 * impact^0.5 * imbalance^0.3)
//
// Parameters:
//   - quoteAsset: The quote currency to use (default: "USDT"). Must be a valid quote asset on Binance.
//   - minVolume: Minimum 24h volume threshold in quote currency (default: 1000000)
//   - topN: Number of top coins to return (default: 10)
//   - rateLimitDelay: Delay between API calls to respect rate limits (default: 100ms)
//   - maxSpread: Maximum allowed bid-ask spread percentage (default: 0.5). Coins with wider spreads are filtered out.
//
// Returns a sorted list of ScalpingCoin structs, ranked by enhanced multi-factor score.
// The algorithm prioritizes coins with high volume, good volatility, deep order books, tight spreads, and low slippage risk.
func (c *BinanceClient) FindScalpingCoins(quoteAsset string, minVolume float64, topN int, rateLimitDelay time.Duration, maxSpread float64) ([]ScalpingCoin, error) {
	// Use a timeout context to prevent indefinite hanging
	// Default timeout: 5 minutes (optimized version should be much faster)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Default values
	if quoteAsset == "" {
		quoteAsset = "USDT"
	}
	if minVolume <= 0 {
		minVolume = 1000000 // Default: 1M in quote currency
	}
	if topN <= 0 {
		topN = 10
	}
	if rateLimitDelay <= 0 {
		rateLimitDelay = 100 * time.Millisecond
	}
	if maxSpread <= 0 {
		maxSpread = 0.5 // Default: 0.5% maximum spread
	}

	quoteAsset = strings.ToUpper(quoteAsset)
	logger := common.DefaultLogger()

	// Get all trading pairs, filtering out Seed tokens (high-risk tokens with potential total loss)
	// Seed tokens are not suitable for scalping due to their high volatility and risk of total loss
	tradingPairs, err := c.GetTradingPairsWithFilter(true)
	if err != nil {
		return nil, fmt.Errorf("failed to get trading pairs: %w", err)
	}

	// Filter pairs by quote asset and build symbol map
	candidatePairs := make([]common.TradingPair, 0)
	symbolMap := make(map[string]common.TradingPair) // binanceSymbol -> TradingPair
	for _, pair := range tradingPairs {
		if strings.EqualFold(pair.QuoteAsset, quoteAsset) {
			candidatePairs = append(candidatePairs, pair)
			binanceSymbol := convertToBinanceSymbol(pair.Symbol)
			symbolMap[binanceSymbol] = pair
		}
	}

	if len(candidatePairs) == 0 {
		return nil, fmt.Errorf("no trading pairs found for quote asset %s", quoteAsset)
	}

	// PHASE 1: Batch fetch all 24h tickers in one API call (major optimization)
	allTickers, err := c.getAllTickers24hr(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch all tickers: %w", err)
	}

	// Filter and score candidates using ticker data only
	type candidateData struct {
		pair        common.TradingPair
		volume      float64
		spread      float64
		priceChange float64 // Use as volatility proxy
		lastPrice   float64
		bidPrice    float64
		askPrice    float64
		score       float64 // Preliminary score
	}

	candidates := make([]candidateData, 0)
	var volumeFiltered, spreadFiltered int

	for _, ticker := range allTickers {
		pair, exists := symbolMap[ticker.Symbol]
		if !exists {
			continue // Not a candidate pair
		}

		// Parse ticker values
		volume := ticker.parseVolume()
		lastPrice := ticker.parseLastPrice()
		bidPrice := ticker.parseBidPrice()
		askPrice := ticker.parseAskPrice()
		priceChangePercent := ticker.parsePriceChangePercent()

		// Validate basic price data
		if lastPrice <= 0 || bidPrice <= 0 || askPrice <= 0 {
			continue // Skip invalid price data
		}

		// Filter by minimum volume
		if volume < minVolume {
			volumeFiltered++
			continue
		}

		// Calculate spread - ensure askPrice >= bidPrice for valid spread
		var spread float64
		if askPrice >= bidPrice {
			midPrice := (bidPrice + askPrice) / 2
			if midPrice > 0 {
				spread = ((askPrice - bidPrice) / midPrice) * 100
			}
		} else {
			// Invalid spread (ask < bid), skip this coin
			continue
		}

		// Filter by maximum spread
		if spread > maxSpread {
			spreadFiltered++
			continue
		}

		// Use priceChangePercent as volatility proxy (absolute value)
		volatilityProxy := math.Abs(priceChangePercent)
		if volatilityProxy <= 0 {
			continue // Skip coins with no price movement
		}

		// Preliminary score using ticker data only (without order book)
		// This allows us to rank and only fetch expensive data for top candidates
		preliminaryScore := calculateScalpingScore(volume, volatilityProxy, 1.0, 1.0, spread, 1.0)

		candidates = append(candidates, candidateData{
			pair:        pair,
			volume:      volume,
			spread:      spread,
			priceChange: volatilityProxy,
			lastPrice:   lastPrice,
			bidPrice:    bidPrice,
			askPrice:    askPrice,
			score:       preliminaryScore,
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidates passed initial filters: volumeFiltered=%d, spreadFiltered=%d", volumeFiltered, spreadFiltered)
	}

	// Sort by preliminary score and take top candidates for detailed analysis
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	// Only analyze top 2*topN candidates in detail (to account for order book filtering)
	// This optimization reduces API calls while ensuring we have enough candidates
	// after detailed analysis filtering
	detailedCandidates := candidates
	maxDetailedCandidates := topN * 3 // Increased to 3x to account for filtering
	if len(candidates) > maxDetailedCandidates {
		detailedCandidates = candidates[:maxDetailedCandidates]
	}

	// PHASE 2: Parallel fetch detailed data (candles and order books) for top candidates
	type detailedResult struct {
		coin       ScalpingCoin
		err        error
		hasDetails bool
	}

	resultChan := make(chan detailedResult, len(detailedCandidates))
	semaphore := make(chan struct{}, 10) // Limit concurrent requests to 10

	var wg sync.WaitGroup
	since := time.Now().Add(-24 * time.Hour)

	for i, cand := range detailedCandidates {
		wg.Add(1)
		go func(cand candidateData, index int) {
			defer wg.Done()

			// Respect rate limiting: stagger requests to avoid hitting rate limits
			// Each goroutine waits a bit longer based on its index
			if index > 0 {
				delay := rateLimitDelay * time.Duration(index%10) // Stagger within batches of 10
				select {
				case <-ctx.Done():
					resultChan <- detailedResult{err: fmt.Errorf("context cancelled during rate limiting: %w", ctx.Err())}
					return
				case <-time.After(delay):
					// Continue after delay
				}
			}

			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			symbol := cand.pair.Symbol

			// Fetch candles for accurate volatility calculation
			candles, err := c.GetCandles(symbol, "1m", since, 1440)
			volatility := cand.priceChange // Default to price change if candles fail
			if err == nil && len(candles) >= 2 {
				volatility = calculateBinanceVolatility(candles)
			}

			if volatility <= 0 {
				resultChan <- detailedResult{err: fmt.Errorf("invalid volatility")}
				return
			}

			// Fetch order book for liquidity analysis
			orderBook, err := c.GetOrderBook(symbol, 20)
			liquidityScore, orderBookImbalance, priceImpactFactor := 1.0, 1.0, 1.0
			hasOrderBook := false
			if err == nil && orderBook != nil {
				hasOrderBook = true
				// Use the most accurate mid price available
				midPrice := cand.lastPrice
				if midPrice <= 0 {
					// Fallback to bid/ask average if lastPrice is invalid
					if cand.bidPrice > 0 && cand.askPrice > 0 && cand.askPrice >= cand.bidPrice {
						midPrice = (cand.bidPrice + cand.askPrice) / 2
					}
				}
				if midPrice > 0 {
					liquidityScore = calculateOrderBookLiquidity(orderBook, midPrice)
					orderBookImbalance = calculateOrderBookImbalance(orderBook)
					priceImpactFactor = calculatePriceImpactFactor(orderBook, midPrice)
				}
			}

			// Calculate final score with all factors
			finalScore := calculateScalpingScore(cand.volume, volatility, liquidityScore, orderBookImbalance, cand.spread, priceImpactFactor)

			coin := ScalpingCoin{
				Code:       cand.pair.BaseAsset,
				Name:       cand.pair.BaseAsset,
				Symbol:     symbol,
				Volume:     cand.volume,
				Volatility: volatility,
				Spread:     cand.spread,
				Score:      finalScore,
			}

			resultChan <- detailedResult{
				coin:       coin,
				hasDetails: hasOrderBook,
			}
		}(cand, i)
	}

	// Close channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	rankedCoins := make([]ScalpingCoin, 0)
	var candlesFetched, orderBooksFetched, volatilityFiltered int

	for result := range resultChan {
		if result.err != nil {
			continue
		}
		if result.coin.Volatility <= 0 {
			volatilityFiltered++
			continue
		}
		if result.hasDetails {
			orderBooksFetched++
		}
		candlesFetched++
		rankedCoins = append(rankedCoins, result.coin)
	}

	// Sort by final score
	sort.Slice(rankedCoins, func(i, j int) bool {
		return rankedCoins[i].Score > rankedCoins[j].Score
	})

	// Debug logging
	logger.Debugf("FindScalpingCoins (optimized): candidatePairs=%d, tickersBatch=1, candlesFetched=%d, orderBooksFetched=%d, volumeFiltered=%d, spreadFiltered=%d, volatilityFiltered=%d, ranked=%d",
		len(candidatePairs), candlesFetched, orderBooksFetched, volumeFiltered, spreadFiltered, volatilityFiltered, len(rankedCoins))

	// If no coins meet criteria, return error
	if len(rankedCoins) == 0 {
		return nil, fmt.Errorf("no coins passed all filters after detailed analysis")
	}

	// Return top N
	if len(rankedCoins) > topN {
		rankedCoins = rankedCoins[:topN]
	}

	return rankedCoins, nil
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

	connMu     sync.RWMutex
	ws         *gowscl.Client
	baseURL    string
	currentURL string
	connected  bool
	logger     *golog.Logger

	// HTTP client and base URL for REST API calls (e.g., listen key)
	httpClient  *gohttpcl.Client
	restBaseURL string
}

const (
	binanceWSComponent = "binance_ws"
	// wsMessageText matches the coder/websocket constant but avoids importing it directly.
	wsMessageText gowscl.MessageType = 1
)

// BinanceTicker represents a WebSocket ticker update
type BinanceTicker struct {
	Symbol    string `json:"s"`
	LastPrice string `json:"c"`
	Volume    string `json:"v"`
	BidPrice  string `json:"b"`
	AskPrice  string `json:"a"`
	CloseTime int64  `json:"C"`
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

type BinanceTickerSubscription struct {
	Symbol string
}

func (s *BinanceTickerSubscription) StreamName() string {
	return fmt.Sprintf("%s@ticker", strings.ToLower(convertToBinanceSymbol(s.Symbol)))
}

func (s *BinanceTickerSubscription) Subscribe(sender wsMessageSender) error {
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
		apiKey:        apiKey,
		apiSecret:     apiSecret,
		callbacks:     make(map[string]interface{}),
		subscriptions: make(map[string]binanceSubscription),
		baseURL:       wsURL,
		currentURL:    wsURL,
		logger:        common.DefaultLogger(),
		httpClient:    httpClient,
		restBaseURL:   restBaseURL,
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
	var subResponse struct {
		Result interface{} `json:"result"`
		ID     int64       `json:"id"`
	}
	if err := json.Unmarshal(message, &subResponse); err == nil && subResponse.ID > 0 {
		return nil
	}

	var streamData struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(message, &streamData); err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}
	if streamData.Stream == "" {
		return nil
	}

	c.mu.RLock()
	callback, ok := c.callbacks[streamData.Stream]
	c.mu.RUnlock()
	if !ok {
		return nil
	}

	switch {
	case strings.Contains(streamData.Stream, "@ticker"):
		var ticker BinanceTicker
		if err := json.Unmarshal(streamData.Data, &ticker); err != nil {
			return err
		}
		if cb, ok := callback.(func(models.Ticker)); ok {
			lastPrice, _ := strconv.ParseFloat(ticker.LastPrice, 64)
			volume, _ := strconv.ParseFloat(ticker.Volume, 64)
			bidPrice, _ := strconv.ParseFloat(ticker.BidPrice, 64)
			askPrice, _ := strconv.ParseFloat(ticker.AskPrice, 64)
			cb(models.Ticker{
				Exchange:  "Binance",
				Symbol:    convertFromBinanceSymbol(ticker.Symbol),
				LastPrice: lastPrice,
				Volume:    volume,
				Bid:       bidPrice,
				Ask:       askPrice,
				Timestamp: time.Unix(ticker.CloseTime/1000, 0),
			})
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

// SubscribeToTicker subscribes to ticker updates
func (c *BinanceWebSocketClient) SubscribeToTicker(symbol string, callback func(models.Ticker)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	sub := &BinanceTickerSubscription{Symbol: symbol}
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	streamName := sub.StreamName()
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
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
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	streamName := sub.StreamName()
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
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	streamName := sub.StreamName()
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
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	streamName := sub.StreamName()
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
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	streamName := sub.StreamName()
	c.mu.Lock()
	c.callbacks[streamName] = callback
	c.subscriptions[streamName] = sub
	c.mu.Unlock()
	return nil
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
	for _, sub := range c.subscriptions {
		subs = append(subs, sub)
	}
	c.mu.RUnlock()

	for _, sub := range subs {
		if err := sub.Subscribe(c); err != nil {
			return fmt.Errorf("failed to resubscribe to %s: %w", sub.StreamName(), err)
		}
	}
	return nil
}

// GetWebSocketClient returns the WebSocket client
func (c *BinanceClient) GetWebSocketClient() *BinanceWebSocketClient {
	return c.wsClient
}
