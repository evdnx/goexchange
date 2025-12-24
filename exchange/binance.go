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

// NewBinanceClient creates a new Binance client for spot and futures trading
func NewBinanceClient(apiKey, apiSecret string, testnet bool, metrics *metrics.Metrics) *BinanceClient {
	baseURL := "https://api.binance.com"
	futuresBaseURL := "https://fapi.binance.com"
	wsURL := "wss://stream.binance.com:9443"

	if testnet {
		baseURL = "https://testnet.binance.vision"
		futuresBaseURL = "https://testnet.binancefuture.com"
		wsURL = "wss://testnet.binance.vision"
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

// addSignature adds timestamp and HMAC SHA256 signature to request parameters
func (c *BinanceClient) addSignature(params url.Values) url.Values {
	timestamp := fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
	params.Add("timestamp", timestamp)
	payload := params.Encode()
	signature := createHMACSHA256Signature(payload, c.APISecret())
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

// createHMACSHA256Signature generates an HMAC SHA256 signature
func createHMACSHA256Signature(payload, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
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
	endpoint := fmt.Sprintf("%s/api/v3/klines", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/ticker/24hr", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/klines", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/trades", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/depth", c.baseURL)
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

// CreateOrder places a spot market order
func (c *BinanceClient) CreateOrder(symbol string, side common.OrderSide, orderType common.OrderType, amount, price float64) (*common.Order, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v3/order", c.baseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("side", strings.ToUpper(side.String()))
	params.Add("type", strings.ToUpper(orderType.String()))

	if strings.EqualFold(orderType.String(), common.OrderTypeLimit.String()) {
		params.Add("timeInForce", string(common.TimeInForceGTC))
	}

	quantity := amount
	if quantity <= 0 {
		return nil, fmt.Errorf("order quantity must be greater than 0")
	}
	params.Add("quantity", strconv.FormatFloat(quantity, 'f', -1, 64))

	if price > 0 {
		params.Add("price", strconv.FormatFloat(price, 'f', -1, 64))
	}

	params = c.addSignature(params)
	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
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
	endpoint := fmt.Sprintf("%s/api/v3/order", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/account", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/openOrders", c.baseURL)
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
	endpoint := fmt.Sprintf("%s/api/v3/allOrders", c.baseURL)
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

// GetTradingPairs returns all available trading pairs
func (c *BinanceClient) GetTradingPairs() ([]common.TradingPair, error) {
	endpoint := fmt.Sprintf("%s/api/v3/exchangeInfo", c.baseURL)
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

	var tradingPairs []common.TradingPair
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.Status == "TRADING" {
			tradingPairs = append(tradingPairs, common.TradingPair{
				Symbol:     symbol.Symbol,
				BaseAsset:  symbol.BaseAsset,
				QuoteAsset: symbol.QuoteAsset,
			})
		}
	}

	return tradingPairs, nil
}

// FindScalpingCoins analyzes all active trading pairs to find the most suitable coins for scalping.
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
	// Default timeout: 30 minutes (enough for analyzing many assets with rate limiting)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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
		rateLimitDelay = 100 * time.Millisecond // Binance allows faster rate limits than Swyftx
	}
	if maxSpread <= 0 {
		maxSpread = 0.5 // Default: 0.5% maximum spread (tighter than Swyftx for better scalping)
	}

	quoteAsset = strings.ToUpper(quoteAsset)

	// Get all trading pairs
	tradingPairs, err := c.GetTradingPairs()
	if err != nil {
		return nil, fmt.Errorf("failed to get trading pairs: %w", err)
	}

	// Filter pairs by quote asset
	var candidatePairs []common.TradingPair
	for _, pair := range tradingPairs {
		if strings.EqualFold(pair.QuoteAsset, quoteAsset) {
			candidatePairs = append(candidatePairs, pair)
		}
	}

	if len(candidatePairs) == 0 {
		return nil, fmt.Errorf("no trading pairs found for quote asset %s", quoteAsset)
	}

	// Analyze each pair
	var rankedCoins []ScalpingCoin
	var allAnalyzedCoins []ScalpingCoin // Track all coins for fallback
	since := time.Now().Add(-24 * time.Hour)

	// Debug counters
	var tickersFetched, candlesFetched, candlesInsufficient, volumeFiltered, volatilityFiltered, spreadFiltered, orderBooksFetched int

	var firstError error
	var errorCount int
	logger := common.DefaultLogger()

	for i, pair := range candidatePairs {
		// Check context cancellation before processing each pair
		select {
		case <-ctx.Done():
			// Return partial results if context is cancelled
			if len(rankedCoins) > 0 {
				sort.Slice(rankedCoins, func(i, j int) bool {
					return rankedCoins[i].Score > rankedCoins[j].Score
				})
				if len(rankedCoins) > topN {
					rankedCoins = rankedCoins[:topN]
				}
				return rankedCoins, fmt.Errorf("context cancelled: %w", ctx.Err())
			}
			return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
		default:
		}

		// Rate limiting: add delay between requests (except for the first one)
		if i > 0 {
			// Use context-aware sleep that respects cancellation
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during rate limiting: %w", ctx.Err())
			case <-time.After(rateLimitDelay):
				// Continue after delay
			}
		}

		symbol := pair.Symbol

		// Fetch 24h ticker first (more efficient - already has volume and bid/ask)
		ticker, err := c.GetTicker(symbol)
		if err != nil {
			// Track first error for debugging
			if firstError == nil {
				firstError = err
			}
			errorCount++
			// Log first few errors for debugging
			if errorCount <= 3 {
				logger.Debugf("GetTicker failed for %s: %v", symbol, err)
			}
			// Skip pairs that fail to fetch ticker
			continue
		}
		tickersFetched++

		// Get volume from ticker (24h volume in quote currency)
		totalVolume := ticker.Volume

		// Filter by minimum volume early (before expensive candle fetch)
		if totalVolume < minVolume {
			volumeFiltered++
			continue
		}

		// Calculate spread from ticker
		var spread float64
		if ticker.Bid > 0 && ticker.Ask > 0 && ticker.Ask >= ticker.Bid {
			midPrice := (ticker.Bid + ticker.Ask) / 2
			if midPrice > 0 {
				spread = ((ticker.Ask - ticker.Bid) / midPrice) * 100
			}
		}

		// Filter by maximum spread early (before expensive candle fetch)
		if spread > maxSpread {
			spreadFiltered++
			continue
		}

		// Fetch 24h of 1-minute candles for volatility calculation (1440 candles = 24 hours)
		candles, err := c.GetCandles(symbol, "1m", since, 1440)
		if err != nil {
			// Track first error for debugging
			if firstError == nil {
				firstError = err
			}
			errorCount++
			// Log first few errors for debugging
			if errorCount <= 3 {
				logger.Debugf("GetCandles failed for %s: %v", symbol, err)
			}
			// Skip pairs that fail to fetch candles
			continue
		}
		candlesFetched++

		if len(candles) < 2 {
			candlesInsufficient++
			continue
		}

		// Calculate volatility from close prices
		volatility := calculateBinanceVolatility(candles)

		// Filter by volatility
		if volatility <= 0 {
			volatilityFiltered++
			continue
		}

		// Fetch order book for liquidity analysis (depth 20 is good for scalping)
		// If order book fetch fails, use default values (no penalty, just no bonus)
		orderBook, err := c.GetOrderBook(symbol, 20)
		var liquidityScore, orderBookImbalance, priceImpactFactor float64 = 1.0, 1.0, 1.0
		if err == nil && orderBook != nil {
			orderBooksFetched++
			midPrice := ticker.LastPrice
			if midPrice <= 0 {
				midPrice = (ticker.Bid + ticker.Ask) / 2
			}
			if midPrice > 0 {
				// Calculate order book metrics
				liquidityScore = calculateOrderBookLiquidity(orderBook, midPrice)
				orderBookImbalance = calculateOrderBookImbalance(orderBook)
				priceImpactFactor = calculatePriceImpactFactor(orderBook, midPrice)
			}
		}

		// Enhanced scoring algorithm for scalping
		// Factors:
		// - Volume (normalized): Higher volume = better liquidity
		// - Volatility (normalized): Need price movement to profit
		// - Liquidity score: Deep order book = less slippage
		// - Order book imbalance: Can indicate momentum (slight preference for balanced)
		// Penalties:
		// - Spread: Wide spreads eat profits
		// - Price impact: Thin order books = high slippage risk
		score := calculateScalpingScore(totalVolume, volatility, liquidityScore, orderBookImbalance, spread, priceImpactFactor)

		// Create coin entry
		coin := ScalpingCoin{
			Code:       pair.BaseAsset,
			Name:       pair.BaseAsset, // Binance doesn't provide asset names in trading pairs
			Symbol:     symbol,
			Volume:     totalVolume,
			Volatility: volatility,
			Spread:     spread,
			Score:      score,
		}

		// Always track all analyzed coins for fallback
		allAnalyzedCoins = append(allAnalyzedCoins, coin)

		// Add to ranked coins if it meets all criteria
		rankedCoins = append(rankedCoins, coin)
	}

	// Sort by score (descending)
	sort.Slice(rankedCoins, func(i, j int) bool {
		return rankedCoins[i].Score > rankedCoins[j].Score
	})

	// Debug logging
	logger.Debugf("FindScalpingCoins: candidatePairs=%d, tickersFetched=%d, candlesFetched=%d, orderBooksFetched=%d, candlesInsufficient=%d, allAnalyzed=%d, volumeFiltered=%d, volatilityFiltered=%d, spreadFiltered=%d, ranked=%d",
		len(candidatePairs), tickersFetched, candlesFetched, orderBooksFetched, candlesInsufficient, len(allAnalyzedCoins), volumeFiltered, volatilityFiltered, spreadFiltered, len(rankedCoins))

	// If no coins were analyzed at all, this indicates a problem
	if len(allAnalyzedCoins) == 0 {
		errMsg := fmt.Sprintf("no coins could be analyzed: candidatePairs=%d, tickersFetched=%d, candlesFetched=%d, candlesInsufficient=%d, errors=%d",
			len(candidatePairs), tickersFetched, candlesFetched, candlesInsufficient, errorCount)
		if firstError != nil {
			errMsg += fmt.Sprintf(" - first error: %v", firstError)
		}
		return nil, fmt.Errorf("%s - check if GetTicker and GetCandles are working correctly", errMsg)
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
func calculatePriceImpactFactor(orderBook *models.OrderBook, midPrice float64) float64 {
	if orderBook == nil || midPrice <= 0 {
		return 1.0
	}

	// Estimate price impact for a trade size of 0.1% of typical daily volume
	// This represents a typical scalping trade size
	typicalTradeSize := midPrice * 0.001 // 0.1% of price in quote currency

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
	endpoint := fmt.Sprintf("%s/api/v3/openOrders", c.baseURL)

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
	endpoint := fmt.Sprintf("%s/api/v3/userDataStream", c.restBaseURL)

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

	endpoint := fmt.Sprintf("%s/api/v3/userDataStream?listenKey=%s", c.restBaseURL, url.QueryEscape(listenKey))

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

	endpoint := fmt.Sprintf("%s/api/v3/userDataStream?listenKey=%s", c.restBaseURL, url.QueryEscape(listenKey))

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
