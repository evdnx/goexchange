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
	"net/http"
	"net/url"
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
	client.wsClient = NewBinanceWebSocketClient(wsURL, apiKey, apiSecret)
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
		return models.MarketData{}, errors.New("invalid kline data format")
	}

	openTime := int64(kline[0].(float64))
	open, _ := strconv.ParseFloat(kline[1].(string), 64)
	high, _ := strconv.ParseFloat(kline[2].(string), 64)
	low, _ := strconv.ParseFloat(kline[3].(string), 64)
	close, _ := strconv.ParseFloat(kline[4].(string), 64)
	volume, _ := strconv.ParseFloat(kline[5].(string), 64)

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
	params.Add("startTime", strconv.FormatInt(since.UnixNano()/int64(time.Millisecond), 10))
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
			return nil, fmt.Errorf("invalid kline data format")
		}

		openTime := int64(kline[0].(float64))
		open, _ := strconv.ParseFloat(kline[1].(string), 64)
		high, _ := strconv.ParseFloat(kline[2].(string), 64)
		low, _ := strconv.ParseFloat(kline[3].(string), 64)
		close, _ := strconv.ParseFloat(kline[4].(string), 64)
		volume, _ := strconv.ParseFloat(kline[5].(string), 64)
		closeTime := int64(kline[6].(float64))

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
func (c *BinanceClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	binanceSymbol := convertToBinanceSymbol(symbol)
	endpoint := fmt.Sprintf("%s/api/v3/depth", c.baseURL)
	params := url.Values{}
	params.Add("symbol", binanceSymbol)
	params.Add("limit", strconv.Itoa(depth))

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

// PlaceOrder places a spot market order
func (c *BinanceClient) PlaceOrder(order common.Order) (string, error) {
	binanceSymbol := convertToBinanceSymbol(order.Symbol)
	endpoint := fmt.Sprintf("%s/api/v3/order", c.baseURL)
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
	params.Add("quantity", strconv.FormatFloat(quantity, 'f', -1, 64))

	if order.Price > 0 {
		params.Add("price", strconv.FormatFloat(order.Price, 'f', -1, 64))
	}

	params = c.addSignature(params)
	response, err := c.doPost(endpoint, []byte(params.Encode()), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	})
	if err != nil {
		return "", fmt.Errorf("failed to place order: %w", err)
	}

	var orderResponse struct {
		OrderID int64 `json:"orderId"`
		BinanceResponse
	}

	if err := json.Unmarshal(response, &orderResponse); err != nil {
		return "", fmt.Errorf("failed to parse order response: %w", err)
	}

	if orderResponse.Code != 0 {
		return "", fmt.Errorf("order error: %s", orderResponse.Message)
	}

	return strconv.FormatInt(orderResponse.OrderID, 10), nil
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
	params.Add("startTime", strconv.FormatInt(since.UnixNano()/int64(time.Millisecond), 10))
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

	if code, exists := respMap["code"]; exists && code.(float64) != 0 {
		return fmt.Errorf("leverage error: %s", respMap["msg"].(string))
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

	if code, exists := respMap["code"]; exists && code.(float64) != 0 && code.(float64) != 200 {
		return fmt.Errorf("margin type error: %s", respMap["msg"].(string))
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
func NewBinanceWebSocketClient(wsURL, apiKey, apiSecret string) *BinanceWebSocketClient {
	client := &BinanceWebSocketClient{
		apiKey:        apiKey,
		apiSecret:     apiSecret,
		callbacks:     make(map[string]interface{}),
		subscriptions: make(map[string]binanceSubscription),
		baseURL:       wsURL,
		currentURL:    wsURL,
		logger:        common.DefaultLogger(),
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
// TODO: implement the actual REST call to fetch a listen key.
func (c *BinanceWebSocketClient) getListenKey() (string, error) {
	return "placeholder_listen_key", nil
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
