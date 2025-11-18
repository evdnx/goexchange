package exchange

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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evdnx/goexchange/models"
	http_client "github.com/evdnx/gohttpcl"
	metrics "github.com/evdnx/gotrademetrics"
	"github.com/evdnx/gowscl"
)

// CoinbaseClient implements the ExchangeClient interface for the Coinbase exchange
type CoinbaseClient struct {
	*BaseClient
	httpClient   *coinbaseHTTPClient
	wsClient     *CoinbaseWebSocketClient
	baseURL      string
	wsURL        string
	passphrase   string
	isTestnet    bool
	metrics      *metrics.Metrics
	orderBooks   map[string]*models.OrderBook
	orderBookMux sync.RWMutex
	wsConnected  bool
}

// CoinbaseChannelType represents WebSocket channel types
type CoinbaseChannelType string

const (
	ChannelHeartbeat CoinbaseChannelType = "heartbeat"
	ChannelTicker    CoinbaseChannelType = "ticker"
	ChannelLevel2    CoinbaseChannelType = "level2"
	ChannelMatches   CoinbaseChannelType = "matches"
	ChannelUser      CoinbaseChannelType = "user"
)

const coinbaseHTTPTimeout = 10 * time.Second

// NewCoinbaseClient creates a new Coinbase client
func NewCoinbaseClient(apiKey, apiSecret, passphrase string, testnet bool, metrics *metrics.Metrics) *CoinbaseClient {
	baseURL := "https://api.exchange.coinbase.com"
	wsURL := "wss://ws-feed.exchange.coinbase.com"

	if testnet {
		baseURL = "https://api-public.sandbox.exchange.coinbase.com"
		wsURL = "wss://ws-feed-public.sandbox.exchange.coinbase.com"
	}

	client := &CoinbaseClient{
		BaseClient:  NewBaseClient("Coinbase", apiKey, apiSecret, testnet),
		baseURL:     baseURL,
		wsURL:       wsURL,
		passphrase:  passphrase,
		isTestnet:   testnet,
		metrics:     metrics,
		orderBooks:  make(map[string]*models.OrderBook),
		wsConnected: false,
	}

	client.httpClient = createCoinbaseHTTPClient(metrics)
	client.wsClient = NewCoinbaseWebSocketClient(wsURL, apiKey, apiSecret, passphrase)
	return client
}

// createCoinbaseHTTPClient creates a configured HTTP client for Coinbase API
func createCoinbaseHTTPClient(metrics *metrics.Metrics) *coinbaseHTTPClient {
	opts := []http_client.Option{
		http_client.WithTimeout(coinbaseHTTPTimeout),
		http_client.WithMaxRetries(5),
		http_client.WithMinBackoff(300 * time.Millisecond),
		http_client.WithMaxBackoff(20 * time.Second),
		http_client.WithBackoffFactor(2.5),
		http_client.WithBackoffStrategy(http_client.BackoffExponential),
		http_client.WithRetryBudget(0.2, time.Minute),
		http_client.WithDefaultHeader("User-Agent", "CryptoBot/1.0"),
	}
	if collector := newHTTPMetricsCollector(metrics, "Coinbase"); collector != nil {
		opts = append(opts, http_client.WithMetrics(collector))
	}
	return &coinbaseHTTPClient{
		client:  http_client.New(opts...),
		timeout: coinbaseHTTPTimeout,
	}
}

type coinbaseHTTPClient struct {
	client  *http_client.Client
	timeout time.Duration
}

func (c *coinbaseHTTPClient) Get(url string, headers map[string]string) ([]byte, error) {
	return c.request(context.Background(), http.MethodGet, url, nil, headers)
}

func (c *coinbaseHTTPClient) Post(url string, body []byte, headers map[string]string) ([]byte, error) {
	return c.request(context.Background(), http.MethodPost, url, body, headers)
}

func (c *coinbaseHTTPClient) Delete(url string, headers map[string]string) ([]byte, error) {
	return c.request(context.Background(), http.MethodDelete, url, nil, headers)
}

func (c *coinbaseHTTPClient) request(ctx context.Context, method, target string, body []byte, headers map[string]string) ([]byte, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("http client not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = coinbaseHTTPTimeout
	}
	options := coinbaseHeaderOptions(headers)

	var (
		resp *http.Response
		err  error
	)
	switch method {
	case http.MethodGet:
		resp, err = c.client.Get(ctx, target, timeout, nil, options...)
	case http.MethodPost:
		resp, err = c.client.Post(ctx, target, bytes.NewReader(body), timeout, nil, options...)
	case http.MethodDelete:
		resp, err = c.client.Delete(ctx, target, timeout, nil, options...)
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
		return nil, NewExchangeHTTPError(resp.StatusCode, payload, string(payload))
	}
	return payload, nil
}

func coinbaseHeaderOptions(headers map[string]string) []http_client.ReqOption {
	if len(headers) == 0 {
		return nil
	}
	options := make([]http_client.ReqOption, 0, len(headers))
	for k, v := range headers {
		options = append(options, http_client.WithHeader(k, v))
	}
	return options
}

// getHeaders returns authenticated headers for API requests
func (c *CoinbaseClient) getHeaders(method, endpoint, query, body string) map[string]string {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	u, err := url.Parse(endpoint)
	requestPath := "/"
	if err == nil {
		requestPath = u.Path
	}
	if query != "" {
		requestPath += "?" + query
	}

	message := timestamp + method + requestPath + body
	signature := c.generateSignature(message)

	return map[string]string{
		"CB-ACCESS-KEY":        c.APIKey(),
		"CB-ACCESS-SIGN":       signature,
		"CB-ACCESS-TIMESTAMP":  timestamp,
		"CB-ACCESS-PASSPHRASE": c.passphrase,
		"Content-Type":         "application/json",
	}
}

// generateSignature creates an HMAC-SHA256 signature
func (c *CoinbaseClient) generateSignature(message string) string {
	secretBytes, err := base64.StdEncoding.DecodeString(c.APISecret())
	if err != nil {
		secretBytes = []byte(c.APISecret())
	}
	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// convertToCoinbaseSymbol converts symbol format (e.g., "BTC/USDT" to "BTC-USD")
func convertToCoinbaseSymbol(symbol string) string {
	symbol = strings.Replace(symbol, "/", "-", 1)
	if strings.HasSuffix(symbol, "-USDT") {
		return strings.TrimSuffix(symbol, "USDT") + "USD"
	}
	return symbol
}

// FetchMarketData fetches market data for a symbol
func (c *CoinbaseClient) FetchMarketData(symbol string) (models.MarketData, error) {
	if c.wsConnected {
		c.orderBookMux.RLock()
		orderBook, exists := c.orderBooks[symbol]
		c.orderBookMux.RUnlock()

		if exists && !orderBook.Timestamp.IsZero() && time.Since(orderBook.Timestamp) < 30*time.Second {
			price := c.getLastPriceFromOrderBook(orderBook)
			if price > 0 {
				return models.MarketData{
					Symbol:    symbol,
					Timestamp: orderBook.Timestamp,
					Open:      price * 0.999,
					High:      price * 1.001,
					Low:       price * 0.999,
					Close:     price,
					Volume:    1000.0,
				}, nil
			}
		}
	}

	return c.fetchMarketDataHTTP(symbol)
}

// fetchMarketDataHTTP fetches market data via HTTP as fallback
func (c *CoinbaseClient) fetchMarketDataHTTP(symbol string) (models.MarketData, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/products/%s/ticker", c.baseURL, coinbaseSymbol)

	response, err := c.httpClient.Get(endpoint, c.getHeaders("GET", endpoint, "", ""))
	if err != nil {
		return models.MarketData{}, fmt.Errorf("failed to fetch ticker data: %w", err)
	}

	var ticker struct {
		Price  string `json:"price"`
		Volume string `json:"volume"`
	}

	if err := json.Unmarshal(response, &ticker); err != nil {
		return models.MarketData{}, fmt.Errorf("failed to decode ticker response: %w", err)
	}

	price, _ := strconv.ParseFloat(ticker.Price, 64)
	volume, _ := strconv.ParseFloat(ticker.Volume, 64)

	return models.MarketData{
		Symbol:    symbol,
		Timestamp: time.Now(),
		Open:      price * 0.999,
		High:      price * 1.001,
		Low:       price * 0.999,
		Close:     price,
		Volume:    volume,
	}, nil
}

// getLastPriceFromOrderBook extracts the mid-price from order book
func (c *CoinbaseClient) getLastPriceFromOrderBook(orderBook *models.OrderBook) float64 {
	if len(orderBook.Bids) > 0 && len(orderBook.Asks) > 0 {
		bestBid := orderBook.Bids[0]
		bestAsk := orderBook.Asks[0]
		return (bestBid.Price + bestAsk.Price) / 2.0
	}
	if len(orderBook.Bids) > 0 {
		return orderBook.Bids[0].Price
	}
	if len(orderBook.Asks) > 0 {
		return orderBook.Asks[0].Price
	}
	return 0.0
}

// GetTicker returns ticker information for a symbol
func (c *CoinbaseClient) GetTicker(symbol string) (*models.Ticker, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/products/%s/ticker", c.baseURL, coinbaseSymbol)

	response, err := c.httpClient.Get(endpoint, c.getHeaders("GET", endpoint, "", ""))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ticker: %w", err)
	}

	var tickerResponse struct {
		Price  string `json:"price"`
		Volume string `json:"volume"`
		Bid    string `json:"bid"`
		Ask    string `json:"ask"`
		Time   string `json:"time"`
	}

	if err := json.Unmarshal(response, &tickerResponse); err != nil {
		return nil, fmt.Errorf("failed to parse ticker: %w", err)
	}

	price, _ := strconv.ParseFloat(tickerResponse.Price, 64)
	volume, _ := strconv.ParseFloat(tickerResponse.Volume, 64)
	bid, _ := strconv.ParseFloat(tickerResponse.Bid, 64)
	ask, _ := strconv.ParseFloat(tickerResponse.Ask, 64)
	timestamp, err := time.Parse(time.RFC3339, tickerResponse.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to parse time: %w", err)
	}

	return &models.Ticker{
		Exchange:  c.GetName(),
		Symbol:    symbol,
		LastPrice: price,
		Volume:    volume,
		Bid:       bid,
		Ask:       ask,
		Timestamp: timestamp,
	}, nil
}

// GetCandles returns candlestick data for a symbol
func (c *CoinbaseClient) GetCandles(symbol, interval string, since time.Time, limit int) ([]models.Candle, error) {
	granularity, err := intervalToSeconds(interval)
	if err != nil {
		return nil, err
	}

	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/products/%s/candles", c.baseURL, coinbaseSymbol)
	params := url.Values{}
	params.Add("start", since.Format(time.RFC3339))
	params.Add("end", time.Now().Format(time.RFC3339))
	params.Add("granularity", strconv.Itoa(granularity))
	query := params.Encode()

	response, err := c.httpClient.Get(endpoint+"?"+query, c.getHeaders("GET", endpoint, query, ""))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch candles: %w", err)
	}

	var candlesResponse [][]float64
	if err := json.Unmarshal(response, &candlesResponse); err != nil {
		var objectResponse struct {
			Data [][]float64 `json:"data"`
		}
		if err2 := json.Unmarshal(response, &objectResponse); err2 == nil {
			candlesResponse = objectResponse.Data
		} else {
			return nil, fmt.Errorf("failed to parse candles: %w", err)
		}
	}

	candles := make([]models.Candle, len(candlesResponse))
	for i, candle := range candlesResponse {
		if len(candle) < 6 {
			return nil, fmt.Errorf("invalid candle data format")
		}
		candles[i] = models.Candle{
			Exchange:  c.GetName(),
			Symbol:    symbol,
			Interval:  interval,
			OpenTime:  time.Unix(int64(candle[0]), 0),
			CloseTime: time.Unix(int64(candle[0]), 0).Add(time.Duration(granularity) * time.Second),
			Open:      candle[3],
			High:      candle[2],
			Low:       candle[1],
			Close:     candle[4],
			Volume:    candle[5],
		}
	}

	return candles, nil
}

// intervalToSeconds converts interval string to seconds
func intervalToSeconds(interval string) (int, error) {
	if len(interval) < 2 {
		return 0, fmt.Errorf("invalid interval format: %s", interval)
	}
	valueStr := interval[:len(interval)-1]
	unit := strings.ToLower(interval[len(interval)-1:])
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return 0, fmt.Errorf("invalid interval value: %s", valueStr)
	}
	switch unit {
	case "m":
		return value * 60, nil
	case "h":
		return value * 3600, nil
	case "d":
		return value * 86400, nil
	default:
		return 0, fmt.Errorf("unsupported interval unit: %s", unit)
	}
}

// GetTrades returns trade history for a symbol
func (c *CoinbaseClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/products/%s/trades", c.baseURL, coinbaseSymbol)

	response, err := c.httpClient.Get(endpoint, c.getHeaders("GET", endpoint, "", ""))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trades: %w", err)
	}

	var tradesResponse []struct {
		Time    string `json:"time"`
		TradeID int64  `json:"trade_id"`
		Price   string `json:"price"`
		Size    string `json:"size"`
		Side    string `json:"side"`
	}

	if err := json.Unmarshal(response, &tradesResponse); err != nil {
		return nil, fmt.Errorf("failed to parse trades: %w", err)
	}

	if len(tradesResponse) > limit {
		tradesResponse = tradesResponse[:limit]
	}

	trades := make([]models.Trade, 0)
	for _, trade := range tradesResponse {
		price, _ := strconv.ParseFloat(trade.Price, 64)
		quantity, _ := strconv.ParseFloat(trade.Size, 64)
		timestamp, err := time.Parse(time.RFC3339, trade.Time)
		if err != nil {
			timestamp = time.Now()
		}
		if timestamp.Before(since) {
			continue
		}
		trades = append(trades, models.Trade{
			Exchange:      c.GetName(),
			Symbol:        symbol,
			ID:            strconv.FormatInt(trade.TradeID, 10),
			Type:          trade.Side,
			Price:         price,
			Quantity:      quantity,
			ExecutionTime: timestamp,
		})
	}

	return trades, nil
}

// GetOrderBook returns the order book for a symbol
func (c *CoinbaseClient) GetOrderBook(symbol string, depth int) (*models.OrderBook, error) {
	if c.wsConnected {
		if wsOrderBook, err := c.getOrderBookWebSocket(symbol); err == nil && len(wsOrderBook.Bids) > 0 && len(wsOrderBook.Asks) > 0 {
			if depth > 0 {
				if len(wsOrderBook.Bids) > depth {
					wsOrderBook.Bids = wsOrderBook.Bids[:depth]
				}
				if len(wsOrderBook.Asks) > depth {
					wsOrderBook.Asks = wsOrderBook.Asks[:depth]
				}
			}
			return wsOrderBook, nil
		}
	}

	return c.getOrderBookHTTP(symbol, depth)
}

// getOrderBookWebSocket returns the order book from WebSocket cache
func (c *CoinbaseClient) getOrderBookWebSocket(symbol string) (*models.OrderBook, error) {
	c.orderBookMux.RLock()
	defer c.orderBookMux.RUnlock()

	orderBook, exists := c.orderBooks[symbol]
	if !exists || time.Since(orderBook.Timestamp) > 30*time.Second {
		return nil, fmt.Errorf("order book not available or stale for %s", symbol)
	}

	return &models.OrderBook{
		Exchange:  c.GetName(),
		Symbol:    orderBook.Symbol,
		Timestamp: orderBook.Timestamp,
		Bids:      orderBook.Bids,
		Asks:      orderBook.Asks,
	}, nil
}

// getOrderBookHTTP fetches the order book via HTTP
func (c *CoinbaseClient) getOrderBookHTTP(symbol string, depth int) (*models.OrderBook, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/products/%s/book", c.baseURL, coinbaseSymbol)
	params := url.Values{}
	level := 2
	if depth <= 1 {
		level = 1
	} else if depth > 50 {
		level = 3
	}
	params.Add("level", strconv.Itoa(level))
	query := params.Encode()

	response, err := c.httpClient.Get(endpoint+"?"+query, c.getHeaders("GET", endpoint, query, ""))
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

// PlaceOrder places a market or limit order
func (c *CoinbaseClient) PlaceOrder(order Order) (string, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(order.Symbol)
	endpoint := fmt.Sprintf("%s/orders", c.baseURL)

	orderRequest := map[string]interface{}{
		"product_id": coinbaseSymbol,
		"side":       strings.ToLower(order.Side.String()),
		"type":       strings.ToLower(order.Type.String()),
	}

	if order.Type == "limit" {
		orderRequest["price"] = strconv.FormatFloat(order.Price, 'f', -1, 64)
		orderRequest["size"] = strconv.FormatFloat(order.Amount, 'f', -1, 64)
		orderRequest["post_only"] = true
	} else {
		if order.Side == "buy" {
			orderRequest["funds"] = strconv.FormatFloat(order.Price*order.Amount, 'f', -1, 64)
		} else {
			orderRequest["size"] = strconv.FormatFloat(order.Amount, 'f', -1, 64)
		}
	}

	body, err := json.Marshal(orderRequest)
	if err != nil {
		return "", fmt.Errorf("failed to marshal order request: %w", err)
	}

	response, err := c.httpClient.Post(endpoint, body, c.getHeaders("POST", endpoint, "", string(body)))
	if err != nil {
		return "", fmt.Errorf("failed to place order: %w", err)
	}

	var orderResponse struct {
		ID      string `json:"id"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(response, &orderResponse); err != nil {
		return "", fmt.Errorf("failed to parse order response: %w", err)
	}

	if orderResponse.Message != "" {
		return "", fmt.Errorf("order error: %s", orderResponse.Message)
	}

	return orderResponse.ID, nil
}

// CancelOrder cancels an order
func (c *CoinbaseClient) CancelOrder(symbol, orderID string) error {
	endpoint := fmt.Sprintf("%s/orders/%s", c.baseURL, orderID)
	response, err := c.httpClient.Delete(endpoint, c.getHeaders("DELETE", endpoint, "", ""))
	if err != nil {
		return fmt.Errorf("failed to cancel order: %w", err)
	}

	if len(response) > 0 {
		var errorResponse struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(response, &errorResponse); err == nil && errorResponse.Message != "" {
			return fmt.Errorf("cancel error: %s", errorResponse.Message)
		}
	}

	return nil
}

// GetOrderStatus retrieves the status of an order
func (c *CoinbaseClient) GetOrderStatus(orderID string) (OrderStatus, error) {
	endpoint := fmt.Sprintf("%s/orders/%s", c.baseURL, orderID)
	response, err := c.httpClient.Get(endpoint, c.getHeaders("GET", endpoint, "", ""))
	if err != nil {
		return "", fmt.Errorf("failed to get order status: %w", err)
	}

	var orderResponse struct {
		Status     string `json:"status"`
		FilledSize string `json:"filled_size"`
		Size       string `json:"size"`
		Message    string `json:"message"`
	}

	if err := json.Unmarshal(response, &orderResponse); err != nil {
		return "", fmt.Errorf("failed to parse order status: %w", err)
	}

	if orderResponse.Message != "" {
		return "", fmt.Errorf("status error: %s", orderResponse.Message)
	}

	switch orderResponse.Status {
	case "open", "pending", "active":
		return OrderStatusNew, nil
	case "done", "settled":
		if orderResponse.FilledSize == orderResponse.Size {
			return OrderStatusFilled, nil
		}
		return OrderStatusCancelled, nil
	case "rejected", "cancelled":
		return OrderStatusCancelled, nil
	default:
		return "", fmt.Errorf("unknown order status: %s", orderResponse.Status)
	}
}

// GetBalance returns the balance for a specific asset
func (c *CoinbaseClient) GetBalance(asset string) (*Balance, error) {
	balances, err := c.GetBalances()
	if err != nil {
		return nil, err
	}
	return balances[asset], nil
}

// GetBalances returns all account balances
func (c *CoinbaseClient) GetBalances() (map[string]*Balance, error) {
	endpoint := fmt.Sprintf("%s/accounts", c.baseURL)
	response, err := c.httpClient.Get(endpoint, c.getHeaders("GET", endpoint, "", ""))
	if err != nil {
		return nil, fmt.Errorf("failed to get accounts: %w", err)
	}

	var accounts []struct {
		Currency  string `json:"currency"`
		Available string `json:"available"`
		Hold      string `json:"hold"`
	}

	if err := json.Unmarshal(response, &accounts); err != nil {
		return nil, fmt.Errorf("failed to parse accounts: %w", err)
	}

	balances := make(map[string]*Balance)
	for _, account := range accounts {
		balances[account.Currency] = &Balance{
			Asset:  account.Currency,
			Free:   account.Available,
			Locked: account.Hold,
		}
	}

	return balances, nil
}

// GetOpenOrders retrieves all open orders for a symbol
func (c *CoinbaseClient) GetOpenOrders(symbol string) ([]Order, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/orders", c.baseURL)
	params := url.Values{}
	params.Add("product_id", coinbaseSymbol)
	params.Add("status", "open")
	query := params.Encode()

	response, err := c.httpClient.Get(endpoint+"?"+query, c.getHeaders("GET", endpoint, query, ""))
	if err != nil {
		return nil, fmt.Errorf("failed to get open orders: %w", err)
	}

	var openOrders []struct {
		ID        string `json:"id"`
		Price     string `json:"price"`
		Size      string `json:"size"`
		Side      string `json:"side"`
		Type      string `json:"type"`
		CreatedAt string `json:"created_at"`
	}

	if err := json.Unmarshal(response, &openOrders); err != nil {
		return nil, fmt.Errorf("failed to parse open orders: %w", err)
	}

	orders := make([]Order, len(openOrders))
	for i, o := range openOrders {
		price, _ := strconv.ParseFloat(o.Price, 64)
		quantity, _ := strconv.ParseFloat(o.Size, 64)
		timestamp, err := time.Parse(time.RFC3339, o.CreatedAt)
		if err != nil {
			timestamp = time.Now()
		}
		orders[i] = Order{
			ID:        o.ID,
			Symbol:    symbol,
			Side:      OrderSideFromString(o.Side),
			Type:      OrderTypeFromString(o.Type),
			Amount:    quantity,
			Price:     price,
			CreatedAt: timestamp,
			Quantity:  quantity,
			Timestamp: timestamp,
		}
	}

	return orders, nil
}

// GetOrders retrieves order history for a symbol
func (c *CoinbaseClient) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	endpoint := fmt.Sprintf("%s/orders", c.baseURL)
	params := url.Values{}
	params.Add("product_id", coinbaseSymbol)
	query := params.Encode()

	response, err := c.httpClient.Get(endpoint+"?"+query, c.getHeaders("GET", endpoint, query, ""))
	if err != nil {
		return nil, fmt.Errorf("failed to get orders: %w", err)
	}

	var ordersResponse []struct {
		ID        string `json:"id"`
		Price     string `json:"price"`
		Size      string `json:"size"`
		Side      string `json:"side"`
		Type      string `json:"type"`
		CreatedAt string `json:"created_at"`
	}

	if err := json.Unmarshal(response, &ordersResponse); err != nil {
		return nil, fmt.Errorf("failed to parse orders: %w", err)
	}

	orders := make([]Order, 0)
	for _, order := range ordersResponse {
		createdAt, err := time.Parse(time.RFC3339, order.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %w", err)
		}
		if createdAt.Before(since) {
			continue
		}
		price, _ := strconv.ParseFloat(order.Price, 64)
		quantity, _ := strconv.ParseFloat(order.Size, 64)
		orders = append(orders, Order{
			ID:        order.ID,
			Symbol:    symbol,
			Side:      OrderSideFromString(order.Side),
			Type:      OrderTypeFromString(order.Type),
			Amount:    quantity,
			Price:     price,
			CreatedAt: createdAt,
			Quantity:  quantity,
			Timestamp: createdAt,
		})
		if len(orders) >= limit {
			break
		}
	}

	return orders, nil
}

// GetTradingPairs returns all available trading pairs
func (c *CoinbaseClient) GetTradingPairs() ([]TradingPair, error) {
	endpoint := fmt.Sprintf("%s/products", c.baseURL)
	response, err := c.httpClient.Get(endpoint, c.getHeaders("GET", endpoint, "", ""))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch products: %w", err)
	}

	var products []struct {
		ID            string `json:"id"`
		BaseCurrency  string `json:"base_currency"`
		QuoteCurrency string `json:"quote_currency"`
		Status        string `json:"status"`
	}

	if err := json.Unmarshal(response, &products); err != nil {
		return nil, fmt.Errorf("failed to parse products: %w", err)
	}

	var tradingPairs []TradingPair
	for _, product := range products {
		if product.Status == "online" {
			tradingPairs = append(tradingPairs, TradingPair{
				Symbol:     product.ID,
				BaseAsset:  product.BaseCurrency,
				QuoteAsset: product.QuoteCurrency,
			})
		}
	}

	return tradingPairs, nil
}

// StartWebSocketOrderBook initializes WebSocket for order book updates
func (c *CoinbaseClient) StartWebSocketOrderBook(symbol string) error {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	snapshot, err := c.getOrderBookHTTP(symbol, 50)
	if err == nil {
		c.orderBookMux.Lock()
		c.orderBooks[symbol] = snapshot
		c.orderBookMux.Unlock()
	}

	err = c.wsClient.SubscribeToLevel2(coinbaseSymbol, func(update CoinbaseL2Update) {
		c.handleOrderBookUpdate(symbol, update)
	})
	if err != nil {
		c.wsConnected = false
		return fmt.Errorf("failed to subscribe to level2 updates: %w", err)
	}

	c.wsConnected = true
	return nil
}

// handleOrderBookUpdate processes WebSocket order book updates
func (c *CoinbaseClient) handleOrderBookUpdate(symbol string, update CoinbaseL2Update) {
	c.orderBookMux.Lock()
	defer c.orderBookMux.Unlock()

	orderBook, exists := c.orderBooks[symbol]
	if !exists {
		orderBook = &models.OrderBook{
			Exchange:  c.GetName(),
			Symbol:    symbol,
			Timestamp: time.Now(),
			Bids:      make([]models.OrderBookEntry, 0),
			Asks:      make([]models.OrderBookEntry, 0),
		}
		c.orderBooks[symbol] = orderBook
	}

	for _, change := range update.Changes {
		if len(change) < 3 {
			continue
		}
		price, err := strconv.ParseFloat(change[1], 64)
		if err != nil {
			continue
		}
		size, err := strconv.ParseFloat(change[2], 64)
		if err != nil {
			continue
		}

		side := change[0]
		targetSide := &orderBook.Bids
		isBid := side == "buy"
		if !isBid {
			targetSide = &orderBook.Asks
		}

		updateOrderBookSide(targetSide, price, size, isBid)
	}

	orderBook.Timestamp = time.Now()
}

// CancelAllOrders implementation for CoinbaseClient
func (c *CoinbaseClient) CancelAllOrders(symbol string) error {
	// Construct the API URL
	endpoint := fmt.Sprintf("%s/orders", c.baseURL)

	// Add query parameters if a symbol is provided
	if symbol != "" {
		endpoint = fmt.Sprintf("%s?product_id=%s", endpoint, symbol)
	}

	// Make the API call
	_, err := c.httpClient.Delete(endpoint, c.getHeaders("DELETE", endpoint, "", ""))
	if err != nil {
		return fmt.Errorf("failed to cancel all orders: %w", err)
	}

	return nil
}

// updateOrderBookSide updates bids or asks in the order book
func updateOrderBookSide(side *[]models.OrderBookEntry, price, size float64, isBid bool) {
	for i, entry := range *side {
		if entry.Price == price {
			if size == 0 {
				*side = append((*side)[:i], (*side)[i+1:]...)
			} else {
				(*side)[i] = models.OrderBookEntry{Price: price, Amount: size}
			}
			return
		}
	}

	if size > 0 {
		newEntry := models.OrderBookEntry{Price: price, Amount: size}
		inserted := false
		for i, entry := range *side {
			shouldInsertHere := (isBid && entry.Price < price) || (!isBid && entry.Price > price)
			if shouldInsertHere {
				*side = append((*side)[:i], append([]models.OrderBookEntry{newEntry}, (*side)[i:]...)...)
				inserted = true
				break
			}
		}
		if !inserted {
			*side = append(*side, newEntry)
		}
	}
}

// CoinbaseWebSocketClient manages WebSocket connections
type CoinbaseWebSocketClient struct {
	apiKey     string
	apiSecret  string
	passphrase string

	callbacks     map[string]interface{}
	mu            sync.RWMutex
	subscriptions map[string]bool

	connMu    sync.RWMutex
	ws        *gowscl.Client
	wsURL     string
	connected bool
}

const (
	coinbaseWSComponent                      = "coinbase_ws"
	coinbaseWSMessageText gowscl.MessageType = 1
)

type coinbaseMessageSender interface {
	SendMessage([]byte) error
}

type coinbaseSubscription interface {
	Subscribe(coinbaseMessageSender) error
}

// CoinbaseTicker represents a WebSocket ticker update
type CoinbaseTicker struct {
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Volume24h string `json:"volume_24h"`
	BestBid   string `json:"best_bid"`
	BestAsk   string `json:"best_ask"`
	Time      string `json:"time"`
	TradeID   int64  `json:"trade_id"`
}

// CoinbaseL2Update represents a WebSocket level2 update
type CoinbaseL2Update struct {
	ProductID string     `json:"product_id"`
	Changes   [][]string `json:"changes"`
	Time      string     `json:"time"`
}

// CoinbaseMatch represents a WebSocket trade update
type CoinbaseMatch struct {
	ProductID string `json:"product_id"`
	TradeID   int64  `json:"trade_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	Time      string `json:"time"`
}

// CoinbaseHeartbeat represents a WebSocket heartbeat
type CoinbaseHeartbeat struct {
	ProductID string `json:"product_id"`
	Time      string `json:"time"`
}

// CoinbaseSubscription represents a WebSocket subscription request
type CoinbaseSubscription struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
	Signature  string   `json:"signature,omitempty"`
	Key        string   `json:"key,omitempty"`
	Passphrase string   `json:"passphrase,omitempty"`
	Timestamp  string   `json:"timestamp,omitempty"`
}

// Subscription implementations
type CoinbaseTickerSubscription struct {
	ProductID string
}

func (s *CoinbaseTickerSubscription) Subscribe(sender coinbaseMessageSender) error {
	subscription := CoinbaseSubscription{
		Type:       "subscribe",
		ProductIDs: []string{s.ProductID},
		Channels:   []string{string(ChannelTicker)},
	}
	data, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}
	return sender.SendMessage(data)
}

type CoinbaseLevel2Subscription struct {
	ProductID string
}

func (s *CoinbaseLevel2Subscription) Subscribe(sender coinbaseMessageSender) error {
	subscription := CoinbaseSubscription{
		Type:       "subscribe",
		ProductIDs: []string{s.ProductID},
		Channels:   []string{string(ChannelLevel2)},
	}
	data, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}
	return sender.SendMessage(data)
}

type CoinbaseMatchesSubscription struct {
	ProductID string
}

func (s *CoinbaseMatchesSubscription) Subscribe(sender coinbaseMessageSender) error {
	subscription := CoinbaseSubscription{
		Type:       "subscribe",
		ProductIDs: []string{s.ProductID},
		Channels:   []string{string(ChannelMatches)},
	}
	data, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}
	return sender.SendMessage(data)
}

type CoinbaseHeartbeatSubscription struct {
	ProductID string
}

func (s *CoinbaseHeartbeatSubscription) Subscribe(sender coinbaseMessageSender) error {
	subscription := CoinbaseSubscription{
		Type:       "subscribe",
		ProductIDs: []string{s.ProductID},
		Channels:   []string{string(ChannelHeartbeat)},
	}
	data, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}
	return sender.SendMessage(data)
}

type CoinbaseUserSubscription struct {
	ApiKey     string
	ApiSecret  string
	Passphrase string
}

func (s *CoinbaseUserSubscription) Subscribe(sender coinbaseMessageSender) error {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	message := timestamp + "GET" + "/users/self/verify"
	secretBytes, err := base64.StdEncoding.DecodeString(s.ApiSecret)
	if err != nil {
		return fmt.Errorf("failed to decode API secret: %w", err)
	}
	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(message))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	subscription := CoinbaseSubscription{
		Type:       "subscribe",
		ProductIDs: []string{},
		Channels:   []string{string(ChannelUser)},
		Key:        s.ApiKey,
		Passphrase: s.Passphrase,
		Timestamp:  timestamp,
		Signature:  signature,
	}
	data, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}
	return sender.SendMessage(data)
}

// NewCoinbaseWebSocketClient creates a WebSocket client
func NewCoinbaseWebSocketClient(wsURL, apiKey, apiSecret, passphrase string) *CoinbaseWebSocketClient {
	client := &CoinbaseWebSocketClient{
		apiKey:        apiKey,
		apiSecret:     apiSecret,
		passphrase:    passphrase,
		callbacks:     make(map[string]interface{}),
		subscriptions: make(map[string]bool),
		wsURL:         wsURL,
	}
	client.replaceClient(wsURL)
	return client
}

func (c *CoinbaseWebSocketClient) replaceClient(url string) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.ws != nil {
		c.ws.Close()
	}
	c.wsURL = url
	logger := defaultLogger()
	c.connected = false
	c.ws = gowscl.NewClient(
		url,
		gowscl.WithLogger(logger),
		gowscl.WithOnMessage(func(data []byte, typ gowscl.MessageType) {
			if err := c.HandleMessage(data); err != nil {
				logger.Warnf("[%s] websocket message handling failed: %v", coinbaseWSComponent, err)
			}
		}),
		gowscl.WithOnOpen(func() {
			c.setConnected(true)
			if err := c.restoreSubscriptions(); err != nil {
				logger.Warnf("[%s] failed to restore subscriptions: %v", coinbaseWSComponent, err)
			}
		}),
		gowscl.WithOnClose(func() {
			c.setConnected(false)
		}),
		gowscl.WithOnError(func(err error) {
			logger.Warnf("[%s] websocket error: %v", coinbaseWSComponent, err)
		}),
	)
}

func (c *CoinbaseWebSocketClient) setConnected(state bool) {
	c.connMu.Lock()
	c.connected = state
	c.connMu.Unlock()
}

// Connect establishes the websocket connection if needed.
func (c *CoinbaseWebSocketClient) Connect() error {
	c.connMu.RLock()
	ws := c.ws
	c.connMu.RUnlock()
	if ws == nil {
		c.replaceClient(c.wsURL)
		c.connMu.RLock()
		ws = c.ws
		c.connMu.RUnlock()
	}
	return ws.Connect()
}

// IsConnected reports whether the underlying websocket is connected.
func (c *CoinbaseWebSocketClient) IsConnected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connected
}

// SendMessage sends a raw JSON payload to Coinbase.
func (c *CoinbaseWebSocketClient) SendMessage(message []byte) error {
	c.connMu.RLock()
	ws := c.ws
	c.connMu.RUnlock()
	if ws == nil {
		return fmt.Errorf("websocket client not initialized")
	}
	return ws.Send(message, coinbaseWSMessageText)
}

// HandleMessage processes WebSocket messages
func (c *CoinbaseWebSocketClient) HandleMessage(message []byte) error {
	var baseMessage struct {
		Type      string `json:"type"`
		ProductID string `json:"product_id"`
	}
	if err := json.Unmarshal(message, &baseMessage); err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	symbol := baseMessage.ProductID
	if symbol != "" {
		symbol = strings.Replace(baseMessage.ProductID, "-", "/", 1)
		if strings.HasSuffix(symbol, "USD") {
			symbol = strings.TrimSuffix(symbol, "USD") + "USDT"
		}
	}

	switch baseMessage.Type {
	case "ticker":
		var ticker CoinbaseTicker
		if err := json.Unmarshal(message, &ticker); err != nil {
			return err
		}
		c.mu.RLock()
		key := fmt.Sprintf("%s:%s", ChannelTicker, ticker.ProductID)
		if callback, ok := c.callbacks[key].(func(models.Ticker)); ok {
			price, _ := strconv.ParseFloat(ticker.Price, 64)
			volume, _ := strconv.ParseFloat(ticker.Volume24h, 64)
			bid, _ := strconv.ParseFloat(ticker.BestBid, 64)
			ask, _ := strconv.ParseFloat(ticker.BestAsk, 64)
			timestamp, _ := time.Parse(time.RFC3339, ticker.Time)
			if timestamp.IsZero() {
				timestamp = time.Now()
			}
			callback(models.Ticker{
				Exchange:  "Coinbase",
				Symbol:    symbol,
				LastPrice: price,
				Volume:    volume,
				Bid:       bid,
				Ask:       ask,
				Timestamp: timestamp,
			})
		}
		c.mu.RUnlock()
	case "l2update":
		var l2update CoinbaseL2Update
		if err := json.Unmarshal(message, &l2update); err != nil {
			return err
		}
		c.mu.RLock()
		key := fmt.Sprintf("%s:%s", ChannelLevel2, l2update.ProductID)
		if callback, ok := c.callbacks[key].(func(CoinbaseL2Update)); ok {
			callback(l2update)
		}
		c.mu.RUnlock()
	case "match", "last_match":
		var match CoinbaseMatch
		if err := json.Unmarshal(message, &match); err != nil {
			return err
		}
		c.mu.RLock()
		key := fmt.Sprintf("%s:%s", ChannelMatches, match.ProductID)
		if callback, ok := c.callbacks[key].(func(models.Trade)); ok {
			price, _ := strconv.ParseFloat(match.Price, 64)
			quantity, _ := strconv.ParseFloat(match.Size, 64)
			timestamp, _ := time.Parse(time.RFC3339, match.Time)
			if timestamp.IsZero() {
				timestamp = time.Now()
			}
			callback(models.Trade{
				Exchange:      "Coinbase",
				Symbol:        symbol,
				ID:            strconv.FormatInt(match.TradeID, 10),
				Type:          match.Side,
				Price:         price,
				Quantity:      quantity,
				ExecutionTime: timestamp,
			})
		}
		c.mu.RUnlock()
	case "heartbeat":
		var heartbeat CoinbaseHeartbeat
		if err := json.Unmarshal(message, &heartbeat); err != nil {
			return err
		}
		c.mu.RLock()
		key := fmt.Sprintf("%s:%s", ChannelHeartbeat, heartbeat.ProductID)
		if callback, ok := c.callbacks[key].(func(CoinbaseHeartbeat)); ok {
			callback(heartbeat)
		}
		c.mu.RUnlock()
	case "received", "open", "done", "change", "activate":
		c.mu.RLock()
		if callback, ok := c.callbacks[string(ChannelUser)].(func(models.UserData)); ok {
			var data map[string]interface{}
			if err := json.Unmarshal(message, &data); err == nil {
				callback(models.UserData{
					Exchange:  "Coinbase",
					EventType: baseMessage.Type,
					Data:      data,
				})
			}
		}
		c.mu.RUnlock()
	case "subscriptions":
	case "error":
		var errorMsg struct {
			Message string `json:"message"`
			Reason  string `json:"reason"`
		}
		if err := json.Unmarshal(message, &errorMsg); err != nil {
			return fmt.Errorf("failed to parse error message: %w", err)
		}
		return fmt.Errorf("WebSocket error: %s (%s)", errorMsg.Message, errorMsg.Reason)
	default:
		return fmt.Errorf("unknown message type: %s", baseMessage.Type)
	}
	return nil
}

// SubscribeToTicker subscribes to ticker updates
func (c *CoinbaseWebSocketClient) SubscribeToTicker(productID string, callback func(models.Ticker)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	key := fmt.Sprintf("%s:%s", ChannelTicker, productID)
	c.mu.Lock()
	c.callbacks[key] = callback
	c.subscriptions[key] = true
	c.mu.Unlock()

	sub := &CoinbaseTickerSubscription{ProductID: productID}
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	return nil
}

// SubscribeToLevel2 subscribes to level2 updates
func (c *CoinbaseWebSocketClient) SubscribeToLevel2(productID string, callback func(CoinbaseL2Update)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	key := fmt.Sprintf("%s:%s", ChannelLevel2, productID)
	c.mu.Lock()
	c.callbacks[key] = callback
	c.subscriptions[key] = true
	c.mu.Unlock()

	sub := &CoinbaseLevel2Subscription{ProductID: productID}
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	return nil
}

// SubscribeToMatches subscribes to trade updates
func (c *CoinbaseWebSocketClient) SubscribeToMatches(productID string, callback func(models.Trade)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	key := fmt.Sprintf("%s:%s", ChannelMatches, productID)
	c.mu.Lock()
	c.callbacks[key] = callback
	c.subscriptions[key] = true
	c.mu.Unlock()

	sub := &CoinbaseMatchesSubscription{ProductID: productID}
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	return nil
}

// SubscribeToHeartbeat subscribes to heartbeat updates
func (c *CoinbaseWebSocketClient) SubscribeToHeartbeat(productID string, callback func(CoinbaseHeartbeat)) error {
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	key := fmt.Sprintf("%s:%s", ChannelHeartbeat, productID)
	c.mu.Lock()
	c.callbacks[key] = callback
	c.subscriptions[key] = true
	c.mu.Unlock()

	sub := &CoinbaseHeartbeatSubscription{ProductID: productID}
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	return nil
}

// SubscribeToUserChannel subscribes to user data updates
func (c *CoinbaseWebSocketClient) SubscribeToUserChannel(callback func(models.UserData)) error {
	if c.apiKey == "" || c.apiSecret == "" || c.passphrase == "" {
		return fmt.Errorf("API credentials required for user channel")
	}
	if !c.IsConnected() {
		if err := c.Connect(); err != nil {
			return err
		}
	}
	key := string(ChannelUser)
	c.mu.Lock()
	c.callbacks[key] = callback
	c.subscriptions[key] = true
	c.mu.Unlock()

	sub := &CoinbaseUserSubscription{
		ApiKey:     c.apiKey,
		ApiSecret:  c.apiSecret,
		Passphrase: c.passphrase,
	}
	if err := sub.Subscribe(c); err != nil {
		return err
	}
	return nil
}

// Unsubscribe removes subscriptions for specified channels and products
func (c *CoinbaseWebSocketClient) Unsubscribe(channels []CoinbaseChannelType, productIDs []string) error {
	if !c.IsConnected() {
		return fmt.Errorf("WebSocket not connected")
	}

	stringChannels := make([]string, len(channels))
	for i, ch := range channels {
		stringChannels[i] = string(ch)
	}

	subscription := CoinbaseSubscription{
		Type:       "unsubscribe",
		ProductIDs: productIDs,
		Channels:   stringChannels,
	}
	data, err := json.Marshal(subscription)
	if err != nil {
		return fmt.Errorf("failed to marshal unsubscribe request: %w", err)
	}

	if err := c.SendMessage(data); err != nil {
		return fmt.Errorf("failed to send unsubscribe message: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, channel := range channels {
		for _, productID := range productIDs {
			key := fmt.Sprintf("%s:%s", channel, productID)
			delete(c.callbacks, key)
			delete(c.subscriptions, key)
		}
		if channel == ChannelUser {
			delete(c.callbacks, string(ChannelUser))
			delete(c.subscriptions, string(ChannelUser))
		}
	}

	return nil
}

// restoreSubscriptions restores subscriptions after reconnection
func (c *CoinbaseWebSocketClient) restoreSubscriptions() error {
	c.mu.RLock()
	keys := make([]string, 0, len(c.subscriptions))
	for key := range c.subscriptions {
		keys = append(keys, key)
	}
	c.mu.RUnlock()

	for _, key := range keys {
		var sub coinbaseSubscription
		if key == string(ChannelUser) {
			sub = &CoinbaseUserSubscription{
				ApiKey:     c.apiKey,
				ApiSecret:  c.apiSecret,
				Passphrase: c.passphrase,
			}
		} else {
			parts := strings.Split(key, ":")
			if len(parts) != 2 {
				continue
			}
			channel, productID := parts[0], parts[1]
			switch CoinbaseChannelType(channel) {
			case ChannelTicker:
				sub = &CoinbaseTickerSubscription{ProductID: productID}
			case ChannelLevel2:
				sub = &CoinbaseLevel2Subscription{ProductID: productID}
			case ChannelMatches:
				sub = &CoinbaseMatchesSubscription{ProductID: productID}
			case ChannelHeartbeat:
				sub = &CoinbaseHeartbeatSubscription{ProductID: productID}
			}
		}
		if sub != nil {
			if err := sub.Subscribe(c); err != nil {
				return fmt.Errorf("failed to resubscribe to %s: %w", key, err)
			}
		}
	}
	return nil
}

// GetWebSocketClient returns the WebSocket client
func (c *CoinbaseClient) GetWebSocketClient() *CoinbaseWebSocketClient {
	return c.wsClient
}

// StopWebSocketOrderBook stops WebSocket order book updates
func (c *CoinbaseClient) StopWebSocketOrderBook(symbol string) error {
	coinbaseSymbol := convertToCoinbaseSymbol(symbol)
	err := c.wsClient.Unsubscribe([]CoinbaseChannelType{ChannelLevel2}, []string{coinbaseSymbol})
	if err != nil {
		return fmt.Errorf("failed to unsubscribe from level2 updates: %w", err)
	}

	c.orderBookMux.Lock()
	delete(c.orderBooks, symbol)
	c.orderBookMux.Unlock()
	return nil
}
