package exchange

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/evdnx/goexchange/models"
	"github.com/evdnx/gohttpcl"
	"github.com/evdnx/golog"
	metrics "github.com/evdnx/gotrademetrics"
)

const (
	swyftxHTTPTimeout        = 12 * time.Second
	swyftxAssetCacheTTL      = 30 * time.Minute
	swyftxTokenRefreshLeeway = 45 * time.Second
	swyftxUserAgent          = "GoExchangeClient/1.0"
	swyftxDefaultRateAmount  = 100.0
)

// SwyftxClient implements the ExchangeClient interface for the Swyftx exchange.
type SwyftxClient struct {
	*BaseClient
	baseURL     string
	httpClient  *gohttpcl.Client
	metrics     *metrics.Metrics
	logger      *golog.Logger
	userAgent   string
	httpTimeout time.Duration

	tokenMu     sync.RWMutex
	accessToken string
	tokenExpiry time.Time

	assetMu       sync.RWMutex
	assets        map[string]*swyftxAsset
	assetsByID    map[int]*swyftxAsset
	assetPairs    []TradingPair
	assetsFetched time.Time
}

// swyftxAsset describes an asset returned by the /markets/assets/ endpoint.
type swyftxAsset struct {
	ID         int     `json:"id"`
	Code       string  `json:"code"`
	Name       string  `json:"name"`
	PrimaryRaw boolish `json:"primary"`
	Secondary  boolish `json:"secondary"`
	PriceScale int     `json:"price_scale"`
	MinOrder   float64 `json:"minimum_order,string"`
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

// NewSwyftxClient creates a new Swyftx API client.
func NewSwyftxClient(apiKey, apiSecret string, testnet bool, metricsClient *metrics.Metrics) *SwyftxClient {
	baseURL := "https://api.swyftx.com.au"
	if testnet {
		baseURL = "https://api.demo.swyftx.com.au"
	}
	client := &SwyftxClient{
		BaseClient:  NewBaseClient("Swyftx", apiKey, apiSecret, testnet),
		baseURL:     strings.TrimRight(baseURL, "/"),
		metrics:     metricsClient,
		logger:      defaultLogger(),
		userAgent:   swyftxUserAgent,
		httpTimeout: swyftxHTTPTimeout,
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
	if collector := newHTTPMetricsCollector(metricsClient, "Swyftx"); collector != nil {
		opts = append(opts, gohttpcl.WithMetrics(collector))
	}
	return gohttpcl.New(opts...)
}

func (c *SwyftxClient) doRequest(ctx context.Context, method, path string, body interface{}, auth bool) ([]byte, error) {
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
	headers := map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   c.userAgent,
	}
	if auth {
		token, err := c.getAccessToken(ctx)
		if err != nil {
			return nil, err
		}
		headers["Authorization"] = "Bearer " + token
	}
	fullURL := c.baseURL + path
	opts := headerOptions(headers)
	var resp *http.Response
	var err error
	switch method {
	case http.MethodGet:
		resp, err = c.httpClient.Get(ctx, fullURL, c.httpTimeout, nil, opts...)
	case http.MethodPost:
		resp, err = c.httpClient.Post(ctx, fullURL, bytes.NewReader(bodyBytes), c.httpTimeout, nil, opts...)
	case http.MethodPut:
		resp, err = c.httpClient.Put(ctx, fullURL, bytes.NewReader(bodyBytes), c.httpTimeout, nil, opts...)
	case http.MethodDelete:
		resp, err = c.httpClient.Delete(ctx, fullURL, c.httpTimeout, nil, opts...)
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
		return nil, NewExchangeHTTPError(resp.StatusCode, data, string(data))
	}
	return data, nil
}

func (c *SwyftxClient) getAccessToken(ctx context.Context) (string, error) {
	c.tokenMu.RLock()
	token := c.accessToken
	expires := c.tokenExpiry
	c.tokenMu.RUnlock()
	if token != "" && time.Until(expires) > swyftxTokenRefreshLeeway {
		return token, nil
	}
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	if c.accessToken != "" && time.Until(c.tokenExpiry) > swyftxTokenRefreshLeeway {
		return c.accessToken, nil
	}
	if c.apiKey == "" {
		return "", NewAuthenticationError("swyftx api key required")
	}
	body := map[string]string{"apiKey": c.apiKey}
	data, err := c.doRequest(ctx, http.MethodPost, "/auth/refresh/", body, false)
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
	expiresAt := parseJWTExpiry(resp.AccessToken)
	c.accessToken = resp.AccessToken
	if expiresAt.IsZero() {
		c.tokenExpiry = time.Now().Add(10 * time.Minute)
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
	data, err := c.doRequest(ctx, http.MethodGet, "/markets/assets/", nil, false)
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
	c.assetPairs = c.buildTradingPairsLocked()
	return nil
}

func (c *SwyftxClient) buildTradingPairsLocked() []TradingPair {
	if len(c.assets) == 0 {
		return nil
	}
	var quotes []*swyftxAsset
	for _, asset := range c.assets {
		if asset.Primary() {
			quotes = append(quotes, asset)
		}
	}
	if len(quotes) == 0 {
		return nil
	}
	var pairs []TradingPair
	for _, quote := range quotes {
		for _, base := range c.assets {
			if !bool(base.Secondary) || base.ID == quote.ID {
				continue
			}
			symbol := fmt.Sprintf("%s/%s", base.Code, quote.Code)
			pairs = append(pairs, TradingPair{
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
func (c *SwyftxClient) GetTradingPairs() ([]TradingPair, error) {
	if err := c.ensureAssets(context.Background()); err != nil {
		return nil, err
	}
	c.assetMu.RLock()
	defer c.assetMu.RUnlock()
	pairs := make([]TradingPair, len(c.assetPairs))
	copy(pairs, c.assetPairs)
	return pairs, nil
}

// GetTicker returns ticker data for a symbol.
func (c *SwyftxClient) GetTicker(symbol string) (*models.Ticker, error) {
	ctx := context.Background()
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
		lastPrice = (buyPrice + sellPrice) / 2
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

type swyftxBasicInfo struct {
	Code      string  `json:"code"`
	ID        int     `json:"id"`
	Buy       string  `json:"buy"`
	Sell      string  `json:"sell"`
	Volume24H float64 `json:"volume24H"`
}

func (c *SwyftxClient) getBasicMarketInfo(ctx context.Context, code string) (*swyftxBasicInfo, error) {
	path := fmt.Sprintf("/markets/info/basic/%s/", strings.ToUpper(code))
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, false)
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
			Amount: base.MinOrder,
		}},
		Asks: []models.OrderBookEntry{{
			Price:  askPrice,
			Amount: base.MinOrder,
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

// GetCandles fetches OHLCV data using the /charts/getBars endpoint.
func (c *SwyftxClient) GetCandles(symbol, interval string, since time.Time, limit int) ([]models.Candle, error) {
	ctx := context.Background()
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
	query := url.Values{}
	query.Set("resolution", strconv.Itoa(resolution))
	query.Set("limit", strconv.Itoa(limit))
	if !since.IsZero() {
		query.Set("timeStart", strconv.FormatInt(since.Unix()*1000, 10))
	}
	path := fmt.Sprintf("/charts/getBars/%s/%s/BUY/?%s", strings.ToUpper(baseAsset.Code), strings.ToUpper(quoteAsset.Code), query.Encode())
	data, err := c.doRequest(ctx, http.MethodGet, path, nil, false)
	if err != nil {
		return nil, err
	}
	var resp []swyftxCandle
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	candles := make([]models.Candle, 0, len(resp))
	for _, candle := range resp {
		openTime := time.UnixMilli(candle.Time)
		candles = append(candles, models.Candle{
			Exchange:  c.GetName(),
			Symbol:    symbol,
			Interval:  interval,
			OpenTime:  openTime,
			CloseTime: openTime.Add(time.Duration(resolution) * time.Minute),
			Open:      parseStringFloat(candle.Open),
			High:      parseStringFloat(candle.High),
			Low:       parseStringFloat(candle.Low),
			Close:     parseStringFloat(candle.Close),
			Volume:    candle.Volume,
		})
	}
	return candles, nil
}

type swyftxCandle struct {
	Time   int64   `json:"time"`
	Open   string  `json:"open"`
	High   string  `json:"high"`
	Low    string  `json:"low"`
	Close  string  `json:"close"`
	Volume float64 `json:"volume"`
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
func (c *SwyftxClient) GetTrades(symbol string, since time.Time, limit int) ([]models.Trade, error) {
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
func (c *SwyftxClient) GetBalance(currency string) (*Balance, error) {
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
func (c *SwyftxClient) GetBalances() (map[string]*Balance, error) {
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
	balances := make(map[string]*Balance, len(resp))
	for _, entry := range resp {
		asset := c.assetsByID[entry.AssetID]
		if asset == nil {
			continue
		}
		balances[asset.Code] = &Balance{
			Asset:  asset.Code,
			Free:   entry.AvailableBalance,
			Locked: entry.LockedBalance,
		}
	}
	return balances, nil
}

// CreateOrder submits a new order using the /orders/ endpoint.
func (c *SwyftxClient) CreateOrder(symbol string, side OrderSide, orderType OrderType, amount, price float64) (*Order, error) {
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
	if side == OrderSideBuy {
		assetQuantity = quoteAsset.Code
	}
	body := map[string]string{
		"primary":       strings.ToUpper(quoteAsset.Code),
		"secondary":     strings.ToUpper(baseAsset.Code),
		"quantity":      strconv.FormatFloat(quantity, 'f', -1, 64),
		"assetQuantity": strings.ToUpper(assetQuantity),
		"orderType":     strconv.Itoa(orderTypeID),
	}
	if price > 0 {
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

func mapSwyftxOrderType(side OrderSide, orderType OrderType) (int, error) {
	switch orderType {
	case OrderTypeMarket:
		if side == OrderSideBuy {
			return 1, nil
		}
		return 2, nil
	case OrderTypeLimit:
		if side == OrderSideBuy {
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
	OrderUUID      string `json:"orderUuid"`
	OrderType      int    `json:"order_type"`
	PrimaryAsset   int    `json:"primary_asset"`
	SecondaryAsset int    `json:"secondary_asset"`
	QuantityAsset  int    `json:"quantity_asset"`
	Quantity       string `json:"quantity"`
	Trigger        string `json:"trigger"`
	Status         int    `json:"status"`
	Created        int64  `json:"created_time"`
	Updated        int64  `json:"updated_time"`
	Amount         string `json:"amount"`
	Total          string `json:"total"`
	Rate           string `json:"rate"`
	FeeAmount      string `json:"feeAmount"`
	FeeAsset       string `json:"feeAsset"`
}

func convertSwyftxOrder(symbol string, body *swyftxOrderBody) (*Order, error) {
	if body == nil {
		return nil, errors.New("swyftx: empty order body")
	}
	price := parseStringFloat(body.Rate)
	amount := parseStringFloat(body.Quantity)
	status := mapSwyftxOrderStatus(body.Status)
	return &Order{
		ID:              body.OrderUUID,
		Symbol:          symbol,
		Side:            mapSwyftxOrderSide(body.OrderType),
		Type:            mapSwyftxOrderTypeToGeneric(body.OrderType),
		Status:          status,
		Price:           price,
		Amount:          amount,
		FilledAmount:    parseStringFloat(body.Amount),
		RemainingAmount: amount - parseStringFloat(body.Amount),
		Fee:             parseStringFloat(body.FeeAmount),
		FeeCurrency:     body.FeeAsset,
		CreatedAt:       time.UnixMilli(body.Created),
		UpdatedAt:       time.UnixMilli(body.Updated),
		Quantity:        amount,
		Timestamp:       time.UnixMilli(body.Created),
		ClientOrderID:   body.OrderUUID,
	}, nil
}

func mapSwyftxOrderStatus(status int) OrderStatus {
	switch status {
	case 1, 5:
		return OrderStatusNew
	case 2, 6, 8:
		return OrderStatusCancelled
	case 3:
		return OrderStatusPartiallyFilled
	case 4:
		return OrderStatusFilled
	case 7, 9:
		return OrderStatusRejected
	default:
		return OrderStatusNew
	}
}

func mapSwyftxOrderSide(orderType int) OrderSide {
	switch orderType {
	case 1, 3, 5:
		return OrderSideBuy
	case 2, 4, 6:
		return OrderSideSell
	default:
		return OrderSideBuy
	}
}

func mapSwyftxOrderTypeToGeneric(orderType int) OrderType {
	switch orderType {
	case 1, 2:
		return OrderTypeMarket
	case 3, 4:
		return OrderTypeLimit
	case 5, 6:
		return OrderTypeStopLimit
	default:
		return OrderTypeMarket
	}
}

// GetOrder retrieves a specific order by UUID.
func (c *SwyftxClient) GetOrder(symbol, orderID string) (*Order, error) {
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

// GetOrders returns orders for a symbol.
func (c *SwyftxClient) GetOrders(symbol string, since time.Time, limit int) ([]Order, error) {
	ctx := context.Background()
	baseAsset, _, err := c.resolveSymbol(ctx, symbol)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
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
	orders := make([]Order, 0, len(resp))
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
		if order.Status == OrderStatusFilled || order.Status == OrderStatusCancelled {
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
