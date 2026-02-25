package exchange

import (
	"context"
	"os"
	"testing"
	"time"

	common "github.com/evdnx/goexchange/common"
)

// TestIndependentReserveClient tests the Independent Reserve client.
// Set IR_API_KEY and IR_API_SECRET environment variables to run authenticated tests.
func TestIndependentReserveClient_NewClient(t *testing.T) {
	client := NewIndependentReserveClient("test-key", "test-secret", false, nil)

	if client == nil {
		t.Fatal("expected non-nil client")
	}

	if client.GetName() != "IndependentReserve" {
		t.Errorf("expected name IndependentReserve, got %s", client.GetName())
	}

	if client.baseURL != irBaseURL {
		t.Errorf("expected baseURL %s, got %s", irBaseURL, client.baseURL)
	}
}

func TestIndependentReserveClient_GetValidPrimaryCurrencyCodes_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	codes, err := client.GetValidPrimaryCurrencyCodes(context.Background())
	if err != nil {
		t.Fatalf("failed to get primary currency codes: %v", err)
	}

	if len(codes) == 0 {
		t.Fatal("expected non-empty list of primary currency codes")
	}

	// Check for common currencies
	foundXbt := false
	foundEth := false
	for _, code := range codes {
		if code == "Xbt" {
			foundXbt = true
		}
		if code == "Eth" {
			foundEth = true
		}
	}

	if !foundXbt {
		t.Error("expected to find Xbt in primary currency codes")
	}
	if !foundEth {
		t.Error("expected to find Eth in primary currency codes")
	}

	t.Logf("Found %d primary currency codes", len(codes))
}

func TestIndependentReserveClient_GetValidSecondaryCurrencyCodes_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	codes, err := client.GetValidSecondaryCurrencyCodes(context.Background())
	if err != nil {
		t.Fatalf("failed to get secondary currency codes: %v", err)
	}

	if len(codes) == 0 {
		t.Fatal("expected non-empty list of secondary currency codes")
	}

	// Check for common currencies
	foundAud := false
	foundUsd := false
	for _, code := range codes {
		if code == "Aud" {
			foundAud = true
		}
		if code == "Usd" {
			foundUsd = true
		}
	}

	if !foundAud {
		t.Error("expected to find Aud in secondary currency codes")
	}
	if !foundUsd {
		t.Error("expected to find Usd in secondary currency codes")
	}

	t.Logf("Found %d secondary currency codes: %v", len(codes), codes)
}

func TestIndependentReserveClient_GetTradingPairs_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	pairs, err := client.GetTradingPairs()
	if err != nil {
		t.Fatalf("failed to get trading pairs: %v", err)
	}

	if len(pairs) == 0 {
		t.Fatal("expected non-empty list of trading pairs")
	}

	// Check for BTC/AUD pair
	foundBtcAud := false
	for _, pair := range pairs {
		if pair.Symbol == "BTC/AUD" {
			foundBtcAud = true
			if pair.BaseAsset != "BTC" {
				t.Errorf("expected base asset BTC for BTC/AUD, got %s", pair.BaseAsset)
			}
			if pair.QuoteAsset != "AUD" {
				t.Errorf("expected quote asset AUD for BTC/AUD, got %s", pair.QuoteAsset)
			}
			break
		}
	}

	if !foundBtcAud {
		t.Error("expected to find BTC/AUD trading pair")
	}

	t.Logf("Found %d trading pairs", len(pairs))
}

func TestIndependentReserveClient_GetTicker_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	ticker, err := client.GetTicker("BTC/AUD")
	if err != nil {
		t.Fatalf("failed to get ticker: %v", err)
	}

	if ticker == nil {
		t.Fatal("expected non-nil ticker")
	}

	if ticker.Exchange != "IndependentReserve" {
		t.Errorf("expected exchange IndependentReserve, got %s", ticker.Exchange)
	}

	if ticker.Symbol != "BTC/AUD" {
		t.Errorf("expected symbol BTC/AUD, got %s", ticker.Symbol)
	}

	if ticker.LastPrice <= 0 {
		t.Error("expected positive last price")
	}

	if ticker.Bid <= 0 {
		t.Error("expected positive bid price")
	}

	if ticker.Ask <= 0 {
		t.Error("expected positive ask price")
	}

	t.Logf("BTC/AUD ticker: Last=%.2f, Bid=%.2f, Ask=%.2f, Volume=%.8f",
		ticker.LastPrice, ticker.Bid, ticker.Ask, ticker.Volume)
}

func TestIndependentReserveClient_GetOrderBook_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	orderBook, err := client.GetOrderBook("BTC/AUD", 10)
	if err != nil {
		t.Fatalf("failed to get order book: %v", err)
	}

	if orderBook == nil {
		t.Fatal("expected non-nil order book")
	}

	if orderBook.Exchange != "IndependentReserve" {
		t.Errorf("expected exchange IndependentReserve, got %s", orderBook.Exchange)
	}

	if len(orderBook.Bids) == 0 {
		t.Error("expected non-empty bids")
	}

	if len(orderBook.Asks) == 0 {
		t.Error("expected non-empty asks")
	}

	// Verify bids are sorted (highest first)
	for i := range min(len(orderBook.Bids)-1, 9) {
		if orderBook.Bids[i].Price < orderBook.Bids[i+1].Price {
			t.Errorf("bids not sorted correctly at index %d", i)
		}
	}

	// Verify asks are sorted (lowest first)
	for i := range min(len(orderBook.Asks)-1, 9) {
		if orderBook.Asks[i].Price > orderBook.Asks[i+1].Price {
			t.Errorf("asks not sorted correctly at index %d", i)
		}
	}

	t.Logf("Order book: %d bids, %d asks, best bid=%.2f, best ask=%.2f",
		len(orderBook.Bids), len(orderBook.Asks),
		orderBook.Bids[0].Price, orderBook.Asks[0].Price)
}

func TestIndependentReserveClient_GetTrades_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	trades, err := client.GetTrades("BTC/AUD", time.Time{}, 10)
	if err != nil {
		t.Fatalf("failed to get trades: %v", err)
	}

	if len(trades) == 0 {
		t.Skip("no recent trades found (market may be quiet)")
	}

	// Verify trade structure
	for _, trade := range trades {
		if trade.Exchange != "IndependentReserve" {
			t.Errorf("expected exchange IndependentReserve, got %s", trade.Exchange)
		}
		if trade.Symbol != "BTC/AUD" {
			t.Errorf("expected symbol BTC/AUD, got %s", trade.Symbol)
		}
		if trade.Price <= 0 {
			t.Error("expected positive price")
		}
		if trade.Quantity <= 0 {
			t.Error("expected positive quantity")
		}
	}

	t.Logf("Found %d recent trades", len(trades))
}

func TestIndependentReserveClient_GetCandles_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	client := NewIndependentReserveClient("", "", false, nil)

	// Get last 24 hours of hourly candles
	since := time.Now().Add(-24 * time.Hour)
	candles, err := client.GetCandles("BTC/AUD", "1h", since, 24)
	if err != nil {
		t.Fatalf("failed to get candles: %v", err)
	}

	// Note: Some hours may have no trades and thus no candles
	t.Logf("Found %d candles in last 24 hours", len(candles))

	// Verify candle structure for any found
	for _, candle := range candles {
		if candle.Exchange != "IndependentReserve" {
			t.Errorf("expected exchange IndependentReserve, got %s", candle.Exchange)
		}
		if candle.Interval != "1h" {
			t.Errorf("expected interval 1h, got %s", candle.Interval)
		}
		if candle.High < candle.Low {
			t.Error("high should be >= low")
		}
		if candle.Open <= 0 || candle.Close <= 0 {
			t.Error("expected positive open and close prices")
		}
	}
}

// TestIndependentReserveClient_GetBalances_Live tests authenticated balance retrieval.
// Requires IR_API_KEY and IR_API_SECRET environment variables.
func TestIndependentReserveClient_GetBalances_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	apiKey := os.Getenv("IR_API_KEY")
	apiSecret := os.Getenv("IR_API_SECRET")

	if apiKey == "" || apiSecret == "" {
		t.Skip("IR_API_KEY and IR_API_SECRET not set")
	}

	client := NewIndependentReserveClient(apiKey, apiSecret, false, nil)

	balances, err := client.GetBalances()
	if err != nil {
		t.Fatalf("failed to get balances: %v", err)
	}

	if len(balances) == 0 {
		t.Error("expected non-empty balances")
	}

	// Log balances
	for currency, balance := range balances {
		t.Logf("%s: Free=%s, Locked=%s", currency, balance.Free, balance.Locked)
	}
}

// TestIndependentReserveClient_CreateOrder_Live tests order creation.
// Requires IR_API_KEY and IR_API_SECRET environment variables.
// WARNING: This creates real orders on the live exchange!
func TestIndependentReserveClient_CreateOrder_Live(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Independent Reserve call in short mode")
	}

	apiKey := os.Getenv("IR_API_KEY")
	apiSecret := os.Getenv("IR_API_SECRET")

	if apiKey == "" || apiSecret == "" {
		t.Skip("IR_API_KEY and IR_API_SECRET not set")
	}

	// Skip by default - uncomment to test actual order placement
	t.Skip("Skipping order creation test - uncomment to test live orders")

	client := NewIndependentReserveClient(apiKey, apiSecret, false, nil)

	// Place a very small limit buy order at a price unlikely to fill
	order, err := client.CreateOrder(
		"BTC/AUD",
		common.OrderSideBuy,
		common.OrderTypeLimit,
		0.0001,  // very small amount
		10000.0, // very low price that won't fill
	)

	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	t.Logf("Created order: %s", order.ID)

	// Cancel the order immediately
	err = client.CancelOrder("BTC/AUD", order.ID)
	if err != nil {
		t.Fatalf("failed to cancel order: %v", err)
	}

	t.Log("Order cancelled successfully")
}

// TestParseIRSymbol tests symbol parsing.
func TestParseIRSymbol(t *testing.T) {
	tests := []struct {
		symbol            string
		expectedPrimary   string
		expectedSecondary string
	}{
		{"BTC/AUD", "Xbt", "AUD"},
		{"ETH/USD", "ETH", "USD"},
		{"XRP/NZD", "XRP", "NZD"},
		{"SOL/SGD", "SOL", "SGD"},
	}

	for _, tt := range tests {
		primary, secondary := parseIRSymbol(tt.symbol)
		if primary != tt.expectedPrimary {
			t.Errorf("parseIRSymbol(%s): expected primary %s, got %s",
				tt.symbol, tt.expectedPrimary, primary)
		}
		if secondary != tt.expectedSecondary {
			t.Errorf("parseIRSymbol(%s): expected secondary %s, got %s",
				tt.symbol, tt.expectedSecondary, secondary)
		}
	}
}

// TestConvertToIRSymbol tests symbol conversion.
func TestConvertToIRSymbol(t *testing.T) {
	tests := []struct {
		primary   string
		secondary string
		expected  string
	}{
		{"Xbt", "Aud", "BTC/AUD"},
		{"Eth", "Usd", "ETH/USD"},
		{"Xrp", "Nzd", "XRP/NZD"},
	}

	for _, tt := range tests {
		result := convertToIRSymbol(tt.primary, tt.secondary)
		if result != tt.expected {
			t.Errorf("convertToIRSymbol(%s, %s): expected %s, got %s",
				tt.primary, tt.secondary, tt.expected, result)
		}
	}
}

// TestConvertIROrderStatus tests order status conversion.
func TestConvertIROrderStatus(t *testing.T) {
	tests := []struct {
		input    string
		expected common.OrderStatus
	}{
		{"Open", common.OrderStatusNew},
		{"PartiallyFilled", common.OrderStatusPartiallyFilled},
		{"Filled", common.OrderStatusFilled},
		{"Cancelled", common.OrderStatusCancelled},
		{"Canceled", common.OrderStatusCancelled},
		{"Rejected", common.OrderStatusRejected},
		{"Expired", common.OrderStatusExpired},
	}

	for _, tt := range tests {
		result := convertIROrderStatus(tt.input)
		if result != tt.expected {
			t.Errorf("convertIROrderStatus(%s): expected %s, got %s",
				tt.input, tt.expected, result)
		}
	}
}

// TestSignatureGeneration tests the signature generation logic.
func TestSignatureGeneration(t *testing.T) {
	client := NewIndependentReserveClient("api_key", "api_secret", false, nil)

	// Test signature generation with known values
	url := "https://api.independentreserve.com/Private/GetOpenOrders"
	nonce := int64(1234567890)
	params := []string{
		"primaryCurrencyCode=Xbt",
		"secondaryCurrencyCode=Usd",
		"pageIndex=1",
		"pageSize=10",
	}

	signature := client.createSignature(url, nonce, params)

	// Signature should be non-empty and uppercase hex
	if signature == "" {
		t.Error("expected non-empty signature")
	}

	if len(signature) != 64 { // SHA256 produces 32 bytes = 64 hex chars
		t.Errorf("expected 64 character signature, got %d characters", len(signature))
	}

	// Should be uppercase
	for _, c := range signature {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) {
			t.Errorf("signature contains invalid character: %c", c)
			break
		}
	}

	t.Logf("Generated signature: %s", signature)
}
