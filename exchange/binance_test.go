package exchange

import (
	"strings"
	"testing"
	"time"

	common "github.com/evdnx/goexchange/common"
)

// TestBinanceCreateOrder_MarketBuy_DUSDT tests placing a market buy order for D/USDT
// worth of 25 USDT in the demo account.
//
// IMPORTANT: This test requires DEMO API keys, which are different from production keys.
// To set up demo API keys:
//  1. Visit https://demo.binance.com/en/my/settings/api-management
//  2. Create API keys in the demo API Management section
//  3. Enable "TRADE" permission in API Management (trading is disabled by default)
//  4. Set BINANCE_API_KEY and BINANCE_API_SECRET environment variables
//
// Note: Demo environment uses https://demo-api.binance.com/api endpoint (no GitHub login required)
func TestBinanceCreateOrder_MarketBuy_DUSDT(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Binance call in short mode")
	}

	// NewBinanceClient will automatically read from environment variables if empty strings are passed
	// (BINANCE_API_KEY and BINANCE_API_SECRET from .env file or environment)
	// The third parameter (true) enables testnet/demo mode, which uses https://demo-api.binance.com/api
	client := NewBinanceClient("", "", true, nil)

	// Verify credentials were loaded - fail if not set (don't skip, as this indicates a configuration issue)
	apiKey := client.APIKey()
	apiSecret := client.APISecret()
	if apiKey == "" {
		t.Fatalf("BINANCE_API_KEY is not set - check .env file or environment variables")
	}
	if apiSecret == "" {
		t.Fatalf("BINANCE_API_SECRET is not set - check .env file or environment variables")
	}

	// Log first few characters of API key for verification (truncated for security)
	keyPreview := apiKey
	if len(apiKey) > 8 {
		keyPreview = apiKey[:8]
	}
	t.Logf("Using API key: %s... (truncated for security)", keyPreview)
	t.Logf("Using demo endpoint: https://demo-api.binance.com/api")
	t.Logf("NOTE: This test requires DEMO API keys created at https://demo.binance.com/en/my/settings/api-management")
	t.Logf("      Demo keys are separate from production keys (no GitHub login required).")
	t.Logf("      Trading permission must be explicitly enabled in demo API Management.")

	// Place a market buy order for D/USDT worth of 25 USDT in demo account
	// This places an actual order in the demo environment
	order, err := client.CreateOrderAdvanced(
		"D/USDT",               // symbol
		common.OrderSideBuy,    // side: BUY
		common.OrderTypeMarket, // type: MARKET
		0,                      // amount: ignored when quoteOrderQty is used
		0,                      // price: not needed for market orders
		25.0,                   // quoteOrderQty: buy $25 worth of D
		"",                     // clientOrderID: optional
		false,                  // test: false to use /order endpoint (places actual order)
		5000,                   // recvWindow: 5000ms
	)

	if err != nil {
		// Provide helpful error message for common authentication issues
		// Based on: https://developers.binance.com/docs/binance-spot-api-docs/testnet/rest-api/request-security
		errMsg := err.Error()
		if strings.Contains(errMsg, "Invalid API-key") || strings.Contains(errMsg, "authentication_failed") {
			t.Fatalf("Authentication failed - possible causes:\n"+
				"  1. Using production API keys with demo (demo keys must be created separately)\n"+
				"  2. API key doesn't have 'TRADE' permission enabled (trading is disabled by default)\n"+
				"  3. Demo API keys not created at https://demo.binance.com/en/my/settings/api-management\n"+
				"  4. IP whitelisting is enabled and your IP is not whitelisted\n"+
				"  5. Incorrect API key or secret\n"+
				"\n"+
				"  Setup instructions:\n"+
				"  - Visit https://demo.binance.com/en/my/settings/api-management\n"+
				"  - Create API keys in the demo API Management section\n"+
				"  - Enable 'TRADE' permission in API Management\n"+
				"  - Set BINANCE_API_KEY and BINANCE_API_SECRET environment variables\n"+
				"\n"+
				"  Error: %v", err)
		}
		t.Fatalf("failed to place market buy order: %v", err)
	}

	if order == nil {
		t.Fatal("expected non-nil order response")
	}

	// Validate order response
	if order.Symbol != "D/USDT" {
		t.Errorf("expected symbol D/USDT, got %s", order.Symbol)
	}

	if order.Side != common.OrderSideBuy {
		t.Errorf("expected side BUY, got %s", order.Side)
	}

	if order.Type != common.OrderTypeMarket {
		t.Errorf("expected type MARKET, got %s", order.Type)
	}

	// Order was placed successfully in demo account
	t.Logf("Order placed successfully:")
	t.Logf("  Symbol: %s", order.Symbol)
	t.Logf("  Side: %s", order.Side)
	t.Logf("  Type: %s", order.Type)
	t.Logf("  Status: %s", order.Status)
	if order.ID != "" {
		t.Logf("  Order ID: %s", order.ID)
	}
	if order.Amount > 0 {
		t.Logf("  Amount: %f", order.Amount)
	}
	if order.Price > 0 {
		t.Logf("  Price: %f", order.Price)
	}
}

// TestBinanceFindScalpingCoins tests getting scalping coins from Binance
func TestBinanceFindScalpingCoins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Binance call in short mode")
	}

	// Create Binance client (no API keys needed for public endpoints)
	client := NewBinanceClient("", "", false, nil)

	// Test with legacy API (using default config values internally)
	coins, err := client.FindScalpingCoins(
		"USDT",               // quoteAsset
		20_000_000,           // minVolume: $20M USD (new default)
		5,                    // topN: top 5 coins for diversification
		100*time.Millisecond, // rateLimitDelay
		0.08,                 // maxSpread: 0.08% (new stricter default)
	)

	if err != nil {
		t.Fatalf("failed to find scalping coins: %v", err)
	}

	if len(coins) == 0 {
		t.Fatal("expected at least one scalping coin, got 0")
	}

	// Log summary with enhanced metrics
	t.Logf("Found %d scalping coins", len(coins))
	for i, coin := range coins {
		t.Logf("  %d. %s (%s) - Score: %.4f, ATR5Min: %.4f%%, Spread: %.4f%%, Profitability: %.2f, Directionality: %.4f, Depth: $%.2f",
			i+1, coin.Code, coin.Symbol, coin.Score, coin.ATR5Min, coin.Spread, coin.ProfitabilityRatio, coin.DirectionalityFactor, coin.OrderBookDepth)
	}

	// Validate results
	if coins[0].Symbol == "" {
		t.Error("expected non-empty symbol for first coin")
	}
	if coins[0].Volume <= 0 {
		t.Error("expected positive volume for first coin")
	}
	if coins[0].Score <= 0 {
		t.Error("expected positive score for first coin")
	}

	// Validate new metrics
	if coins[0].ATR5Min <= 0 {
		t.Error("expected positive ATR5Min for first coin")
	}
	if coins[0].ProfitabilityRatio < 3.0 {
		t.Errorf("expected profitability ratio >= 3.0, got %.2f", coins[0].ProfitabilityRatio)
	}
	if coins[0].DirectionalityFactor <= 0 {
		t.Error("expected positive directionality factor for first coin")
	}
	if coins[0].OrderBookDepth < 5000 {
		t.Errorf("expected order book depth >= $5000, got $%.2f", coins[0].OrderBookDepth)
	}
}

// TestBinanceFindScalpingCoinsWithConfig tests the new config-based API
func TestBinanceFindScalpingCoinsWithConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Binance call in short mode")
	}

	client := NewBinanceClient("", "", false, nil)

	// Use custom config with relaxed thresholds for testing
	config := DefaultScalpingConfig()
	config.MinVolume = 10_000_000      // Lower volume threshold
	config.MaxSpread = 0.15            // More lenient spread
	config.MinProfitabilityRatio = 2.0 // Lower profitability threshold
	config.MinATR5Min = 0.2            // Lower ATR threshold
	config.MinOrderBookDepth = 2000    // Lower depth requirement
	config.TopN = 10

	coins, err := client.FindScalpingCoinsWithConfig(config)
	if err != nil {
		t.Fatalf("failed to find scalping coins with config: %v", err)
	}

	t.Logf("Found %d coins with relaxed config", len(coins))
	if len(coins) > 0 {
		t.Logf("Top coin: %s - Score: %.4f", coins[0].Code, coins[0].Score)
	}
}

