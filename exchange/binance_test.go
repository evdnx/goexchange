package exchange

import (
	"encoding/json"
	"fmt"
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
// and logs the full API response from exchangeInfo endpoint where tag data is located.
func TestBinanceFindScalpingCoins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Binance call in short mode")
	}

	// Create Binance client (no API keys needed for public endpoints)
	client := NewBinanceClient("", "", false, nil)

	// First, fetch and log the full exchangeInfo response to see tag data structure
	fmt.Printf("\n=== Fetching exchangeInfo to inspect tag data ===\n")
	endpoint := fmt.Sprintf("%s/exchangeInfo", client.apiPath("v3"))
	response, err := client.doGet(endpoint)
	if err != nil {
		t.Fatalf("failed to fetch exchange info: %v", err)
	}

	// Log the full raw JSON response (using fmt.Printf so it always shows)
	fmt.Printf("Full exchangeInfo API Response (raw JSON):\n")
	fmt.Printf("Response length: %d bytes\n\n", len(response))

	// Pretty print the JSON for readability
	var prettyJSON interface{}
	if err := json.Unmarshal(response, &prettyJSON); err == nil {
		prettyBytes, err := json.MarshalIndent(prettyJSON, "", "  ")
		if err == nil {
			// Log in chunks to avoid overwhelming output
			prettyStr := string(prettyBytes)
			chunkSize := 5000 // Log in 5000 char chunks
			for i := 0; i < len(prettyStr); i += chunkSize {
				end := i + chunkSize
				if end > len(prettyStr) {
					end = len(prettyStr)
				}
				fmt.Printf("Response chunk [%d:%d]:\n%s\n", i, end, prettyStr[i:end])
			}
		} else {
			// Fallback to raw response if pretty printing fails
			fmt.Printf("Raw response (first 10000 chars):\n%s\n", string(response[:min(len(response), 10000)]))
		}
	} else {
		// Fallback to raw response if unmarshaling fails
		fmt.Printf("Raw response (first 10000 chars):\n%s\n", string(response[:min(len(response), 10000)]))
	}

	// Parse the response to extract tag information
	var exchangeInfo struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Tags       []struct {
				Name string `json:"name"`
			} `json:"tags,omitempty"`
		} `json:"symbols"`
	}

	if err := json.Unmarshal(response, &exchangeInfo); err != nil {
		t.Fatalf("failed to parse exchange info: %v", err)
	}

	// Log statistics about tags
	fmt.Printf("\n=== Tag Data Analysis ===\n")
	tagCounts := make(map[string]int)
	symbolsWithTags := 0
	symbolsWithMonitoringTag := 0
	symbolsWithSeedTag := 0

	for _, symbol := range exchangeInfo.Symbols {
		if len(symbol.Tags) > 0 {
			symbolsWithTags++
			for _, tag := range symbol.Tags {
				tagCounts[tag.Name]++
				if strings.EqualFold(tag.Name, "Monitoring") {
					symbolsWithMonitoringTag++
				}
				if strings.EqualFold(tag.Name, "Seed") {
					symbolsWithSeedTag++
				}
			}
		}
	}

	fmt.Printf("Total symbols: %d\n", len(exchangeInfo.Symbols))
	fmt.Printf("Symbols with tags: %d\n", symbolsWithTags)
	fmt.Printf("Symbols with 'Monitoring' tag: %d\n", symbolsWithMonitoringTag)
	fmt.Printf("Symbols with 'Seed' tag: %d\n", symbolsWithSeedTag)
	fmt.Printf("\nTag distribution:\n")
	for tagName, count := range tagCounts {
		fmt.Printf("  %s: %d symbols\n", tagName, count)
	}

	// Show examples of symbols with tags
	fmt.Printf("\n=== Example symbols with tags ===\n")
	examplesShown := 0
	for _, symbol := range exchangeInfo.Symbols {
		if len(symbol.Tags) > 0 && examplesShown < 10 {
			tagNames := make([]string, len(symbol.Tags))
			for i, tag := range symbol.Tags {
				tagNames[i] = tag.Name
			}
			fmt.Printf("  %s (%s/%s): tags=%v\n", symbol.Symbol, symbol.BaseAsset, symbol.QuoteAsset, tagNames)
			examplesShown++
		}
	}

	// Now test FindScalpingCoins
	fmt.Printf("\n=== Testing FindScalpingCoins ===\n")
	coins, err := client.FindScalpingCoins(
		"USDT",               // quoteAsset
		1000000,              // minVolume: 1M USDT
		10,                   // topN: top 10 coins
		100*time.Millisecond, // rateLimitDelay
		0.5,                  // maxSpread: 0.5%
	)

	if err != nil {
		t.Fatalf("failed to find scalping coins: %v", err)
	}

	if len(coins) == 0 {
		t.Fatal("expected at least one scalping coin, got 0")
	}

	fmt.Printf("\n=== Top Scalping Coins (found %d) ===\n", len(coins))
	for i, coin := range coins {
		fmt.Printf("%d. %s (%s)\n", i+1, coin.Code, coin.Symbol)
		fmt.Printf("   Volume: %.2f USDT\n", coin.Volume)
		fmt.Printf("   Volatility: %.4f%%\n", coin.Volatility)
		fmt.Printf("   Spread: %.4f%%\n", coin.Spread)
		fmt.Printf("   Score: %.4f\n", coin.Score)
		fmt.Printf("\n")
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
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
