package exchange

import (
	"fmt"
	"testing"
	"time"
)

// Integration-style check that public market data endpoints respond.
func TestSwyftxGetTicker_TRX_AUD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Swyftx call in short mode")
	}

	client := NewSwyftxClient("", "", true, nil)

	ticker, err := client.GetTicker("TRX/AUD")
	if err != nil {
		t.Fatalf("failed to fetch TRX/AUD ticker: %v", err)
	}
	if ticker == nil {
		t.Fatal("nil ticker returned")
	}
	if ticker.LastPrice <= 0 {
		t.Fatalf("expected last price > 0, got %f", ticker.LastPrice)
	}
}

func TestSwyftxFetchMarketData_TRX_AUD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Swyftx call in short mode")
	}

	client := NewSwyftxClient("", "", true, nil)

	md, err := client.FetchMarketData("TRX/AUD")
	if err != nil {
		t.Fatalf("failed to fetch TRX/AUD market data: %v", err)
	}
	if md.Close <= 0 {
		t.Fatalf("expected close > 0, got %f", md.Close)
	}
	if md.Price == nil || *md.Price <= 0 {
		t.Fatalf("expected price pointer > 0, got %#v", md.Price)
	}
}

func TestSwyftxGetCandles_TRX_AUD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Swyftx call in short mode")
	}

	client := NewSwyftxClient("", "", true, nil)

	// Fetch 1 hour of 1-minute candles
	// The fully built URL would look like:
	// https://api.swyftx.com.au/getbars/?baseAsset=TRX&secondaryAsset=AUD&resolution=1&side=bid&limit=60&timeStart=<timestamp_in_ms>
	// Where:
	//   - baseAsset: TRX (secondary/base asset code)
	//   - secondaryAsset: AUD (primary/quote asset code)
	//   - resolution: 1 (1 minute)
	//   - side: bid (for buy side)
	//   - limit: 60 (number of candles)
	//   - timeStart: Unix timestamp in milliseconds
	since := time.Now().Add(-1 * time.Hour)
	candles, err := client.GetCandles("TRX/AUD", "1m", since, 60)
	if err != nil {
		t.Fatalf("failed to fetch TRX/AUD candles: %v", err)
	}
	if len(candles) == 0 {
		t.Fatal("expected at least one candle, got 0")
	}

	// Validate candle data
	for i, candle := range candles {
		if candle.Open <= 0 {
			t.Errorf("candle %d: expected open > 0, got %f", i, candle.Open)
		}
		if candle.High <= 0 {
			t.Errorf("candle %d: expected high > 0, got %f", i, candle.High)
		}
		if candle.Low <= 0 {
			t.Errorf("candle %d: expected low > 0, got %f", i, candle.Low)
		}
		if candle.Close <= 0 {
			t.Errorf("candle %d: expected close > 0, got %f", i, candle.Close)
		}
		if candle.High < candle.Low {
			t.Errorf("candle %d: high (%f) should be >= low (%f)", i, candle.High, candle.Low)
		}
		if candle.OpenTime.IsZero() {
			t.Errorf("candle %d: expected non-zero open time", i)
		}
	}

	t.Logf("Successfully fetched %d candles for TRX/AUD", len(candles))
	fmt.Printf("Successfully fetched %d candles for TRX/AUD\n", len(candles))
}

func TestSwyftxFindScalpingCoins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Swyftx call in short mode")
	}

	// Note: This test may take several minutes due to API rate limiting
	// Run with: go test -timeout 10m -run TestSwyftxFindScalpingCoins
	t.Log("Starting scalping coins analysis (this may take a few minutes)...")
	fmt.Println("Starting scalping coins analysis (this may take a few minutes)...")

	client := NewSwyftxClient("", "", true, nil)

	// First, let's check if we can get trading pairs to see if assets are loading
	pairs, err := client.GetTradingPairs()
	if err != nil {
		t.Fatalf("failed to get trading pairs: %v", err)
	}
	t.Logf("Found %d total trading pairs", len(pairs))
	fmt.Printf("Found %d total trading pairs\n", len(pairs))

	coins, err := client.FindScalpingCoins("AUD", 1000, 10, 200*time.Millisecond)
	if err != nil {
		t.Logf("Error details: %v", err)
		fmt.Printf("Error details: %v\n", err)
		t.Fatalf("failed to find scalping coins: %v", err)
	}

	// Log found coins to terminal
	t.Log("\n=== Found Scalping Coins ===")
	fmt.Println("\n=== Found Scalping Coins ===")
	if len(coins) == 0 {
		t.Fatalf("No coins found matching the criteria - this is not normal")
	}

	for i, coin := range coins {
		coinInfo := fmt.Sprintf("%d. %s (%s)\n   Symbol: %s\n   Volume: %.2f AUD\n   Volatility: %.4f%%\n   Score: %.2f",
			i+1, coin.Code, coin.Name, coin.Symbol, coin.Volume, coin.Volatility, coin.Score)
		t.Log(coinInfo)
		fmt.Printf("%d. %s (%s)\n", i+1, coin.Code, coin.Name)
		fmt.Printf("   Symbol: %s\n", coin.Symbol)
		fmt.Printf("   Volume: %.2f AUD\n", coin.Volume)
		fmt.Printf("   Volatility: %.4f%%\n", coin.Volatility)
		fmt.Printf("   Score: %.2f\n", coin.Score)
		fmt.Println()
	}
	totalMsg := fmt.Sprintf("Total coins found: %d", len(coins))
	t.Log(totalMsg)
	fmt.Printf("Total coins found: %d\n", len(coins))
	t.Log("===========================")
	fmt.Println("===========================")
}
