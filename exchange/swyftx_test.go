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

func TestSwyftxFindScalpingCoins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live Swyftx call in short mode")
	}

	// Note: This test may take several minutes due to API rate limiting
	// Run with: go test -timeout 10m -run TestSwyftxFindScalpingCoins
	t.Log("Starting scalping coins analysis (this may take a few minutes)...")
	fmt.Println("Starting scalping coins analysis (this may take a few minutes)...")

	client := NewSwyftxClient("", "", true, nil)

	coins, err := client.FindScalpingCoins("AUD", 500000, 10, 200*time.Millisecond)
	if err != nil {
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
