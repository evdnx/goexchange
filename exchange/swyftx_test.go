package exchange

import (
	"testing"
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
