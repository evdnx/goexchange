# goexchange

Go clients for trading and market data across Binance, Coinbase, and Swyftx. The library wraps REST and WebSocket flows with shared models, resilient HTTP defaults, and pluggable metrics so you can trade or stream data with a consistent interface.

## Features
- Unified `ExchangeClient` interface covering trading pairs, tickers, order books, candles, trades, balances, orders, and cancellations.
- Ready-made clients: Binance (spot and futures), Coinbase Exchange, and Swyftx.
- WebSocket helpers for streaming tickers, klines, trades, depth, and user data (exchange dependent) with auto-reconnect and subscription restore.
- Resilient HTTP defaults (retries, exponential backoff, configurable timeouts) built on [`github.com/evdnx/gohttpcl`](https://github.com/evdnx/gohttpcl).
- Structured logging via [`github.com/evdnx/golog`](https://github.com/evdnx/golog) and optional metrics hooks via [`github.com/evdnx/gotrademetrics`](https://github.com/evdnx/gotrademetrics).
- Typed models for tickers, candles, order books, trades, and orders in `models/` and `common/`.

## Install

```bash
go get github.com/evdnx/goexchange
```

Requires Go 1.25 or newer.

## Quick start

```go
package main

import (
	"fmt"

	"github.com/evdnx/goexchange/exchange"
)

func main() {
	client := exchange.NewBinanceClient("API_KEY", "API_SECRET", false, nil)

	ticker, err := client.GetTicker("BTCUSDT")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s last price: %.2f (bid %.2f / ask %.2f)\n", ticker.Symbol, ticker.LastPrice, ticker.Bid, ticker.Ask)
}
```

Swap in `NewCoinbaseClient` or `NewSwyftxClient` for other venues. Pass `true` for the `testnet` flag when the exchange supports sandbox endpoints.

## Streaming example (Binance)

```go
ws := client.GetWebSocketClient()
if err := ws.Connect(); err != nil {
	panic(err)
}
_ = ws.SubscribeToTicker("btcusdt", func(t models.Ticker) {
	fmt.Printf("stream tick: %s %.2f\n", t.Symbol, t.LastPrice)
})
```

## Error handling

HTTP and parsing failures surface as `*common.ExchangeError` with `Type`, `Code`, `StatusCode`, and `RawResponse` fields. Helper functions such as `common.IsNetworkError(err)` and `common.IsRateLimitError(err)` make classification easy for retries or alerting.

## Metrics and logging

- Metrics: pass a `*metrics.Metrics` instance when creating a client to enable request/latency/error reporting via `gotrademetrics`. Use `nil` to disable.
- Logging: use `common.DefaultLogger()` if you need a shared `golog` logger.

## License

MIT-0. See `LICENSE`.
