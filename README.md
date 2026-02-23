# poly-maker-bot

Market maker bot for Polymarket's 15-minute BTC/ETH up/down prediction markets. Discovers markets automatically, provides two-sided liquidity, and seamlessly transitions between 15-minute windows.

## Architecture

```
LiquidityProviderBot
├── Market Lifecycle Thread     # discovers markets, manages 15m window transitions
│   ├── discover_market()       # finds current/next market via Gamma API
│   ├── _activate_market()      # subscribes orderbook + starts strategy
│   └── _transition_to_next()   # stops old strategy, starts new one
│
├── StrategyEngine Thread       # runs quoting logic per market (1s tick)
│   ├── on_tick()               # read orderbook, place/cancel orders
│   └── on_fill()               # react to fills
│
├── WS Orderbook Client         # real-time orderbook data per token
├── WS Trade Client             # real-time fill notifications
├── OrderPlacer / OrderManager  # order execution + tracking
├── PositionTracker             # position state + PnL
├── TradeDatabase (SQLite)      # persistent trade history + session tracking
└── DashboardServer (optional)  # real-time web dashboard via SSE
```

### Strategy Engine

The bot uses a pluggable strategy pattern. `StrategyEngine` is an abstract base class — subclass it and implement `on_tick()` and `on_fill()` to create a new strategy:

```python
from poly_maker_bot.strategy import StrategyEngine

class MyStrategy(StrategyEngine):
    def on_tick(self):
        book = self.get_orderbook(self.market.up_token_id)
        # your quoting logic here

    def on_fill(self, token_id, side, price, size):
        # react to fills
```

Swap it in by changing the `_activate_market()` method in `maker_bot.py`.

## Setup

### Prerequisites

- Python 3.11+
- A Polymarket account with API credentials

### Install

```bash
git clone <repo-url>
cd poly-maker-bot
pip install -r requirements.txt
pip install -e .
```

### Environment Variables

Create a `.env` file in the project root:

```env
POLYMARKET_PK=<your-private-key>
POLYMARKET_FUNDER=<your-funder-address>
BUILDER_API_KEY=<builder-api-key>
BUILDER_SECRET=<builder-secret>
BUILDER_PASS_PHRASE=<builder-passphrase>

# Optional: enable the web dashboard on this port
POLY_DASHBOARD_PORT=8080
```

### Configuration

Exchange endpoints are configured in `config.yaml`. Defaults point to Polymarket production — you shouldn't need to change these.

## Usage

```bash
# Run with default settings
python -m poly_maker_bot.maker_bot

# Verbose logging
python -m poly_maker_bot.maker_bot -v
```

The bot will:
1. Initialize exchange client and WebSocket connections
2. Discover the current 15m BTC up/down market
3. Subscribe to orderbook data for both tokens
4. Start the strategy engine
5. Pre-fetch the next market 60s before the window ends
6. Transition seamlessly at the window boundary
7. Repeat

Graceful shutdown with `Ctrl+C` — cancels open orders and closes all connections.

## Dashboard

The bot includes an optional real-time web dashboard for monitoring PnL, positions, trades, and strategy state. Enable it by setting `POLY_DASHBOARD_PORT` in your `.env`:

```env
POLY_DASHBOARD_PORT=8080
```

Then open `http://localhost:8080` in your browser.

The dashboard shows:
- **All-time and session PnL** — realized, unrealized, and per-market breakdowns
- **Current market** — slug, countdown timer, orderbook top-of-book for both tokens
- **Live positions** — size, avg price, cost basis, current value, PnL and PnL %
- **Open orders** — side, price, size per order
- **Strategy state** — running status, pending reactions, fill accumulator
- **Trade history** — every fill the bot executes, persisted in SQLite
- **PnL by market** — aggregated trade count, volume, and estimated PnL per market
- **API latency metrics** — p50/p95/p99 for order placement and cancellation

All data is bot-specific (tracked locally, not pulled from your Polymarket account). Trade history is stored in `bot_data.db` (SQLite) and persists across restarts.

The dashboard runs in a daemon thread and has zero impact on bot performance — it only reads from existing thread-safe data structures and the SQLite database.

## Project Structure

```
poly_maker_bot/
├── maker_bot.py                 # main bot + market lifecycle
├── db.py                        # SQLite persistence (trades, markets, sessions)
├── config/                      # YAML config loading
├── exchange/
│   ├── polymarket_client.py     # CLOB REST API client
│   └── gamma_client.py          # Gamma API client
├── market/
│   ├── discovery.py             # discover_market() + Market dataclass
│   └── tracking.py              # MarketTracker
├── market_data/
│   ├── ws_orderbook_client.py   # WebSocket orderbook feed
│   ├── ws_trade_client.py       # WebSocket trade/fill feed
│   └── orderbook_store.py       # in-memory orderbook
├── strategy/
│   ├── base.py                  # StrategyEngine ABC
│   └── simple_mm.py             # SimpleMarketMaker
├── orders/
│   ├── manager.py               # order state tracking
│   └── placer.py                # order placement
├── position/
│   ├── tracker.py               # position tracking
│   └── pnl_calculator.py        # PnL calculations
├── dashboard/
│   ├── __init__.py              # HTTP server + SSE stream
│   ├── data_collector.py        # assembles snapshots from bot state + DB
│   └── templates/
│       └── index.html           # single-page dashboard (inline CSS/JS)
├── metrics/                     # latency + rate tracking
└── utils/                       # price rounding, slug helpers
```
