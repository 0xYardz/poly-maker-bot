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
└── PositionTracker             # position state + PnL
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
4. Start the strategy engine (currently a no-op skeleton)
5. Pre-fetch the next market 60s before the window ends
6. Transition seamlessly at the window boundary
7. Repeat

Graceful shutdown with `Ctrl+C` — cancels open orders and closes all connections.

## Project Structure

```
poly_maker_bot/
├── maker_bot.py                 # main bot + market lifecycle
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
│   └── simple_mm.py             # SimpleMarketMaker skeleton
├── orders/
│   ├── manager.py               # order state tracking
│   └── placer.py                # order placement
├── position/
│   ├── tracker.py               # position tracking
│   └── pnl_calculator.py        # PnL calculations
├── metrics/                     # latency + rate tracking
└── utils/                       # price rounding, slug helpers
```
