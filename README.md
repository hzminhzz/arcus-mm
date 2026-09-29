# Arcus Algorithmic Trading Suite

A high-performance, modular Python algorithmic trading bot suite for the
[Arcus](https://arcus.xyz) decentralized perpetual exchange. It provides a
continuous two-sided market maker anchored to Binance USD-M futures, an
asynchronous multi-limit directional grid bot, and a real-time account and order
book terminal monitor.

---

## Table of Contents

- [Overview](#overview)
- [Project Architecture](#project-architecture)
- [Installation](#installation)
- [Security and Environment Setup](#security-and-environment-setup)
- [1. Continuous Market Maker (`arcus-maker`)](#1-continuous-market-maker-arcus-maker)
  - [How It Works](#how-it-works)
  - [CLI Parameter Reference](#cli-parameter-reference)
  - [Dry-Run / Preview Mode](#dry-run--preview-mode)
  - [Live Trading on Testnet](#live-trading-on-testnet)
  - [Live Trading on Mainnet](#live-trading-on-mainnet)
  - [Risk Limits and Resting Time](#risk-limits-and-resting-time)
  - [Alpha Candidate Evaluation and Shadow Mode](#alpha-candidate-evaluation-and-shadow-mode)
- [2. Multi-Limit Directional Grid Bot (`arcus-bot`)](#2-multi-limit-directional-grid-bot-arcus-bot)
  - [How It Works](#how-it-works-1)
  - [CLI Parameter Reference](#cli-parameter-reference-1)
  - [Previewing the Grid](#previewing-the-grid)
  - [Live Trading with Grid Bot](#live-trading-with-grid-bot)
  - [Legacy Cycle Strategy](#legacy-cycle-strategy)
- [3. Read-Only Account Monitor (`arcus-monitor`)](#3-read-only-account-monitor-arcus-monitor)
- [Deployment and 24/7 Operations (systemd)](#deployment-and-247-operations-systemd)
- [Alpha Research, Recording, and Replay](#alpha-research-recording-and-replay)
- [Development and Testing](#development-and-testing)

---

## Overview

The repository exposes three CLI entry points:

1. **`arcus-maker`**: Continuous quoting market maker for `BTC-USD` and `ETH-USD`.
   Consumes Binance USD-M futures public order book tickers (`BTCUSDT`, `ETHUSDT`)
   as external fair value references, adjusts prices for fee economics, inventory
   skew, and latency buffers, and submits resting bid/ask limit quotes on Arcus.
2. **`arcus-bot`**: Multi-limit directional grid trading bot. Places passive entry
   limit orders, attaches take-profit limit orders upon fills, and spaces orders
   according to a configurable percentage grid step.
3. **`arcus-monitor`**: Read-only terminal dashboard showing order book depth,
   current positions, equity, free collateral, open orders, and recent fills.

---

## Project Architecture

```text
src/arcus_bot/
├── bots/
│   ├── market_maker/   # Continuous quoting strategy with Binance reference feed
│   │   ├── index.py    # Runtime coordinator, quote loops, freshness gating
│   │   ├── market.py   # Arcus market definitions, tick tiers, Binance mappings
│   │   ├── quoter.py   # Fair value estimation, basis tracker, price skew calculations
│   │   ├── order_manager.py # Lifecycle of quotes, minimum rest times, cancellations
│   │   └── runtime.py  # Maker configuration and state tracking
│   ├── grid/           # Multi-limit grid strategy and close-order tracking
│   └── cycle/          # Legacy sequential one-at-a-time entry/exit strategy
├── cli/
│   ├── maker.py        # Entry point for `arcus-maker`
│   ├── maker_config.py # CLI argument parsing and validation for market maker
│   ├── bot.py          # Entry point for `arcus-bot`
│   └── monitor.py      # Entry point for `arcus-monitor`
├── pricing/
│   └── binance.py      # Asynchronous Binance USD-M futures bookTicker feed
├── sdk/
│   ├── client.py       # Arcus WebSocket client and channel subscriptions
│   ├── account.py      # Balance, position, and order tracking
│   ├── orderbook.py    # L2 order book management
│   └── orders.py       # EIP-712 order signing and submission
├── utils/
│   └── logger.py       # Structured logging setup
└── types.py            # Core domain dataclasses and validation errors
tests/                  # Unit and integration test suite
deploy/                 # Production systemd service unit templates
```

---

## Installation

### Prerequisites

- **Python**: Version 3.12 or newer
- **uv**: Fast Python package manager ([install instructions](https://github.com/astral-sh/uv))

### From Git Checkout

Clone the repository and install dependencies using `uv`:

```bash
git clone https://github.com/hzminhzz/arcus-mm.git
cd arcus-mm
uv sync
```

The CLI binaries are placed in `.venv/bin/` and can also be run with `uv run`:
- `uv run arcus-maker --help`
- `uv run arcus-bot --help`
- `uv run arcus-monitor --help`

### As a Global CLI Tool

To install directly into your environment using `uv tool`:

```bash
uv tool install git+https://github.com/hzminhzz/arcus-mm
uv tool update-shell
```

---

## Security and Environment Setup

### Private Key Management

Live order placement requires an Ethereum-compatible signing key authorized for
your Arcus subaccount.

- The key is read exclusively from the `ARCUS_API_SIGNING_KEY` environment variable.
- **Never** hardcode private keys in code or scripts.
- **Never** pass private keys via CLI flags (CLI arguments are visible in process tables like `ps aux`).
- Store local keys in a restricted environment file (`chmod 600`):

```bash
# Example ~/.config/arcus-maker.env
ARCUS_API_SIGNING_KEY=0x...your-32-byte-hex-signing-key...
```

To load it in your current terminal session:

```bash
set -a
source ~/.config/arcus-maker.env
set +a
```

---

## 1. Continuous Market Maker (`arcus-maker`)

### How It Works

`arcus-maker` quotes two-sided liquidity around a reference price:

1. **Binance Reference**: Subscribes to real-time `bookTicker` events from
   Binance USD-M Futures (`wss://fstream.binance.com/public/ws`) for `BTCUSDT`
   and `ETHUSDT`.
2. **Basis Adjustment**: Tracks rolling median basis differences between Arcus
   and Binance to account for persistent exchange spread divergences.
3. **Economic Pricing**:
   - Computes bid and ask prices from fair value plus/minus maker fees, minimum
     edge, and latency buffer.
   - Applies an inventory skew penalty: if long inventory accumulates, bids are
     discounted and asks become more aggressive to unwind inventory.
4. **Resting Quote Management**:
   - Healthy quotes rest for at least `--minimum-order-rest-ms` (default 5000 ms)
     to avoid excessive churn and allow orders to accumulate queue priority.
   - If reference feeds go stale, basis limits breach, or book cross conditions
     occur, quotes are immediately canceled for protection.

### CLI Parameter Reference

| Flag | Required | Default | Description |
|---|---|---|---|
| `--markets` | No | `BTC-USD,ETH-USD` | Comma-separated Arcus markets: `BTC-USD`, `ETH-USD`. |
| `--order-size-usd` | **Yes** | — | Order notional per quote in USD (e.g. `40`). |
| `--max-position-usd` | **Yes** | — | Maximum net position allowed in USD (e.g. `400`). |
| `--maker-fee-bps` | **Yes** | — | Arcus maker fee in bps (can be `0` or negative for rebates). |
| `--minimum-edge-bps` | **Yes** | — | Minimum expected edge/profit margin in bps (e.g. `3`). |
| `--latency-buffer-bps`| **Yes** | — | Execution latency risk buffer in bps (e.g. `2`). |
| `--inventory-skew-bps`| **Yes** | — | Inventory skew factor in bps per 100% position limit (e.g. `10`). |
| `--max-basis-bps` | **Yes** | — | Max allowable basis divergence between Binance and Arcus before quoting pauses (e.g. `25`). |
| `--account-address` | For `--submit` | `""` | Arcus Ethereum account address (`0x...`). |
| `--account-index` | No | `0` | Subaccount index (`0` through `9`). |
| `--minimum-order-rest-ms` | No | `5000` | Minimum resting time (ms) before replacing healthy orders. |
| `--maximum-feed-age-ms` | No | `2000` | Maximum age (ms) of Binance reference feed before pausing. |
| `--maximum-book-age-ms` | No | `1000` | Maximum age (ms) of Arcus order book before pausing. |
| `--requote-interval-ms` | No | `500` | Frequency (ms) of quoting cycle checks. |
| `--basis-window-seconds`| No | `300` | Rolling window (seconds) for basis estimation. |
| `--basis-samples` | No | `3` | Minimum basis samples required before quoting begins. |
| `--duration-seconds` | No | `0` | Auto-stop preview after N seconds (`0` runs indefinitely). |
| `--candidate-mode` | No | `off` | Alpha candidate evaluation mode (`off`, `shadow`, `bounded`). |
| `--max-alpha-bps` | No | `0` | Maximum candidate alpha offset in basis points. |
| `--alpha-report-path` | For bounded | `""` | Path to authentic GO alpha evaluation report (required for bounded mode). |
| `--dry-run` | No | `True` | Non-trading preview mode (default). |
| `--submit` | For live orders | `False`| Explicit opt-in flag required to place live orders. |
| `--mainnet` | For mainnet | `False`| Route to Arcus mainnet (must be paired with `--submit`). |
| `--log-level` | No | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

### Dry-Run / Preview Mode

Test configuration, pricing calculations, and feed connectivity without submitting orders:

```bash
uv run arcus-maker \
  --markets BTC-USD,ETH-USD \
  --order-size-usd 40 \
  --max-position-usd 400 \
  --maker-fee-bps 0 \
  --minimum-edge-bps 3 \
  --latency-buffer-bps 2 \
  --inventory-skew-bps 10 \
  --max-basis-bps 25 \
  --duration-seconds 30
```

### Live Trading on Testnet

Submits real quotes to the Arcus testnet. Requires `ARCUS_API_SIGNING_KEY` and `--account-address`:

```bash
set -a && source ~/.config/arcus-maker.env && set +a

uv run arcus-maker \
  --markets BTC-USD,ETH-USD \
  --account-address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --order-size-usd 40 \
  --max-position-usd 400 \
  --maker-fee-bps 0 \
  --minimum-edge-bps 3 \
  --latency-buffer-bps 2 \
  --inventory-skew-bps 10 \
  --max-basis-bps 25 \
  --submit
```

### Live Trading on Mainnet

To submit live orders to Arcus mainnet, provide both `--submit` and `--mainnet`:

```bash
set -a && source ~/.config/arcus-maker.env && set +a

uv run arcus-maker \
  --markets BTC-USD,ETH-USD \
  --account-address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --order-size-usd 40 \
  --max-position-usd 400 \
  --maker-fee-bps 0 \
  --minimum-edge-bps 3 \
  --latency-buffer-bps 2 \
  --inventory-skew-bps 10 \
  --max-basis-bps 25 \
  --submit \
  --mainnet
```

### Risk Limits and Resting Time

- **Dedicated Subaccount**: On startup, `arcus-maker` automatically cancels any
  pre-existing open orders for the targeted market to take clean ownership of quotes.
- **Graceful Shutdown**: On `SIGINT` (Ctrl+C) or `SIGTERM`, all active maker
  quotes are automatically canceled and verified before exiting.
- **Order Rest Time**: Controlled by `--minimum-order-rest-ms` (default 5000 ms).
  Healthy quotes are held to provide queue priority and avoid API spam. Unhealthy
  quotes (breached limits, crossed markets, stale feeds) are canceled immediately.

### Alpha Candidate Evaluation and Shadow Mode

The market maker supports evaluating pricing signals alongside the production baseline quoter without risking capital.

Run shadow mode in a local dry run:

```bash
uv run arcus-maker \
  --markets BTC-USD \
  --candidate-mode shadow \
  --dry-run
```

In shadow mode, the quoter computes candidate prices alongside baseline quotes. It logs side-by-side quote comparisons, including alpha offsets in basis points and resulting spreads. When run with `--submit`, shadow mode places only baseline quotes, so candidate logic has zero live order impact. Passing `--dry-run` guarantees no orders touch any exchange.

---

## 2. Multi-Limit Directional Grid Bot (`arcus-bot`)

### How It Works

`arcus-bot` runs a directional grid strategy:
- Maintains up to `--max-orders` working limit entry orders.
- Each time an entry fills, a corresponding take-profit exit order is automatically
  submitted at `(fill_price * (1 + take_profit_percent))`.
- Spaces entry orders using a directional percentage grid step (`--grid-step`).
- Manages entry timeouts (`--entry-timeout-seconds`) to cancel unfilled entries
  and re-anchor closer to the market.
- Halts new entries when reaching `--stop-price` or `--pause-price`.
- Shuts down once `--max-total-volume` cumulative gross turnover is reached.

### CLI Parameter Reference

| Flag | Required | Default | Description |
|---|---|---|---|
| `--strategy` | No | `grid` | Strategy engine: `grid` or `cycle`. |
| `--address` | **Yes** | — | Arcus account address (`0x...`). |
| `--account-index` | **Yes** | `0` | Subaccount index (`0` through `9`). |
| `--market-id` | **Yes** | — | Market ID (`1` for BTC-USD, `2` for ETH-USD). |
| `--market` | **Yes** | — | Market display name (`BTC-USD`, `ETH-USD`). |
| `--side` | No | `BUY` | Entry side (`BUY` or `SELL`). |
| `--quantity` | **Yes** | — | Order quantity in base asset units (e.g. `0.0006`). |
| `--tick-size` | **Yes** | — | Market price tick size (e.g. `0.1` for BTC-USD). |
| `--step-size` | **Yes** | — | Market quantity step size (e.g. `0.00000001`). |
| `--take-profit-percent` | No | `0.02` | Take profit target percent (e.g. `0.02` for 2%). |
| `--max-order-notional` | No | `1000` | Maximum notional value in USD per single order. |
| `--max-total-volume` | No | `10000`| Total cumulative gross trading volume cap. |
| `--max-orders` | No | `4` | Maximum total concurrent active entry + exit orders. |
| `--wait-seconds` | No | `450` | Cooldown period between entry submissions. |
| `--entry-timeout-seconds` | No | `300` | Timeout after which an unfilled entry is canceled. |
| `--grid-step` | No | `0.5` | Percent price spacing between grid levels (`0` disables). |
| `--stop-price` | No | `-1` | Stop adding new entries at this directional price (`-1` disables). |
| `--pause-price` | No | `-1` | Temporarily pause new entries at this directional price (`-1` disables). |
| `--submit` | For live orders | `False`| Place live orders on Arcus (default is preview). |
| `--mainnet` | For mainnet | `False`| Use mainnet instead of testnet. |

### Previewing the Grid

Test your grid geometry without placing orders:

```bash
uv run arcus-bot \
  --address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --market-id 1 \
  --market BTC-USD \
  --side BUY \
  --quantity 0.0006 \
  --tick-size 0.1 \
  --step-size 0.00000001 \
  --max-orders 4 \
  --wait-seconds 450 \
  --entry-timeout-seconds 300 \
  --grid-step 0.5 \
  --max-order-notional 60 \
  --max-total-volume 5000
```

### Live Trading with Grid Bot

To execute on testnet:

```bash
set -a && source ~/.config/arcus-maker.env && set +a

uv run arcus-bot \
  --address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --market-id 1 \
  --market BTC-USD \
  --side BUY \
  --quantity 0.0006 \
  --tick-size 0.1 \
  --step-size 0.00000001 \
  --max-orders 4 \
  --wait-seconds 450 \
  --entry-timeout-seconds 300 \
  --grid-step 0.5 \
  --max-order-notional 60 \
  --max-total-volume 5000 \
  --submit
```

Add `--mainnet` for live mainnet execution.

### Legacy Cycle Strategy

The cycle strategy executes one order cycle at a time: it places a single entry,
waits for fill, places a take-profit order, waits for exit fill, and repeats:

```bash
uv run arcus-bot \
  --strategy cycle \
  --address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --market-id 1 \
  --market BTC-USD \
  --side BUY \
  --quantity 0.0006 \
  --tick-size 0.1 \
  --step-size 0.00000001 \
  --take-profit-percent 0.02 \
  --cycles 10 \
  --submit
```

---

## 3. Read-Only Account Monitor (`arcus-monitor`)

`arcus-monitor` provides a live terminal dashboard displaying:
- Top-of-book BBO and spread
- Account equity, balance, and free collateral
- Active open positions, unrealized PnL, and liquidation prices
- Open orders and recent fill history

It operates on public WebSocket channels and does not require a private key.

### Usage

```bash
# Testnet monitor
uv run arcus-monitor \
  --address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --market-id 1 \
  --market BTC-USD

# Mainnet monitor
uv run arcus-monitor \
  --address 0xYOUR_ACCOUNT_ADDRESS \
  --account-index 0 \
  --market-id 1 \
  --market BTC-USD \
  --mainnet
```

---

## Deployment and 24/7 Operations (systemd)

For continuous, reliable operation on a Linux VPS, run `arcus-maker` under a
user-level systemd service unit.

### 1. Configure Environment File

Create `~/.config/arcus-maker.env` and lock permissions:

```bash
cat << 'EOF' > ~/.config/arcus-maker.env
ARCUS_API_SIGNING_KEY=your-32-byte-hex-signing-key
ARCUS_MARKETS=BTC-USD,ETH-USD
ARCUS_ADDRESS=0xyour-account-address
ARCUS_ACCOUNT_INDEX=0
ARCUS_ORDER_SIZE_USD=40
ARCUS_MAX_POSITION_USD=400
ARCUS_MAKER_FEE_BPS=0
ARCUS_MINIMUM_EDGE_BPS=3
ARCUS_LATENCY_BUFFER_BPS=2
ARCUS_INVENTORY_SKEW_BPS=10
ARCUS_MAX_BASIS_BPS=25
EOF

chmod 600 ~/.config/arcus-maker.env
```

### 2. Install the User Service Unit

The repository includes `deploy/arcus-maker.service`:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/arcus-maker.service ~/.config/systemd/user/arcus-maker.service
```

*(Note: The default unit executes in preview mode. To submit live orders, edit
`ExecStart` in `~/.config/systemd/user/arcus-maker.service` to append `--submit`
or `--submit --mainnet`)*

### 3. Service Commands

```bash
# Reload user daemon
systemctl --user daemon-reload

# Start the market maker
systemctl --user start arcus-maker

# Check status
systemctl --user status arcus-maker

# Follow live output logs
journalctl --user -u arcus-maker -f

# Stop gracefully (SIGINT allows 30s to cancel open orders)
systemctl --user stop arcus-maker
```

To enable persistent execution across SSH logout:

```bash
loginctl enable-linger "$USER"
```

For extended VPS operational patterns, see [`VPS.md`](VPS.md).

---

## Alpha Research, Recording, and Replay

The alpha evaluation pipeline lets operators record market conditions, test candidate signals against historical book states, and verify safety before considering live changes.

### Public Data Recording CLI

Operators can record synchronized Arcus and Binance market feeds using the standalone recorder:

```bash
uv run python -m arcus_bot.alpha.record \
  --markets BTC-USD,ETH-USD \
  --duration-seconds 12 \
  --max-bytes 1048576 \
  --output .omo/evidence/arcus-maker-alpha/public.jsonl
```

This recorder connects to public websockets on both exchanges. Top-of-book updates capture monotonic receipt timestamps (`recv_time_ns`) and exchange event times (`event_time_ms`). Output stops when reaching `--duration-seconds` or `--max-bytes`.

### Point-in-Time Offline Replay and Evaluation CLI

Replay recorded public book states against observed fill records:

```bash
uv run python -m arcus_bot.alpha.replay \
  --input .omo/evidence/arcus-maker-alpha/public.jsonl \
  --fills .omo/evidence/arcus-maker-alpha/fills.jsonl \
  --output .omo/evidence/arcus-maker-alpha/replay.json
```

The replay engine scores candidate quotes against baseline markouts over a specified horizon (`--horizon-ms`, default 100 ms). It runs bootstrap confidence intervals, split-halves sign tests, and cost stress tests at 1x, 2x, and 3x fee multiples.

Economics values in the JSON report are `null` when unavailable, not zero. `economics_status` labels each top-level markout, difference, bootstrap interval, and halves-stability field; each `cost_stress_results` multiplier has a `status` applying to its markouts, difference, and `positive` flag. Status is `unavailable` until paired observed fills and matching post-horizon books permit calculation, then `estimated`: future-mid markouts and their derived tests are estimates, not realized P&L. `measured` is reserved for directly observed quantities (for example, validated action latency provenance), not these projected economics. The report's input SHA-256 hashes bind self-authored files to the report but do not authenticate venue observations or independently prove fill provenance. A fixture/test `GO` exercises the decision gate only; it is never live performance evidence or deployment approval. The public-only capture has no fills and cannot support empirical paired-policy economics.

### NO_GO and INCONCLUSIVE Decision Semantics

The replay evaluator enforces strict risk gates before emitting a verdict:

- **Synthetic fills trigger NO_GO**: Synthetic or counterfactual fills can never certify a GO decision. Simulated fills cannot model adverse selection, queue placement, or price impact. If any fill has a source or provenance other than `observed`, the evaluator flags an immediate `NO_GO`.
- **Sparse data triggers INCONCLUSIVE**: The evaluator returns `INCONCLUSIVE` whenever observations fall below statistical thresholds (under 30 observed fills per policy or fewer than 100 independent book decisions). After-cost markouts require separately observed fills under both policies in comparable regimes.
- **Cost stress and stability tests**: A candidate must show positive net markouts across all fee stress multiples (1x, 2x, 3x) and maintain consistent signs across chronological halves.
- **Mandatory human operator review**: An automated report never authorizes deployment by itself. Live rollout requires separate operator review to inspect market regimes, fee tiers, and risk limits.

### Operational Safety and Non-Disruption Guarantees

The alpha workflow protects running production infrastructure:

1. **Running bot remains untouched**: The live maker service runs independently. Local recording, replay, and dry-run shadow quoting do not send signals or share state with existing bot processes.
2. **Service unit stays on safe default**: `deploy/arcus-maker.service` leaves `--candidate-mode` omitted, defaulting to `off`. Candidate mode "off" serves as the primary operational fallback, avoiding unplanned restarts.
3. **Local candidate inspection**: Operators inspect candidate outputs locally using `--dry-run` or offline replay files without touching mainnet orders or altering production state. Testnet observations must not be confused with mainnet profitability.

---

## Development and Testing

Run test suite with pytest:

```bash
uv run pytest
```

Run static type checking with basedpyright:

```bash
uv run basedpyright
```
