# Arcus Multi-Limit Grid Bot

The default strategy keeps multiple passive entry limits working, pairs each
entry fill with a take-profit limit, and spaces projected close prices using a
directional grid. A legacy one-at-a-time strategy remains available with
`--strategy cycle`.

```text
src/arcus_bot/
├── bots/cycle/        # Legacy one-at-a-time entry/exit strategy
├── bots/grid/         # Multi-entry grid and close-order management
├── bots/market_maker/ # Continuous testnet quoting strategy
├── cli/               # Trading and read-only monitor commands
├── pricing/           # External reference price feeds
├── sdk/               # Arcus WebSocket, account, book, and order adapters
├── utils/             # Process logging
└── types.py           # Shared account and strategy types
tests/                # Strategy, signing-payload, and account-state tests
```

Install the project and development tools:

```bash
uv sync
```

For a locked production install and VPS/systemd instructions, see
[`VPS.md`](VPS.md). The public repository can also be installed as a user-level
CLI package with `uv tool install git+https://github.com/hzminhzz/arcus-mm`.

Preview the grid without connecting to Arcus:

```bash
uv run arcus-bot \
  --address 0x... --account-index 0 \
  --market-id 1 --market BTC-USD \
  --side BUY --quantity 0.0006 --tick-size 0.1 \
  --step-size 0.00000001 --max-orders 4 \
  --wait-seconds 450 --entry-timeout-seconds 300 \
  --grid-step 0.5 --max-order-notional 60 \
  --max-total-volume 5000
```

`--max-orders` counts active entries and exits; the bot reserves one exit slot
for every active entry. `--entry-timeout-seconds` applies to entry orders only.
Take-profit orders stay open until filled or canceled. There is no overall
wall-clock timeout: the grid runs until its volume cap, `--stop-price`, or a
manual interrupt. Stop and pause prices are disabled by default (`-1`); for
BUY, both trigger when price is at or above their threshold, and for SELL they
trigger at or below it. The grid-step value is a percent; `0` disables spacing.

`--max-total-volume` is a cumulative gross-turnover limit, not a loss cap.
Startup requires a flat subaccount with no open orders. This strategy has no
stop-loss.

Live order submission requires `ARCUS_API_SIGNING_KEY` and the explicit
`--submit` flag. Testnet is the default; add `--mainnet` to select mainnet.

The separate read-only dashboard uses public Arcus channels and does not need
the signing key:

```bash
uv run arcus-monitor \
  --address 0x... --account-index 0 \
  --market-id 1 --market BTC-USD
```

The monitor shows the book, position, account equity and free collateral, open
orders, and recent fills. Press Ctrl+C to disconnect.

## Continuous BTC/ETH market maker

`arcus-maker` runs a separate continuous quoting strategy for Arcus BTC-USD
(market 1) and ETH-USD (market 2), using BTCUSDT and ETHUSDT Binance USD-M
perpetual book-ticker streams as external references. It defaults to
non-trading previews on Arcus testnet. `--submit` is required to place orders;
mainnet additionally requires `--mainnet`. Either live mode requires
`ARCUS_API_SIGNING_KEY` and `--account-address`.

Fee and risk/economic inputs are mandatory rather than guessed. Supply
`--maker-fee-bps`, `--minimum-edge-bps`, `--latency-buffer-bps`,
`--inventory-skew-bps`, `--order-size-usd`, `--max-position-usd`, and
`--max-basis-bps`. The account should be dedicated to this maker: startup
cancels any existing orders for the selected market, and shutdown cancels and
confirms its remaining orders.

```bash
uv run arcus-maker \
  --markets BTC-USD,ETH-USD \
  --maker-fee-bps "$MAKER_FEE_BPS" \
  --minimum-edge-bps "$MINIMUM_EDGE_BPS" \
  --latency-buffer-bps "$LATENCY_BUFFER_BPS" \
  --inventory-skew-bps "$INVENTORY_SKEW_BPS" \
  --order-size-usd "$ORDER_SIZE_USD" \
  --max-position-usd "$MAX_POSITION_USD" \
  --max-basis-bps "$MAX_BASIS_BPS"
```

Use `--duration-seconds N` to stop a non-trading preview after `N` seconds. To
submit testnet orders, add `--submit --account-address 0x...`. Mainnet requires
both `--submit` and `--mainnet --account-address 0x...`.
