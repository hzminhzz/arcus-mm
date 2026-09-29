# Install and run Arcus on a VPS

The project requires Python 3.12 or newer. The commands below use `uv` and the
checked-in `uv.lock` to install the same dependency versions used for testing.

## Install

Install `uv` on Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

Clone the repository and install the production dependencies:

```bash
git clone https://github.com/hzminhzz/arcus-mm.git "$HOME/arcus-mm"
cd "$HOME/arcus-mm"
uv sync --frozen --no-dev
```

The commands are available in `.venv/bin`. Confirm the install without
connecting to Arcus or submitting orders:

```bash
.venv/bin/arcus-bot --help
.venv/bin/arcus-monitor --help
.venv/bin/arcus-maker --help
```

For a user-level command installation instead of a checkout:

```bash
uv tool install git+https://github.com/hzminhzz/arcus-mm
uv tool update-shell
```

Open a new shell after `uv tool update-shell`. The package exposes the
`arcus-bot`, `arcus-monitor`, and `arcus-maker` commands.

## Run the BTC grid

Set `ARCUS_API_SIGNING_KEY` in the process environment; never put the key in
the repository or in a command-line argument. For an interactive shell:

```bash
read -r -s -p "Arcus signing key: " ARCUS_API_SIGNING_KEY
printf '\n'
export ARCUS_API_SIGNING_KEY
read -r -p "Arcus account address: " ARCUS_ADDRESS
export ARCUS_ADDRESS
```

Preview the configuration first. A dry run does not connect to Arcus or submit
orders:

```bash
.venv/bin/arcus-bot \
  --strategy grid \
  --address "$ARCUS_ADDRESS" --account-index 0 \
  --market-id 1 --market BTC-USD --side BUY \
  --quantity 0.0006 --tick-size 0.1 --step-size 0.00000001 \
  --take-profit-percent 0.02 \
  --max-order-notional 60 --max-total-volume 5000 \
  --max-orders 4 --wait-seconds 450 --entry-timeout-seconds 300 \
  --grid-step 0.5 --stop-price -1 --pause-price -1
```

To submit real mainnet orders, add `--submit --mainnet` after reviewing the
settings. The example is a starting configuration, not a guarantee of profit.
The bot has no stop-loss. `--max-orders` limits concurrent order slots, while
`--max-total-volume` limits cumulative gross turnover; neither caps losses.

Run a separate read-only account monitor in another shell:

```bash
.venv/bin/arcus-monitor \
  --address "$ARCUS_ADDRESS" --account-index 0 \
  --market-id 1 --market BTC-USD --mainnet
```

## Optional systemd service

For unattended operation, install the checkout under `/opt/arcus-mm`, create a
dedicated Linux service account, and store runtime values in a root-readable
environment file such as `/etc/arcus-mm/bot.env`:

```text
ARCUS_API_SIGNING_KEY=your-32-byte-hex-signing-key
ARCUS_ADDRESS=0x-your-account-address
```

Set that file to mode `0600`. Do not commit it. Create
`/etc/systemd/system/arcus-grid.service`, replacing the service user and
install path as appropriate. Clone the repository and run
`uv sync --frozen --no-dev` as the service account before installing the unit:

```ini
[Unit]
Description=Arcus BTC-USD grid bot
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=arcus
WorkingDirectory=/opt/arcus-mm
EnvironmentFile=/etc/arcus-mm/bot.env
ExecStart=/opt/arcus-mm/.venv/bin/arcus-bot --strategy grid --address ${ARCUS_ADDRESS} --account-index 0 --market-id 1 --market BTC-USD --side BUY --quantity 0.0006 --tick-size 0.1 --step-size 0.00000001 --take-profit-percent 0.02 --max-order-notional 60 --max-total-volume 5000 --max-orders 4 --wait-seconds 450 --entry-timeout-seconds 300 --grid-step 0.5 --stop-price -1 --pause-price -1 --submit --mainnet
Restart=no
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Then enable the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now arcus-grid
sudo journalctl -u arcus-grid -f
```

The bot requires a flat subaccount with no open orders at startup. It does not
reconcile a pre-existing position after a VPS reboot or process failure.
Before restarting it, check the account and manage any remaining position or
take-profit orders through Arcus. A normal bot stop cancels entry orders but
leaves take-profit orders working.

## Keep the continuous maker running

The reference maker project documents Docker Compose with a restart policy.
This project provides a user-level systemd unit instead, which survives shell
logout and records output in the journal. Automatic restarts are deliberately
disabled: the continuous maker cancels its owned orders when it stops, and on
startup it cancels every open order in the selected account before quoting.
It does not flatten existing positions. Do not enable or restart the service
until you have verified the account's positions and orders and intend to let
this maker take ownership of all open orders in that account.

Install the unit for the current user:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/arcus-maker.service ~/.config/systemd/user/arcus-maker.service
```

Create `~/.config/arcus-maker.env` with the local signing key and verified
configuration. Keep this file private (`chmod 600`) and do not put credentials
in the unit, command history, or repository. For example, with deliberately
non-submitting testnet settings:

```text
ARCUS_API_SIGNING_KEY=<local key>
ARCUS_MARKETS=BTC-USD,ETH-USD
ARCUS_ADDRESS=<account>
ARCUS_ACCOUNT_INDEX=0
ARCUS_ORDER_SIZE_USD=40
ARCUS_MAX_POSITION_USD=400
ARCUS_MAKER_FEE_BPS=0
ARCUS_MINIMUM_EDGE_BPS=3
ARCUS_LATENCY_BUFFER_BPS=2
ARCUS_INVENTORY_SKEW_BPS=10
ARCUS_MAX_BASIS_BPS=25
```

The unit is not enabled at boot and uses `Restart=no`. Inspect the account and
processes before every start; starting a second maker is not a recovery method.
The checked-in unit omits `--submit` and `--mainnet`, so it is deliberately
limited to the CLI's dry-run/testnet defaults and cannot place live orders.
For an authorized live deployment, review and explicitly change `ExecStart`
to add both opt-ins; never store trading credentials in that command.

```bash
systemctl --user daemon-reload
systemctl --user start arcus-maker
systemctl --user status arcus-maker
journalctl --user -u arcus-maker -f
```

Stop it with `systemctl --user stop arcus-maker` and allow up to 30 seconds for
SIGINT shutdown to cancel and confirm maker orders. A stop does not flatten any
BTC or ETH position. Check Arcus positions and open orders after both start and
stop. To keep it across logout, the user manager must support lingering:
`loginctl enable-linger "$USER"` (requires administrator authorization).

## Alpha evaluation and operator safety

Operators can record public market feeds, evaluate candidate alpha models offline, and test candidate quoting in shadow mode without disturbing active production services.

### Safe shadow mode inspection

To observe candidate pricing on a VPS or local machine without altering live orders:

```bash
uv run arcus-maker \
  --markets BTC-USD \
  --candidate-mode shadow \
  --dry-run
```

Shadow mode runs candidate quoter logic alongside the production baseline, logging side-by-side comparisons of alpha offsets and quote spreads. Because `--dry-run` is active, it places no orders. If run in live mode with `--submit`, shadow mode places only baseline orders, keeping live order placement insulated from candidate shifts.

### Public data recording

Collect public order book events from Arcus and Binance over a bounded window:

```bash
uv run python -m arcus_bot.alpha.record \
  --markets BTC-USD,ETH-USD \
  --duration-seconds 12 \
  --max-bytes 1048576 \
  --output .omo/evidence/arcus-maker-alpha/public.jsonl
```

The recorder captures public websockets with monotonic nanosecond receipt stamps. It runs completely outside the trading service and needs no API credentials.

### Point-in-time replay evaluation

Evaluate alpha candidates offline against historical public book data and fill records:

```bash
uv run python -m arcus_bot.alpha.replay \
  --input .omo/evidence/arcus-maker-alpha/public.jsonl \
  --fills .omo/evidence/arcus-maker-alpha/fills.jsonl \
  --output .omo/evidence/arcus-maker-alpha/replay.json
```

Replay evaluation enforces strict decision thresholds:

- **Synthetic fills force NO_GO**: Simulated or counterfactual fills can never certify a GO decision. They cannot capture real queue priority or adverse selection. Any non-observed fill provenance results in an immediate NO_GO.
- **Data scarcity yields INCONCLUSIVE**: Evaluations require at least 30 observed fills per policy and at least 100 independent decisions. If thresholds are not met, the verdict is INCONCLUSIVE. After-cost markouts require separately observed fills under both policies in comparable regimes.
- **Operator review is required**: Replay reports inform operators but do not automate deployment. Any live rollout requires human operator inspection and sign-off.

### Production non-disruption guarantees

VPS deployments follow strict operational boundaries:

- **Running maker is untouched**: Never restart or interrupt `arcus-maker.service` while investigating candidates. The live service maintains active quotes and positions.
- **Service file remains unchanged**: `deploy/arcus-maker.service` keeps candidate mode disabled by default (`candidate-mode off`). The default fallback is turning candidate mode off rather than restarting the process.
- **No unverified mainnet claims**: Testnet observations never imply mainnet profitability. Keep evaluation isolated to dry runs and recorded replays until real evidence is reviewed.
