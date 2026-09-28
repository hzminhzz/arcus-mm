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
