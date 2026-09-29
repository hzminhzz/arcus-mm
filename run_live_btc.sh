#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ -f .env/env ]; then
  set -a
  source .env/env
  set +a
fi

export PYTHONUNBUFFERED=1

exec uv run arcus-maker \
  --markets BTC-USD \
  --account-address 0xC7b0Dc3EF72a946A659cE6104b05D2922435DFE6 \
  --account-index 0 \
  --order-size-usd 100 \
  --max-position-usd 100 \
  --maker-fee-bps 0 \
  --minimum-edge-bps 3 \
  --latency-buffer-bps 2 \
  --inventory-skew-bps 10 \
  --max-basis-bps 25 \
  --candidate-mode off \
  --submit \
  --mainnet
