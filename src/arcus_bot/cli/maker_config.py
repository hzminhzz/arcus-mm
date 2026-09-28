"""Command-line parsing for the BTC/ETH continuous maker."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

from arcus_bot.bots.market_maker.market import MARKET_MAPPINGS, MarketMapping
from arcus_bot.bots.market_maker.quoter import MakerQuoteConfig
from arcus_bot.types import InputError

_HEX: Final = frozenset("0123456789abcdefABCDEF")
_ZERO_ADDRESS: Final = "0x0000000000000000000000000000000000000000"


@dataclass(slots=True)
class MakerArguments(argparse.Namespace):
    """Parsed continuous-maker command-line options."""

    markets: str = "BTC-USD,ETH-USD"
    account_address: str = ""
    account_index: int = 0
    order_size_usd: str = ""
    max_position_usd: str = ""
    maker_fee_bps: str = ""
    minimum_edge_bps: str = ""
    latency_buffer_bps: str = ""
    inventory_skew_bps: str = ""
    max_basis_bps: str = ""
    maximum_feed_age_ms: int = 2_000
    maximum_book_age_ms: int = 1_000
    maximum_pair_skew_ms: int = 1_000
    requote_interval_ms: int = 500
    minimum_order_rest_ms: int = 5_000
    basis_window_seconds: int = 300
    basis_samples: int = 3
    duration_seconds: int = 0
    dry_run: bool = False
    submit: bool = False
    mainnet: bool = False
    log_level: str = "INFO"


@dataclass(frozen=True, slots=True)
class MakerOptions:
    """Validated CLI settings before opening any exchange connection."""

    markets: tuple[MarketMapping, ...]
    account_address: str
    account_index: int
    quote_config: MakerQuoteConfig
    maximum_basis_bps: Decimal
    maximum_feed_age_ms: int
    maximum_book_age_ms: int
    maximum_pair_skew_ms: int
    requote_interval_ms: int
    minimum_order_rest_ms: int
    basis_window_seconds: int
    basis_samples: int
    duration_seconds: int
    submit: bool
    mainnet: bool
    signing_key: str | None
    log_level: str


def _decimal(value: str, label: str, *, allow_negative: bool = False) -> Decimal:
    """Parse one finite decimal user setting."""
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InputError(f"{label} must be a decimal number") from error
    if not parsed.is_finite():
        raise InputError(f"{label} must be finite")
    if not allow_negative and parsed < 0:
        raise InputError(f"{label} cannot be negative")
    return parsed


def parse_options(argv: list[str] | None = None) -> MakerOptions:
    """Parse explicit markets, quote economics, and hard limits."""
    parser = argparse.ArgumentParser(
        description=(
            "Run the continuous BTC/ETH Arcus maker. "
            "Testnet dry-run is the default; --submit is required for order placement."
        )
    )
    _ = parser.add_argument(
        "--markets",
        default="BTC-USD,ETH-USD",
        help="Comma-separated supported Arcus markets (default: BTC-USD,ETH-USD).",
    )
    _ = parser.add_argument("--account-address", default="")
    _ = parser.add_argument("--account-index", type=int, default=0)
    _ = parser.add_argument("--order-size-usd", required=True)
    _ = parser.add_argument("--max-position-usd", required=True)
    _ = parser.add_argument("--maker-fee-bps", required=True)
    _ = parser.add_argument("--minimum-edge-bps", required=True)
    _ = parser.add_argument("--latency-buffer-bps", required=True)
    _ = parser.add_argument("--inventory-skew-bps", required=True)
    _ = parser.add_argument("--max-basis-bps", required=True)
    _ = parser.add_argument("--maximum-feed-age-ms", type=int, default=2_000)
    _ = parser.add_argument("--maximum-book-age-ms", type=int, default=1_000)
    _ = parser.add_argument("--maximum-pair-skew-ms", type=int, default=1_000)
    _ = parser.add_argument("--requote-interval-ms", type=int, default=500)
    _ = parser.add_argument(
        "--minimum-order-rest-ms",
        type=int,
        default=5_000,
        help="Minimum healthy quote lifetime before replacement (default: 5000).",
    )
    _ = parser.add_argument("--basis-window-seconds", type=int, default=300)
    _ = parser.add_argument("--basis-samples", type=int, default=3)
    _ = parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Stop a non-trading preview after this many seconds (0 means continuous).",
    )
    _ = parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicitly select the default non-trading preview mode.",
    )
    _ = parser.add_argument(
        "--submit",
        action="store_true",
        help="Submit real orders; dry-run is the default.",
    )
    _ = parser.add_argument(
        "--mainnet",
        action="store_true",
        help="Select Arcus mainnet; requires --submit in addition to this flag.",
    )
    _ = parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    args: MakerArguments = parser.parse_args(argv, namespace=MakerArguments())

    if args.submit and args.dry_run:
        raise InputError("choose either --submit or --dry-run")
    if args.mainnet and not args.submit:
        raise InputError("--mainnet requires --submit")
    if args.duration_seconds < 0 or (args.submit and args.duration_seconds > 0):
        raise InputError("--duration-seconds is only allowed for non-trading previews")
    if args.account_index not in range(10):
        raise InputError("--account-index must be between 0 and 9")
    if min(
        args.maximum_feed_age_ms,
        args.maximum_book_age_ms,
        args.maximum_pair_skew_ms,
        args.requote_interval_ms,
        args.minimum_order_rest_ms,
        args.basis_window_seconds,
        args.basis_samples,
    ) <= 0:
        raise InputError("freshness, requote, and basis limits must be positive")

    requested = tuple(symbol.strip().upper() for symbol in args.markets.split(","))
    if not requested or len(requested) != len(set(requested)):
        raise InputError("--markets must contain unique supported market symbols")
    mappings: list[MarketMapping] = []
    for symbol in requested:
        mapping = MARKET_MAPPINGS.get(symbol)
        if mapping is None:
            raise InputError(f"unsupported Arcus market {symbol!r}; choose BTC-USD or ETH-USD")
        mappings.append(mapping)

    order_size = _decimal(args.order_size_usd, "--order-size-usd")
    maximum_position = _decimal(args.max_position_usd, "--max-position-usd")
    if order_size <= 0 or maximum_position <= 0:
        raise InputError("order size and maximum position must be positive")
    if order_size > maximum_position:
        raise InputError("--order-size-usd cannot exceed --max-position-usd")
    quote_config = MakerQuoteConfig(
        order_size_usd=order_size,
        maximum_position_usd=maximum_position,
        maker_fee_bps=_decimal(args.maker_fee_bps, "--maker-fee-bps", allow_negative=True),
        minimum_edge_bps=_decimal(args.minimum_edge_bps, "--minimum-edge-bps"),
        latency_buffer_bps=_decimal(args.latency_buffer_bps, "--latency-buffer-bps"),
        inventory_skew_bps=_decimal(args.inventory_skew_bps, "--inventory-skew-bps"),
    )
    maximum_basis = _decimal(args.max_basis_bps, "--max-basis-bps")
    if maximum_basis <= 0:
        raise InputError("--max-basis-bps must be positive")

    account_address = _ZERO_ADDRESS
    signing_key: str | None = None
    if args.submit:
        address = args.account_address.removeprefix("0x").removeprefix("0X")
        if len(address) != 40 or any(character not in _HEX for character in address):
            raise InputError("--account-address must contain exactly 40 hexadecimal digits")
        account_address = f"0x{address.lower()}"
        signing_key = os.environ.get("ARCUS_API_SIGNING_KEY")
        if not signing_key:
            raise InputError("set ARCUS_API_SIGNING_KEY before using --submit")

    return MakerOptions(
        markets=tuple(mappings),
        account_address=account_address,
        account_index=args.account_index,
        quote_config=quote_config,
        maximum_basis_bps=maximum_basis,
        maximum_feed_age_ms=args.maximum_feed_age_ms,
        maximum_book_age_ms=args.maximum_book_age_ms,
        maximum_pair_skew_ms=args.maximum_pair_skew_ms,
        requote_interval_ms=args.requote_interval_ms,
        minimum_order_rest_ms=args.minimum_order_rest_ms,
        basis_window_seconds=args.basis_window_seconds,
        basis_samples=args.basis_samples,
        duration_seconds=args.duration_seconds,
        submit=args.submit,
        mainnet=args.mainnet,
        signing_key=signing_key,
        log_level=args.log_level,
    )
