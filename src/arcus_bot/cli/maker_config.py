"""Command-line parsing for the BTC/ETH continuous maker."""

from __future__ import annotations

import argparse
import hashlib
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from arcus_bot.bots.market_maker.market import MARKET_MAPPINGS, MarketMapping
from arcus_bot.bots.market_maker.quoter import MakerQuoteConfig
from arcus_bot.types import InputError, JsonObject, JSON_ADAPTER

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
    max_traded_notional_usd: str | None = None
    max_loss_usd: str | None = None
    candidate_mode: str = "off"
    max_alpha_bps: str = "0"
    alpha_report_path: str = ""
    reference_feed: str = "auto"
    preview_feeds: bool = False
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
    max_traded_notional_usd: Decimal | None
    max_loss_usd: Decimal | None
    submit: bool
    mainnet: bool
    signing_key: str | None
    log_level: str
    candidate_mode: str = "off"
    max_alpha_bps: Decimal = Decimal("0")
    alpha_report_path: str = ""
    alpha_report_data: JsonObject | None = None
    reference_feed: str = "auto"
    market_feeds: dict[str, tuple[str, str]] = field(default_factory=dict)
    preview_feeds: bool = False


MakerConfig = MakerOptions


def verify_authentic_go_report(
    report_path: str | Path | None = None,
    report_data: JsonObject | None = None,
    max_age_seconds: float = 86400.0,
) -> tuple[bool, str, JsonObject | None]:
    """Recompute a fresh replay from hash-bound inputs, never trust summary claims."""
    from arcus_bot.alpha.replay import evaluate_replay, load_jsonl

    now = time.time()
    if report_path and "fixture" in str(report_path).lower().replace("\\", "/"):
        return False, "fixture reports cannot certify authentic GO", None
    if report_data is None:
        if not report_path:
            return False, "no alpha evaluation report provided", None
        path = Path(report_path)
        if not path.is_file():
            return False, f"alpha report file not found: {report_path}", None
        try:
            age = now - path.stat().st_mtime
            if age < 0 or age > max_age_seconds:
                return False, "alpha report file is stale or future-dated", None
            parsed = JSON_ADAPTER.validate_json(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            return False, f"invalid alpha report: {exc}", None
        if not isinstance(parsed, dict):
            return False, "alpha report must be a JSON object", None
        data = parsed
    else:
        data = report_data

    if data.get("decision") != "GO":
        return False, f"alpha report decision is {data.get('decision')}: {data.get('reason', '')}", None
    generated = data.get("generated_at_ms")
    if isinstance(generated, bool) or not isinstance(generated, int) or not 0 <= now * 1000 - generated <= max_age_seconds * 1000:
        return False, "alpha report timestamp is missing, stale or future-dated", None
    provenance = data.get("data_provenance")
    if not isinstance(provenance, dict):
        return False, "missing source/data provenance", None
    try:
        inputs: list[list[JsonObject]] = []
        for name in ("public", "fills"):
            source = provenance.get(f"{name}_path")
            digest = provenance.get(f"{name}_sha256")
            if not isinstance(source, str) or not Path(source).is_absolute() or not isinstance(digest, str):
                return False, f"missing {name} source provenance", None
            path = Path(source)
            if "fixture" in str(path).lower().replace("\\", "/"):
                return False, "fixture inputs cannot certify authentic GO", None
            age = now - path.stat().st_mtime
            if age < 0 or age > max_age_seconds:
                return False, f"{name} input is stale or future-dated", None
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                return False, f"{name} input hash mismatch", None
            inputs.append(load_jsonl(path))
        horizon = data.get("action_horizon_ms")
        if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon <= 0:
            return False, "invalid action horizon", None
        replay = evaluate_replay(inputs[0], inputs[1], action_horizon_ms=horizon)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return False, f"invalid replay provenance: {exc}", None
    if replay["decision"] != "GO":
        return False, f"source replay decision is {replay['decision']}: {replay['reason']}", None
    if any(data.get(key) != value for key, value in replay.items()):
        return False, "report summary does not match source replay", None
    return True, "recomputed GO from fresh hash-bound observations", data


def parse_reference_feed(
    raw_feed: str,
    mappings: tuple[MarketMapping, ...],
) -> dict[str, tuple[str, str]]:
    """Resolve (feed_type, symbol) for each configured market mapping."""
    cleaned = raw_feed.strip().lower()
    feed_map: dict[str, tuple[str, str]] = {}
    if "=" in cleaned:
        overrides: dict[str, str] = {}
        for item in raw_feed.split(","):
            part = item.strip()
            if not part:
                continue
            if "=" not in part:
                raise InputError(f"invalid market=feed format in --reference-feed: {part!r}")
            mkt, feed = part.split("=", 1)
            mkt_clean = mkt.strip().upper()
            feed_clean = feed.strip().lower()
            if feed_clean not in ("binance", "hyperliquid", "auto"):
                raise InputError(
                    f"unsupported feed {feed_clean!r} for market {mkt_clean}; choose binance or hyperliquid"
                )
            overrides[mkt_clean] = feed_clean
        for mapping in mappings:
            target_feed = overrides.get(mapping.market, "auto")
            feed_type, feed_sym = mapping.resolve_feed(target_feed)
            feed_map[mapping.market] = (feed_type, feed_sym)
    else:
        if cleaned not in ("auto", "binance", "hyperliquid"):
            raise InputError(f"unsupported reference feed {raw_feed!r}; choose binance or hyperliquid")
        for mapping in mappings:
            target_feed = None if cleaned == "auto" else cleaned
            feed_type, feed_sym = mapping.resolve_feed(target_feed)
            feed_map[mapping.market] = (feed_type, feed_sym)
    return feed_map


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
            "Run the continuous Arcus maker. "
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
        help="Bound run duration in seconds; live runs require 1-300 seconds.",
    )
    _ = parser.add_argument(
        "--max-traded-notional-usd",
        default=None,
        help="Required for live runs; maximum cumulative gross fill notional in USD.",
    )
    _ = parser.add_argument(
        "--max-loss-usd",
        default=None,
        help="Required for live runs; stop quoting at this account-equity drawdown.",
    )
    _ = parser.add_argument(
        "--candidate-mode",
        choices=("off", "shadow", "bounded"),
        default="off",
        help="Alpha candidate evaluation mode (off, shadow, bounded; default: off).",
    )
    _ = parser.add_argument(
        "--max-alpha-bps",
        default="0",
        help="Maximum candidate alpha offset in basis points (default: 0).",
    )
    _ = parser.add_argument(
        "--alpha-report-path",
        default="",
        help="Path to authentic GO alpha evaluation report (required for bounded mode).",
    )
    _ = parser.add_argument(
        "--reference-feed",
        default="auto",
        help="Reference pricing feed engine (auto, binance, hyperliquid, or MARKET=feed; default: auto).",
    )
    _ = parser.add_argument(
        "--preview-feeds",
        action="store_true",
        help="Safe non-trading live preview of reference feeds and Arcus orderbooks.",
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
    if args.mainnet and not args.submit and not args.preview_feeds:
        raise InputError("--mainnet requires --submit")
    if args.duration_seconds < 0:
        raise InputError("--duration-seconds cannot be negative")
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
            supported = ", ".join(sorted(MARKET_MAPPINGS))
            raise InputError(f"unsupported Arcus market {symbol!r}; choose {supported}")
        mappings.append(mapping)

    order_size = _decimal(args.order_size_usd, "--order-size-usd")
    maximum_position = _decimal(args.max_position_usd, "--max-position-usd")
    if order_size <= 0 or maximum_position <= 0:
        raise InputError("order size and maximum position must be positive")
    if order_size > maximum_position:
        raise InputError("--order-size-usd cannot exceed --max-position-usd")
    maximum_traded_notional = (
        _decimal(args.max_traded_notional_usd, "--max-traded-notional-usd")
        if args.max_traded_notional_usd is not None
        else None
    )
    maximum_loss = (
        _decimal(args.max_loss_usd, "--max-loss-usd")
        if args.max_loss_usd is not None
        else None
    )
    if args.submit and maximum_traded_notional is not None and maximum_traded_notional <= 0:
        raise InputError("--max-traded-notional-usd must be positive")
    if args.submit and maximum_loss is not None:
        if maximum_loss <= 0:
            raise InputError("--max-loss-usd must be positive")
        if maximum_loss > maximum_position:
            raise InputError("--max-loss-usd cannot exceed --max-position-usd")
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

    candidate_mode = args.candidate_mode
    max_alpha_bps = _decimal(args.max_alpha_bps, "--max-alpha-bps")
    if max_alpha_bps < 0:
        raise InputError("--max-alpha-bps cannot be negative")

    alpha_report_path = args.alpha_report_path.strip()
    alpha_report_data: JsonObject | None = None
    if candidate_mode == "bounded":
        if not alpha_report_path:
            raise InputError(
                "bounded candidate mode requires a valid authentic GO evaluation report (--alpha-report-path)"
            )
        is_go, go_reason, alpha_report_data = verify_authentic_go_report(report_path=alpha_report_path)
        if not is_go:
            raise InputError(
                f"bounded candidate mode rejected: authentic GO verification failed: {go_reason}"
            )
    elif alpha_report_path:
        path = Path(alpha_report_path)
        if not path.is_file():
            raise InputError(f"--alpha-report-path file not found: {alpha_report_path}")

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

    market_feeds = parse_reference_feed(args.reference_feed, tuple(mappings))

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
        max_traded_notional_usd=maximum_traded_notional,
        max_loss_usd=maximum_loss,
        submit=args.submit,
        mainnet=args.mainnet,
        signing_key=signing_key,
        log_level=args.log_level,
        candidate_mode=candidate_mode,
        max_alpha_bps=max_alpha_bps,
        alpha_report_path=alpha_report_path,
        alpha_report_data=alpha_report_data,
        reference_feed=args.reference_feed,
        market_feeds=market_feeds,
        preview_feeds=args.preview_feeds,
    )
