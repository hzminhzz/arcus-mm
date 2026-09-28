"""Pure fair-price and inventory-aware quote calculations."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Literal

from .market import MarketInfo
from arcus_bot.types import InputError, ProtocolError

_BPS: Decimal = Decimal(10_000)
type Side = Literal["BUY", "SELL"]


@dataclass(frozen=True, slots=True)
class BasisSample:
    """Latest local-minus-reference midpoint sample."""

    offset: Decimal
    sampled_at_ns: int


class BasisEstimator:
    """Bounded rolling median of the Arcus/Binance midpoint basis."""

    window_ns: int
    maximum_basis_bps: Decimal
    minimum_samples: int
    samples: deque[BasisSample]
    last_sample_second: int

    def __init__(
        self,
        window_ns: int,
        maximum_basis_bps: Decimal,
        minimum_samples: int,
    ) -> None:
        if window_ns <= 0 or maximum_basis_bps <= 0 or minimum_samples <= 0:
            raise InputError("basis window, maximum basis, and sample count must be positive")
        self.window_ns = window_ns
        self.maximum_basis_bps = maximum_basis_bps
        self.minimum_samples = minimum_samples
        self.samples = deque[BasisSample]()
        self.last_sample_second = -1

    def add_sample(
        self,
        local_mid: Decimal,
        reference_mid: Decimal,
        sampled_at_ns: int,
    ) -> None:
        """Record at most one fresh basis sample per second."""
        if local_mid <= 0 or reference_mid <= 0:
            raise ProtocolError("cannot estimate basis from a non-positive midpoint")
        second = sampled_at_ns // 1_000_000_000
        if second <= self.last_sample_second:
            self._prune(sampled_at_ns)
            return
        offset = local_mid - reference_mid
        if abs(offset) * _BPS / reference_mid > self.maximum_basis_bps:
            self._prune(sampled_at_ns)
            return
        self.samples.append(BasisSample(offset=offset, sampled_at_ns=sampled_at_ns))
        self.last_sample_second = second
        self._prune(sampled_at_ns)

    def _prune(self, now_ns: int) -> None:
        cutoff = now_ns - self.window_ns
        while self.samples and self.samples[0].sampled_at_ns < cutoff:
            _ = self.samples.popleft()

    def median_offset(self, now_ns: int) -> Decimal | None:
        """Return the rolling median only after the configured warm-up."""
        self._prune(now_ns)
        if len(self.samples) < self.minimum_samples:
            return None
        offsets = sorted(sample.offset for sample in self.samples)
        middle = len(offsets) // 2
        if len(offsets) % 2:
            return offsets[middle]
        return (offsets[middle - 1] + offsets[middle]) / 2

    def fair_price(self, reference_mid: Decimal, now_ns: int) -> Decimal | None:
        """Apply the validated median basis to the latest reference midpoint."""
        offset = self.median_offset(now_ns)
        if offset is None:
            return None
        if reference_mid <= 0:
            raise ProtocolError("Binance reference midpoint must be positive")
        if abs(offset) * _BPS / reference_mid > self.maximum_basis_bps:
            return None
        return reference_mid + offset


@dataclass(frozen=True, slots=True)
class MakerQuoteConfig:
    """User-supplied quote economics and position limits."""

    order_size_usd: Decimal
    maximum_position_usd: Decimal
    maker_fee_bps: Decimal
    minimum_edge_bps: Decimal
    latency_buffer_bps: Decimal
    inventory_skew_bps: Decimal

    def __post_init__(self) -> None:
        if self.order_size_usd <= 0 or self.maximum_position_usd <= 0:
            raise InputError("order size and maximum position must be positive")
        if self.minimum_edge_bps < 0 or self.latency_buffer_bps < 0:
            raise InputError("minimum edge and latency buffer cannot be negative")
        if self.inventory_skew_bps < 0:
            raise InputError("inventory skew cannot be negative")


@dataclass(frozen=True, slots=True)
class Quote:
    """One tick-aligned post-only Arcus limit order."""

    side: Side
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class QuoteContext:
    """All validated inputs required for pure quote generation."""

    market: MarketInfo
    config: MakerQuoteConfig
    fair_price: Decimal
    best_bid: Decimal
    best_ask: Decimal
    position: Decimal


def _align(price: Decimal, tick: Decimal, rounding: str) -> Decimal:
    """Round a positive price to the selected market tick."""
    return (price / tick).to_integral_value(rounding=rounding) * tick


def _align_size(quantity: Decimal, step: Decimal) -> Decimal:
    """Round an order quantity down to the market step."""
    return (quantity / step).to_integral_value(rounding=ROUND_FLOOR) * step


def calculate_quotes(context: QuoteContext) -> tuple[Quote, ...]:
    """Create safe bid/ask candidates within fee, BBO, and inventory bounds."""
    market = context.market
    config = context.config
    fair = context.fair_price
    bid = context.best_bid
    ask = context.best_ask
    if fair <= 0 or bid <= 0 or ask <= bid:
        raise ProtocolError("quote inputs contain an invalid fair price or Arcus BBO")

    fee_cover_bps = config.maker_fee_bps + config.minimum_edge_bps + config.latency_buffer_bps
    inventory_cap = config.maximum_position_usd / fair
    if inventory_cap <= 0:
        raise InputError("maximum position does not allow a positive quantity")
    inventory_ratio = max(Decimal(-1), min(Decimal(1), context.position / inventory_cap))
    center = fair * (1 - inventory_ratio * config.inventory_skew_bps / _BPS)
    if center <= 0:
        raise InputError("inventory skew would place the quote center at or below zero")
    half_spread_bps = max(fee_cover_bps, Decimal(0))
    half_spread = center * half_spread_bps / _BPS
    buy_price = center - half_spread
    sell_price = center + half_spread
    if buy_price <= 0:
        raise ProtocolError("calculated bid is non-positive")

    buy_tick = market.tick_size_for(buy_price)
    sell_tick = market.tick_size_for(sell_price)
    buy_price = _align(buy_price, buy_tick, ROUND_FLOOR)
    sell_price = _align(sell_price, sell_tick, ROUND_CEILING)
    safe_bid = ask - market.tick_size_for(ask)
    safe_ask = bid + market.tick_size_for(bid)
    buy_price = min(buy_price, _align(safe_bid, market.tick_size_for(safe_bid), ROUND_FLOOR))
    sell_price = max(sell_price, _align(safe_ask, market.tick_size_for(safe_ask), ROUND_CEILING))

    buy_room = max(Decimal(0), inventory_cap - context.position)
    sell_room = max(Decimal(0), inventory_cap + context.position)
    if context.position >= inventory_cap:
        buy_room = Decimal(0)
        sell_room = min(sell_room, context.position)
    elif context.position <= -inventory_cap:
        sell_room = Decimal(0)
        buy_room = min(buy_room, -context.position)

    desired_quantity = config.order_size_usd / fair
    candidates: tuple[tuple[Side, Decimal, Decimal], ...] = (
        ("BUY", buy_price, min(desired_quantity, buy_room)),
        ("SELL", sell_price, min(desired_quantity, sell_room)),
    )
    quotes: list[Quote] = []
    for side, price, quantity in candidates:
        aligned_quantity = _align_size(quantity, market.mapping.step_size)
        if aligned_quantity > market.mapping.max_order_size:
            aligned_quantity = _align_size(
                market.mapping.max_order_size,
                market.mapping.step_size,
            )
        if aligned_quantity < market.mapping.min_order_size:
            continue
        if aligned_quantity * price < market.mapping.min_order_notional:
            continue
        match side:
            case "BUY":
                edge_bps = (fair - price) * _BPS / fair - config.maker_fee_bps
                is_passive = price < ask
            case "SELL":
                edge_bps = (price - fair) * _BPS / fair - config.maker_fee_bps
                is_passive = price > bid
        if edge_bps < config.minimum_edge_bps + config.latency_buffer_bps:
            continue
        if not is_passive:
            continue
        quotes.append(Quote(side=side, price=price, quantity=aligned_quantity))
    return tuple(quotes)
