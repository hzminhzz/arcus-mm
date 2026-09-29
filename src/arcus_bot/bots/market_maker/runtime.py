"""Typed contracts and per-market runtime settings."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Protocol

from arcus_bot.bots.market_maker.quoter import MakerQuoteConfig
from arcus_bot.sdk.account import AccountState
from arcus_bot.sdk.orderbook import OrderbookState
from arcus_bot.types import (
    AccountRef,
    InputError,
    JsonObject,
    JsonValue,
    OrderRules,
    OrderState,
)

MAXIMUM_ACCOUNT_AGE_MS: Final = 5_000


class MakerClient(Protocol):
    """Arcus capabilities required by one maker strategy session."""

    orderbook: OrderbookState
    state: AccountState

    async def subscribe(self, channel: str, identifier: str, **extra: JsonValue) -> None: ...

    async def subscribe_maker_channels(self) -> None: ...

    async def next_message(self) -> JsonObject: ...

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None: ...


class MakerOrderActions(Protocol):
    """Arcus order operations required by the quote manager."""

    async def place(
        self,
        side: str,
        price: Decimal,
        quantity: Decimal,
        rules: OrderRules | None = None,
    ) -> str: ...

    async def cancel(self, order_id: str) -> None: ...


class ReferenceFeed(Protocol):
    """External reference market stream (Binance or Hyperliquid)."""

    symbol: str
    feed_name: str
    latest: object

    async def run(self) -> None: ...


@dataclass(frozen=True, slots=True)
class MakerRuntime:
    """Validated inputs shared by one per-market maker session."""

    account: AccountRef
    quote_config: MakerQuoteConfig
    submit: bool
    signing_key: str | None
    run_id: str
    maximum_basis_bps: Decimal
    reference_feed: str = "binance"
    reference_symbol: str = ""
    basis_window_seconds: int = 300
    basis_minimum_samples: int = 3
    maximum_feed_age_ms: int = 2_000
    maximum_book_age_ms: int = 1_000
    maximum_pair_skew_ms: int = 1_000
    requote_interval_ms: int = 500
    minimum_order_rest_ms: int = 5_000
    duration_seconds: int = 0
    run_deadline_ns: int | None = None
    run_expiration_time_us: int | None = None
    max_traded_notional_usd: Decimal | None = None
    max_loss_usd: Decimal | None = None
    mainnet: bool = False
    candidate_mode: str = "off"
    max_alpha_bps: Decimal = Decimal("0")
    alpha_report_path: str = ""
    alpha_report_data: JsonObject | None = None
    emergency_flatten_ratio: Decimal = Decimal("1.20")
    emergency_flatten_buffer_bps: Decimal = Decimal("10")

    def __post_init__(self) -> None:
        if self.maximum_basis_bps <= 0:
            raise InputError("maximum basis must be positive")
        if self.emergency_flatten_ratio <= 1:
            raise InputError("emergency flatten ratio must be greater than 1.0")
        if self.emergency_flatten_buffer_bps < 0:
            raise InputError("emergency flatten buffer bps cannot be negative")
        if self.basis_window_seconds <= 0 or self.basis_minimum_samples <= 0:
            raise InputError("basis window and warm-up sample count must be positive")
        if min(
            self.maximum_feed_age_ms,
            self.maximum_book_age_ms,
            self.maximum_pair_skew_ms,
            self.requote_interval_ms,
            self.minimum_order_rest_ms,
        ) <= 0:
            raise InputError(
                "feed, book, skew, requote, and rest limits must be positive"
            )
        if self.duration_seconds < 0:
            raise InputError("run duration cannot be negative")
        if self.mainnet and not self.submit:
            raise InputError("mainnet requires the explicit --submit opt-in")
        if self.submit:
            if not self.signing_key:
                raise InputError("live Arcus submission requires ARCUS_API_SIGNING_KEY")
            if self.duration_seconds < 0:
                raise InputError("run duration cannot be negative")
            if (
                self.max_traded_notional_usd is not None
                and self.max_traded_notional_usd <= 0
            ):
                raise InputError("live traded-notional cap must be positive")
            if self.max_loss_usd is not None:
                if self.max_loss_usd <= 0:
                    raise InputError("live loss limit must be positive")
                if self.max_loss_usd > self.quote_config.maximum_position_usd:
                    raise InputError("live loss limit cannot exceed the maximum position")
