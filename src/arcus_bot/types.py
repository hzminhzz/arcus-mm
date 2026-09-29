"""Shared typed values for Arcus account and order operations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from pydantic import TypeAdapter

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]
JSON_ADAPTER: Final[TypeAdapter[JsonValue]] = TypeAdapter[JsonValue](JsonValue)


class InputError(Exception):
    """A user-supplied value cannot form a valid strategy."""


class ProtocolError(Exception):
    """Arcus returned a message that violates the expected API contract."""


@dataclass(frozen=True, slots=True)
class AccountRef:
    """Identifies one Arcus subaccount and perpetual market."""

    address: str
    account_index: int
    market_id: int
    market: str


@dataclass(frozen=True, slots=True)
class Config:
    """Trading limits and quote settings for a bounded cycle run."""

    account: AccountRef
    side: str
    quantity: Decimal
    tick_size: Decimal
    step_size: Decimal
    take_profit_percent: Decimal
    max_order_notional: Decimal
    max_total_volume: Decimal
    cycles: int
    wait_seconds: int
    entry_timeout_seconds: int
    mainnet: bool


@dataclass(frozen=True, slots=True)
class OrderConfig:
    """Minimal per-market signing context for non-cycle strategies."""

    account: AccountRef
    tick_size: Decimal
    step_size: Decimal


@dataclass(frozen=True, slots=True)
class OrderRules:
    """Per-market signing increments, reduce-only flag, time-in-force, and client order ID."""

    tick_size: Decimal
    step_size: Decimal
    client_id: str | None = None
    reduce_only: bool = False
    time_in_force: str = "ALO"


@dataclass(frozen=True, slots=True)
class OrderState:
    """Latest order lifecycle state and aggregate execution data."""

    status: str
    filled_quantity: Decimal
    average_fill_price: Decimal
    side: str | None = None
    sequence_number: int = 0


@dataclass(frozen=True, slots=True)
class Fill:
    """An observed fill row for terminal monitoring."""

    trade_id: str
    order_id: str
    side: str
    price: Decimal
    size: Decimal
    fee: Decimal | None
    role: str | None

