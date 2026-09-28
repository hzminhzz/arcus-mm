"""Verified Arcus perpetual market mappings and live testnet metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import ClassVar, Final, Literal

import anyio
import websockets
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr, ValidationError

from arcus_bot.types import JSON_ADAPTER, JsonObject, JsonValue, ProtocolError

TESTNET_WS: Final = "wss://api.testnet.arcus.xyz/v1/ws"
MAINNET_WS: Final = "wss://api.arcus.xyz/v1/ws"


@dataclass(frozen=True, slots=True)
class TickTier:
    """Price increment below an optional exclusive upper price."""

    upper_price: Decimal | None
    tick_size: Decimal


@dataclass(frozen=True, slots=True)
class MarketMapping:
    """Explicit Arcus market to Binance symbol and verified increments."""

    market: str
    market_id: int
    base_asset: str
    binance_symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_order_size: Decimal
    min_order_notional: Decimal
    max_order_size: Decimal
    tick_tiers: tuple[TickTier, ...]


@dataclass(frozen=True, slots=True)
class MarketInfo:
    """Current, validated Arcus market metadata used by the strategy."""

    mapping: MarketMapping
    status: str

    def tick_size_for(self, price: Decimal) -> Decimal:
        """Return the Arcus price increment applicable at a candidate price."""
        for tier in self.mapping.tick_tiers:
            if tier.upper_price is None or price < tier.upper_price:
                return tier.tick_size
        raise ProtocolError(f"no Arcus tick tier covers {self.mapping.market} price {price}")


class _MarketRow(BaseModel):
    """Typed subset of an Arcus markets-channel row."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    marketDisplayName: StrictStr
    marketId: StrictInt
    status: StrictStr
    baseAsset: StrictStr
    quoteAsset: StrictStr
    type: StrictStr
    tickSize: StrictStr
    stepSize: StrictStr
    minOrderSize: StrictStr
    maxOrderSize: StrictStr


class _MarketsContents(BaseModel):
    """Typed global-market snapshot contents."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    markets: dict[str, JsonValue]


class _MarketsFrame(BaseModel):
    """Typed Arcus markets-channel envelope."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    channel: Literal["markets"]
    type: Literal["subscribed", "channel_data"]
    contents: _MarketsContents


MARKET_MAPPINGS: Final = {
    "BTC-USD": MarketMapping(
        market="BTC-USD",
        market_id=1,
        base_asset="BTC",
        binance_symbol="BTCUSDT",
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.00000001"),
        min_order_size=Decimal("0.0001"),
        min_order_notional=Decimal("5"),
        max_order_size=Decimal("10000"),
        tick_tiers=(
            TickTier(Decimal("500000"), Decimal("0.1")),
            TickTier(Decimal("1000000"), Decimal("0.2")),
            TickTier(Decimal("2000000"), Decimal("0.5")),
            TickTier(Decimal("5000000"), Decimal("1")),
            TickTier(Decimal("10000000"), Decimal("2")),
            TickTier(None, Decimal("5")),
        ),
    ),
    "ETH-USD": MarketMapping(
        market="ETH-USD",
        market_id=2,
        base_asset="ETH",
        binance_symbol="ETHUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.0000001"),
        min_order_size=Decimal("0.001"),
        min_order_notional=Decimal("5"),
        max_order_size=Decimal("100000"),
        tick_tiers=(
            TickTier(Decimal("10000"), Decimal("0.01")),
            TickTier(Decimal("20000"), Decimal("0.02")),
            TickTier(Decimal("50000"), Decimal("0.05")),
            TickTier(Decimal("100000"), Decimal("0.1")),
            TickTier(Decimal("200000"), Decimal("0.2")),
            TickTier(None, Decimal("0.5")),
        ),
    ),
}


def parse_market_snapshot(raw: str | bytes, mapping: MarketMapping) -> MarketInfo:
    """Parse a global Arcus markets snapshot and validate the requested mapping."""
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        frame = _MarketsFrame.model_validate_json(text)
    except ValidationError as error:
        raise ProtocolError(f"Arcus returned an invalid markets snapshot: {error}") from error
    if frame.type != "subscribed":
        raise ProtocolError("expected the initial Arcus markets snapshot")
    row_value = frame.contents.markets.get(str(mapping.market_id))
    if not isinstance(row_value, dict):
        raise ProtocolError(f"Arcus markets snapshot omitted {mapping.market}")
    try:
        row = _MarketRow.model_validate(row_value)
    except ValidationError as error:
        raise ProtocolError(
            f"Arcus market {mapping.market} has incomplete metadata: {error}"
        ) from error

    fields = {
        "marketDisplayName": (row.marketDisplayName, mapping.market),
        "marketId": (row.marketId, mapping.market_id),
        "baseAsset": (row.baseAsset, mapping.base_asset),
        "quoteAsset": (row.quoteAsset, "USD"),
        "type": (row.type, "PERPETUAL"),
    }
    for key, (value, expected) in fields.items():
        if value != expected:
            raise ProtocolError(
                f"Arcus market {mapping.market} has unexpected {key}: {value!r}"
            )
    if row.status != "ONLINE":
        raise ProtocolError(f"Arcus market {mapping.market} is not ONLINE")

    for key, value, expected in (
        ("tickSize", row.tickSize, mapping.tick_size),
        ("stepSize", row.stepSize, mapping.step_size),
        ("minOrderSize", row.minOrderSize, mapping.min_order_size),
        ("maxOrderSize", row.maxOrderSize, mapping.max_order_size),
    ):
        try:
            actual = Decimal(value)
        except InvalidOperation as error:
            raise ProtocolError(
                f"Arcus market {mapping.market} has invalid {key}: {value!r}"
            ) from error
        if not actual.is_finite() or actual != expected:
            raise ProtocolError(
                f"Arcus market {mapping.market} has unexpected {key}: {actual!r}"
            )
    return MarketInfo(mapping=mapping, status=row.status)


async def fetch_market_info(mapping: MarketMapping, mainnet: bool = False) -> MarketInfo:
    """Read current market metadata from the explicitly selected Arcus environment."""
    async with websockets.connect(
        MAINNET_WS if mainnet else TESTNET_WS,
        open_timeout=15,
        ping_interval=20,
        ping_timeout=20,
    ) as socket:
        await socket.send(json.dumps({"type": "subscribe", "channel": "markets"}))
        with anyio.fail_after(15):
            while True:
                raw = await socket.recv()
                text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
                try:
                    parsed = JSON_ADAPTER.validate_json(text)
                except ValidationError as error:
                    raise ProtocolError(f"Arcus returned invalid JSON: {error}") from error
                if not isinstance(parsed, dict):
                    raise ProtocolError("expected a JSON object from Arcus markets channel")
                message: JsonObject = parsed
                message_type = message.get("type")
                if message_type == "connected" or message_type == "channel_data":
                    continue
                if message_type == "subscribed" and message.get("channel") == "markets":
                    return parse_market_snapshot(text, mapping)
                if message_type == "error":
                    raise ProtocolError(f"Arcus rejected the markets subscription: {message}")
