"""Binance USD-M Futures book-ticker feed."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from time import monotonic_ns
from typing import ClassVar, Final

import anyio
import websockets
from pydantic import BaseModel, ConfigDict, StrictStr, ValidationError, model_validator
from websockets.asyncio.client import ClientConnection

from arcus_bot.types import ProtocolError

logger = logging.getLogger(__name__)

BINANCE_USDM_PUBLIC_WS: Final = "wss://fstream.binance.com/public/ws"
_MAX_RECONNECT_SECONDS: Final = 30


@dataclass(frozen=True, slots=True)
class BinanceBookTicker:
    """Validated BBO, top-of-book quantities, and local monotonic receive time."""

    symbol: str
    bid: Decimal
    ask: Decimal
    received_at_ns: int
    bid_qty: Decimal | None = None
    ask_qty: Decimal | None = None

    @property
    def mid(self) -> Decimal:
        """Return the best-bid/ask midpoint."""
        return (self.bid + self.ask) / 2

    @property
    def fair_anchor(self) -> Decimal:
        """Return the fair-value anchor price."""
        return self.mid


class _BookTickerFrame(BaseModel):
    """Validated raw Binance bookTicker payload."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    s: StrictStr | None = None
    b: StrictStr
    a: StrictStr
    B: StrictStr | None = None
    A: StrictStr | None = None

    @model_validator(mode="after")
    def validate_prices(self) -> _BookTickerFrame:
        """Reject non-positive, non-finite, or crossed BBO data."""
        try:
            bid = Decimal(self.b)
            ask = Decimal(self.a)
        except InvalidOperation as error:
            raise ValueError("Binance bookTicker prices must be decimal strings") from error
        if not bid.is_finite() or not ask.is_finite() or bid <= 0 or ask <= bid:
            raise ValueError("Binance bookTicker prices must be finite, positive, and uncrossed")
        return self


def _parse_quantity(raw_qty: str | None) -> Decimal | None:
    """Parse wire quantity field into positive finite Decimal or None."""
    if raw_qty is None:
        return None
    try:
        qty = Decimal(raw_qty)
    except InvalidOperation:
        return None
    if not qty.is_finite() or qty <= 0:
        return None
    return qty


def parse_book_ticker(
    raw: str | bytes,
    expected_symbol: str,
    received_at_ns: int,
) -> BinanceBookTicker:
    """Parse one Binance bookTicker event and validate its symbol and BBO."""
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        frame = _BookTickerFrame.model_validate_json(text)
    except ValidationError as error:
        raise ProtocolError("Binance returned an invalid bookTicker event") from error
    symbol = expected_symbol.upper()
    if frame.s is not None and frame.s != symbol:
        raise ProtocolError(
            f"Binance bookTicker symbol {frame.s} does not match configured {symbol}"
        )
    return BinanceBookTicker(
        symbol=symbol,
        bid=Decimal(frame.b),
        ask=Decimal(frame.a),
        received_at_ns=received_at_ns,
        bid_qty=_parse_quantity(frame.B),
        ask_qty=_parse_quantity(frame.A),
    )


@dataclass(slots=True)
class BinanceBookTickerFeed:
    """Reconnect and cache one explicitly configured Binance BBO stream."""

    feed_name: ClassVar[str] = "Binance"
    symbol: str
    latest: BinanceBookTicker | None

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self.latest = None

    async def run(self) -> None:
        """Read public futures BBO updates and reconnect with bounded backoff."""
        delay = 1
        url = f"{BINANCE_USDM_PUBLIC_WS}/{self.symbol.lower()}@bookTicker"
        while True:
            try:
                async with websockets.connect(
                    url,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=10,
                ) as socket:
                    connected_at_ns = monotonic_ns()
                    await self._read_stream(socket)
                    if monotonic_ns() - connected_at_ns >= 10_000_000_000:
                        delay = 1
            except (OSError, TimeoutError, websockets.WebSocketException) as error:
                self.latest = None
                logger.warning("Binance feed disconnected for %s: %s", self.symbol, error)
            await anyio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_SECONDS)

    async def _read_stream(self, socket: ClientConnection) -> None:
        """Update the latest BBO, clearing it when a malformed frame arrives."""
        async for raw in socket:
            try:
                self.latest = parse_book_ticker(raw, self.symbol, monotonic_ns())
            except (ProtocolError, UnicodeDecodeError):
                self.latest = None
                logger.warning("Ignoring invalid Binance bookTicker for %s", self.symbol)
