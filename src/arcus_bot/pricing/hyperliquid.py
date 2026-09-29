"""Hyperliquid WebSocket l2Book and oracle reference price feed."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from time import monotonic_ns
from typing import ClassVar, Final, Literal

import anyio
import websockets
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr, ValidationError
from websockets.asyncio.client import ClientConnection

from arcus_bot.types import ProtocolError

logger = logging.getLogger(__name__)

HYPERLIQUID_MAINNET_WS: Final = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_TESTNET_WS: Final = "wss://api.hyperliquid-testnet.xyz/ws"
_MAX_RECONNECT_SECONDS: Final = 30
_PING_INTERVAL_SECONDS: Final = 20
_MAX_ORACLE_DIVERGENCE: Final = Decimal("0.20")


def normalize_hyperliquid_symbol(symbol: str) -> str:
    """Normalize symbol ensuring lowercase deployer prefix (e.g. xyz:NVDA) or uppercase coin."""
    cleaned = symbol.strip()
    if ":" in cleaned:
        prefix, name = cleaned.split(":", 1)
        return f"{prefix.lower()}:{name.upper()}"
    return cleaned.upper()


@dataclass(frozen=True, slots=True)
class HyperliquidBookTicker:
    """Validated Hyperliquid BBO, top-of-book quantities, oracle price, and local receive time."""

    symbol: str
    bid: Decimal
    ask: Decimal
    received_at_ns: int
    bid_qty: Decimal | None = None
    ask_qty: Decimal | None = None
    oracle_px: Decimal | None = None
    server_time_ms: int | None = None

    @property
    def mid(self) -> Decimal:
        """Return the best-bid/ask midpoint."""
        return (self.bid + self.ask) / 2

    @property
    def fair_anchor(self) -> Decimal:
        """Return the oracle reference price as fair anchor when available, else midpoint."""
        if (
            self.oracle_px is not None
            and self.oracle_px > 0
            and self.oracle_px.is_finite()
        ):
            return self.oracle_px
        return self.mid


class _LevelEntry(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    px: StrictStr
    sz: StrictStr
    n: StrictInt | None = None


class _L2BookData(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    coin: StrictStr
    time: StrictInt
    levels: list[list[_LevelEntry]]


class _L2BookFrame(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    channel: Literal["l2Book"]
    data: _L2BookData


class _ActiveAssetCtx(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    oraclePx: StrictStr | None = None
    markPx: StrictStr | None = None
    midPx: StrictStr | None = None


class _ActiveAssetCtxData(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    coin: StrictStr
    ctx: _ActiveAssetCtx


class _ActiveAssetCtxFrame(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    channel: Literal["activeAssetCtx"]
    data: _ActiveAssetCtxData


def parse_l2_book(
    raw: str | bytes,
    expected_symbol: str,
    received_at_ns: int,
    known_oracle_px: Decimal | None = None,
) -> HyperliquidBookTicker:
    """Parse one Hyperliquid l2Book event and validate positive, finite, uncrossed BBO."""
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        frame = _L2BookFrame.model_validate_json(text)
    except ValidationError as error:
        raise ProtocolError(f"Hyperliquid returned invalid l2Book event: {error}") from error

    expected_norm = normalize_hyperliquid_symbol(expected_symbol)
    frame_coin_norm = normalize_hyperliquid_symbol(frame.data.coin)
    if frame_coin_norm != expected_norm:
        raise ProtocolError(
            f"Hyperliquid l2Book symbol {frame.data.coin} does not match expected {expected_symbol}"
        )

    levels = frame.data.levels
    if len(levels) < 2 or not levels[0] or not levels[1]:
        raise ProtocolError(f"Hyperliquid l2Book for {expected_symbol} has empty book levels")

    top_bid = levels[0][0]
    top_ask = levels[1][0]

    try:
        bid = Decimal(top_bid.px)
        ask = Decimal(top_ask.px)
        bid_qty = Decimal(top_bid.sz)
        ask_qty = Decimal(top_ask.sz)
    except InvalidOperation as error:
        raise ProtocolError(
            f"Hyperliquid l2Book price or quantity is not a valid decimal: {error}"
        ) from error

    if not bid.is_finite() or not ask.is_finite() or bid <= 0 or ask <= bid:
        raise ProtocolError(
            f"Hyperliquid l2Book prices must be finite, positive, and uncrossed (bid={bid}, ask={ask})"
        )

    if not bid_qty.is_finite() or not ask_qty.is_finite() or bid_qty <= 0 or ask_qty <= 0:
        raise ProtocolError(
            f"Hyperliquid l2Book quantities must be finite and positive (bid_qty={bid_qty}, ask_qty={ask_qty})"
        )

    mid = (bid + ask) / 2
    if known_oracle_px is not None and known_oracle_px > 0 and known_oracle_px.is_finite():
        divergence = abs(mid - known_oracle_px) / mid
        if divergence > _MAX_ORACLE_DIVERGENCE:
            raise ProtocolError(
                f"Hyperliquid book midpoint {mid} diverges from oracle {known_oracle_px} by {divergence:.2%}"
            )

    return HyperliquidBookTicker(
        symbol=expected_norm,
        bid=bid,
        ask=ask,
        received_at_ns=received_at_ns,
        bid_qty=bid_qty,
        ask_qty=ask_qty,
        oracle_px=known_oracle_px,
        server_time_ms=frame.data.time,
    )


def parse_active_asset_ctx(
    raw: str | bytes,
    expected_symbol: str,
) -> Decimal | None:
    """Parse one Hyperliquid activeAssetCtx event and extract the validated oracle price."""
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        frame = _ActiveAssetCtxFrame.model_validate_json(text)
    except ValidationError:
        return None

    expected_norm = normalize_hyperliquid_symbol(expected_symbol)
    frame_coin_norm = normalize_hyperliquid_symbol(frame.data.coin)
    if frame_coin_norm != expected_norm:
        return None

    raw_oracle = frame.data.ctx.oraclePx
    if raw_oracle is None:
        return None

    try:
        oracle = Decimal(raw_oracle)
    except InvalidOperation:
        return None

    if not oracle.is_finite() or oracle <= 0:
        return None

    return oracle


@dataclass(slots=True)
class HyperliquidBookTickerFeed:
    """Reconnect and cache one Hyperliquid l2Book and oracle reference stream."""

    feed_name: ClassVar[str] = "Hyperliquid"
    symbol: str
    ws_url: str = HYPERLIQUID_MAINNET_WS
    latest: HyperliquidBookTicker | None = None
    known_oracle_px: Decimal | None = None

    def __init__(
        self,
        symbol: str,
        testnet: bool = False,
        ws_url: str | None = None,
    ) -> None:
        self.symbol = normalize_hyperliquid_symbol(symbol)
        self.latest = None
        self.known_oracle_px = None
        if ws_url is not None:
            self.ws_url = ws_url
        else:
            self.ws_url = HYPERLIQUID_TESTNET_WS if testnet else HYPERLIQUID_MAINNET_WS

    async def run(self) -> None:
        """Connect to Hyperliquid WS, subscribe to l2Book and activeAssetCtx, and stream updates."""
        delay = 1
        while True:
            try:
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=10,
                ) as socket:
                    connected_at_ns = monotonic_ns()
                    await self._subscribe(socket)
                    async with anyio.create_task_group() as tg:
                        _ = tg.start_soon(self._ping_loop, socket)
                        await self._read_stream(socket)
                    if monotonic_ns() - connected_at_ns >= 10_000_000_000:
                        delay = 1
            except (OSError, TimeoutError, websockets.WebSocketException) as error:
                self.latest = None
                logger.warning("Hyperliquid feed disconnected for %s: %s", self.symbol, error)
            await anyio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_SECONDS)

    async def _subscribe(self, socket: ClientConnection) -> None:
        """Send l2Book and activeAssetCtx subscriptions for this symbol."""
        sub_book = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": self.symbol},
        }
        await socket.send(json.dumps(sub_book))
        sub_ctx = {
            "method": "subscribe",
            "subscription": {"type": "activeAssetCtx", "coin": self.symbol},
        }
        await socket.send(json.dumps(sub_ctx))

    async def _ping_loop(self, socket: ClientConnection) -> None:
        """Periodically send Hyperliquid application ping frames."""
        ping_msg = json.dumps({"method": "ping"})
        while True:
            await anyio.sleep(_PING_INTERVAL_SECONDS)
            await socket.send(ping_msg)

    async def _read_stream(self, socket: ClientConnection) -> None:
        """Read incoming frames and update latest ticker or oracle price."""
        async for raw in socket:
            now_ns = monotonic_ns()
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            if '"channel":"l2Book"' in text:
                try:
                    self.latest = parse_l2_book(
                        raw,
                        self.symbol,
                        now_ns,
                        known_oracle_px=self.known_oracle_px,
                    )
                except (ProtocolError, UnicodeDecodeError) as error:
                    self.latest = None
                    logger.warning(
                        "Ignoring invalid Hyperliquid l2Book for %s: %s",
                        self.symbol,
                        error,
                    )
            elif '"channel":"activeAssetCtx"' in text:
                oracle = parse_active_asset_ctx(raw, self.symbol)
                if oracle is not None:
                    self.known_oracle_px = oracle
                    if self.latest is not None:
                        mid = self.latest.mid
                        divergence = abs(mid - oracle) / mid
                        if divergence <= _MAX_ORACLE_DIVERGENCE:
                            self.latest = HyperliquidBookTicker(
                                symbol=self.latest.symbol,
                                bid=self.latest.bid,
                                ask=self.latest.ask,
                                received_at_ns=self.latest.received_at_ns,
                                bid_qty=self.latest.bid_qty,
                                ask_qty=self.latest.ask_qty,
                                oracle_px=oracle,
                                server_time_ms=self.latest.server_time_ms,
                            )
                        else:
                            self.latest = None
                            logger.warning(
                                "Hyperliquid oracle %s diverged from book mid %s for %s",
                                oracle,
                                mid,
                                self.symbol,
                            )
