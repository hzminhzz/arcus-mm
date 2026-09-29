from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import ClassVar, Final

import websockets
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr, ValidationError
from websockets.asyncio.client import ClientConnection

from arcus_bot.bots.market_maker.market import MARKET_MAPPINGS

logger: Final = logging.getLogger(__name__)

DEFAULT_MARKETS: Final = "BTC-USD,ETH-USD"
DEFAULT_DURATION_SECONDS: Final = 12.0
DEFAULT_MAX_BYTES: Final = 1048576
DEFAULT_OUTPUT_PATH: Final = ".omo/evidence/arcus-maker-alpha/public.jsonl"
DEFAULT_ARCUS_WS: Final = "wss://api.testnet.arcus.xyz/v1/ws"
DEFAULT_BINANCE_WS_BASE: Final = "wss://fstream.binance.com/public/ws"


@dataclass(frozen=True, slots=True)
class PublicEvent:
    recv_time_ns: int
    event_time_ms: int
    symbol: str
    b: str
    B: str
    a: str
    A: str
    venue: str

    def to_dict(self) -> dict[str, str | int]:
        return {
            "recv_time_ns": self.recv_time_ns,
            "event_time_ms": self.event_time_ms,
            "symbol": self.symbol,
            "b": self.b,
            "B": self.B,
            "a": self.a,
            "A": self.A,
            "venue": self.venue,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))


class _BinanceFrame(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    s: StrictStr
    b: StrictStr
    a: StrictStr
    B: StrictStr
    A: StrictStr
    E: StrictInt | None = None
    T: StrictInt | None = None


class _ArcusContents(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    bids: list[list[StrictStr]]
    asks: list[list[StrictStr]]
    timestamp: StrictInt | None = None


class _ArcusFrame(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")

    type: StrictStr
    channel: StrictStr
    id: StrictStr
    contents: _ArcusContents


def parse_binance_book_ticker(
    raw: str | bytes,
    recv_time_ns: int | None = None,
) -> PublicEvent | None:
    current_recv_ns = time.monotonic_ns() if recv_time_ns is None else recv_time_ns

    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        frame = _BinanceFrame.model_validate_json(text)
    except (ValidationError, UnicodeDecodeError, ValueError):
        return None

    try:
        bid = Decimal(frame.b)
        ask = Decimal(frame.a)
        bid_qty = Decimal(frame.B)
        ask_qty = Decimal(frame.A)
    except (InvalidOperation, ValueError):
        return None

    if not (bid.is_finite() and ask.is_finite() and bid_qty.is_finite() and ask_qty.is_finite()):
        return None

    if bid <= 0 or ask <= 0 or bid_qty <= 0 or ask_qty <= 0 or bid >= ask:
        return None

    raw_event_time = frame.E if frame.E is not None else frame.T
    event_time_ms = int(raw_event_time) if raw_event_time is not None else int(time.time() * 1000)

    return PublicEvent(
        recv_time_ns=current_recv_ns,
        event_time_ms=event_time_ms,
        symbol=frame.s.upper(),
        b=frame.b,
        B=frame.B,
        a=frame.a,
        A=frame.A,
        venue="binance",
    )


def parse_arcus_l2_frame(
    raw: str | bytes,
    recv_time_ns: int | None = None,
) -> PublicEvent | None:
    current_recv_ns = time.monotonic_ns() if recv_time_ns is None else recv_time_ns

    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        frame = _ArcusFrame.model_validate_json(text)
    except (ValidationError, UnicodeDecodeError, ValueError):
        return None

    if frame.type not in ("subscribed", "channel_data") or frame.channel != "l2Orderbook":
        return None

    if len(frame.contents.bids) == 0 or len(frame.contents.asks) == 0:
        return None

    top_bid = frame.contents.bids[0]
    top_ask = frame.contents.asks[0]
    if len(top_bid) < 2 or len(top_ask) < 2:
        return None

    b_val = top_bid[0]
    B_val = top_bid[1]
    a_val = top_ask[0]
    A_val = top_ask[1]

    try:
        bid = Decimal(b_val)
        ask = Decimal(a_val)
        bid_qty = Decimal(B_val)
        ask_qty = Decimal(A_val)
    except (InvalidOperation, ValueError):
        return None

    if not (bid.is_finite() and ask.is_finite() and bid_qty.is_finite() and ask_qty.is_finite()):
        return None

    if bid <= 0 or ask <= 0 or bid_qty <= 0 or ask_qty <= 0 or bid >= ask:
        return None

    raw_timestamp = frame.contents.timestamp
    if raw_timestamp is not None and raw_timestamp > 0:
        event_time_ms = int(raw_timestamp // 1000) if raw_timestamp > 10**14 else int(raw_timestamp)
    else:
        event_time_ms = int(time.time() * 1000)

    return PublicEvent(
        recv_time_ns=current_recv_ns,
        event_time_ms=event_time_ms,
        symbol=frame.id,
        b=b_val,
        B=B_val,
        a=a_val,
        A=A_val,
        venue="arcus",
    )


def resolve_binance_symbol(market: str) -> str:
    normalized = market.strip().upper()
    if normalized in MARKET_MAPPINGS:
        return MARKET_MAPPINGS[normalized].binance_symbol

    cleaned = normalized.replace("-", "").replace("/", "")
    if cleaned.endswith("USD"):
        return cleaned + "T"
    if not cleaned.endswith("USDT"):
        return cleaned + "USDT"
    return cleaned


@dataclass(slots=True)
class RecordArguments(argparse.Namespace):
    markets: str = DEFAULT_MARKETS
    duration_seconds: float = DEFAULT_DURATION_SECONDS
    max_bytes: int = DEFAULT_MAX_BYTES
    output: str = DEFAULT_OUTPUT_PATH
    arcus_ws: str = DEFAULT_ARCUS_WS
    binance_ws_base: str = DEFAULT_BINANCE_WS_BASE


class PublicDataRecorder:
    markets: list[str]
    duration_seconds: float
    max_bytes: int
    output_path: Path
    arcus_ws: str | None
    binance_ws_base: str

    bytes_written: int
    events_recorded: int

    def __init__(
        self,
        markets: list[str],
        duration_seconds: float = DEFAULT_DURATION_SECONDS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        output_path: Path | str = DEFAULT_OUTPUT_PATH,
        arcus_ws: str | None = DEFAULT_ARCUS_WS,
        binance_ws_base: str = DEFAULT_BINANCE_WS_BASE,
    ) -> None:
        self.markets = [m.strip() for m in markets if m.strip()]
        self.duration_seconds = max(0.0, float(duration_seconds))
        self.max_bytes = max(0, int(max_bytes))
        self.output_path = Path(output_path)
        self.arcus_ws = arcus_ws
        self.binance_ws_base = binance_ws_base.rstrip("/")

        self.bytes_written = 0
        self.events_recorded = 0

    async def _consume_binance(
        self,
        symbol: str,
        queue: asyncio.Queue[PublicEvent],
        stop_event: asyncio.Event,
        active_sockets: set[ClientConnection],
    ) -> None:
        url = f"{self.binance_ws_base}/{symbol.lower()}@bookTicker"
        reconnect_delay = 1.0
        while not stop_event.is_set():
            ws: ClientConnection | None = None
            try:
                async with websockets.connect(
                    url,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=1.0,
                ) as socket:
                    ws = socket
                    _ = active_sockets.add(socket)
                    reconnect_delay = 1.0
                    while not stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(socket.recv(), timeout=2.0)
                        except TimeoutError:
                            continue
                        recv_ns = time.monotonic_ns()
                        event = parse_binance_book_ticker(msg, recv_time_ns=recv_ns)
                        if event is not None:
                            await queue.put(event)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if stop_event.is_set():
                    break
                logger.debug("Binance connection error for %s: %s", symbol, exc)
                try:
                    _ = await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
                    break
                except TimeoutError:
                    reconnect_delay = min(reconnect_delay * 2, 10.0)
            finally:
                if ws is not None:
                    _ = active_sockets.discard(ws)
                    try:
                        await ws.close()
                    except Exception:
                        pass

    async def _consume_arcus(
        self,
        markets: list[str],
        queue: asyncio.Queue[PublicEvent],
        stop_event: asyncio.Event,
        active_sockets: set[ClientConnection],
    ) -> None:
        if not self.arcus_ws:
            return

        reconnect_delay = 1.0
        while not stop_event.is_set():
            ws: ClientConnection | None = None
            try:
                async with websockets.connect(
                    self.arcus_ws,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=1.0,
                ) as socket:
                    ws = socket
                    _ = active_sockets.add(socket)
                    reconnect_delay = 1.0

                    for m in markets:
                        sub_payload = json.dumps(
                            {"type": "subscribe", "channel": "l2Orderbook", "id": m},
                            separators=(",", ":"),
                        )
                        await socket.send(sub_payload)

                    while not stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(socket.recv(), timeout=2.0)
                        except TimeoutError:
                            continue
                        recv_ns = time.monotonic_ns()
                        event = parse_arcus_l2_frame(msg, recv_time_ns=recv_ns)
                        if event is not None:
                            await queue.put(event)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if stop_event.is_set():
                    break
                logger.debug("Arcus connection error: %s", exc)
                try:
                    _ = await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
                    break
                except TimeoutError:
                    reconnect_delay = min(reconnect_delay * 2, 10.0)
            finally:
                if ws is not None:
                    _ = active_sockets.discard(ws)
                    try:
                        await ws.close()
                    except Exception:
                        pass

    async def _writer_loop(
        self,
        queue: asyncio.Queue[PublicEvent],
        stop_event: asyncio.Event,
    ) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        last_recv_time_ns = 0
        last_event_time_ms = 0

        with self.output_path.open("w", encoding="utf-8") as out_file:
            while not stop_event.is_set() or not queue.empty():
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=0.1)
                except TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break

                safe_recv_ns = max(event.recv_time_ns, last_recv_time_ns)
                safe_event_ms = max(event.event_time_ms, last_event_time_ms)
                last_recv_time_ns = safe_recv_ns
                last_event_time_ms = safe_event_ms

                record = {
                    "recv_time_ns": safe_recv_ns,
                    "event_time_ms": safe_event_ms,
                    "symbol": event.symbol,
                    "b": event.b,
                    "B": event.B,
                    "a": event.a,
                    "A": event.A,
                    "venue": event.venue,
                }
                line = json.dumps(record, separators=(",", ":")) + "\n"
                encoded = line.encode("utf-8")
                line_len = len(encoded)

                if self.bytes_written + line_len > self.max_bytes and self.bytes_written > 0:
                    _ = stop_event.set()
                    break

                _ = out_file.write(line)
                out_file.flush()
                self.bytes_written += line_len
                self.events_recorded += 1

                if self.bytes_written >= self.max_bytes:
                    _ = stop_event.set()
                    break

    async def run(self) -> int:
        stop_event = asyncio.Event()
        active_sockets: set[ClientConnection] = set()
        queue: asyncio.Queue[PublicEvent] = asyncio.Queue(maxsize=1000)

        writer_task = asyncio.create_task(self._writer_loop(queue, stop_event))
        consumer_tasks: list[asyncio.Task[None]] = []

        if self.arcus_ws:
            consumer_tasks.append(
                asyncio.create_task(
                    self._consume_arcus(self.markets, queue, stop_event, active_sockets)
                )
            )

        for market in self.markets:
            binance_sym = resolve_binance_symbol(market)
            consumer_tasks.append(
                asyncio.create_task(
                    self._consume_binance(binance_sym, queue, stop_event, active_sockets)
                )
            )

        start_time = time.monotonic()
        try:
            try:
                async with asyncio.timeout(self.duration_seconds):
                    _ = await stop_event.wait()
            except (TimeoutError, asyncio.TimeoutError):
                _ = stop_event.set()
        finally:
            _ = stop_event.set()

            for task in consumer_tasks:
                _ = task.cancel()
            _ = await asyncio.gather(*consumer_tasks, return_exceptions=True)

            for socket in list(active_sockets):
                try:
                    await socket.close()
                except Exception:
                    pass
            active_sockets.clear()

            try:
                await asyncio.wait_for(writer_task, timeout=2.0)
            except (TimeoutError, asyncio.CancelledError):
                _ = writer_task.cancel()
                _ = await asyncio.gather(writer_task, return_exceptions=True)

        elapsed = time.monotonic() - start_time
        logger.info(
            "Recording finished: %s events, %s bytes in %.2fs",
            self.events_recorded,
            self.bytes_written,
            elapsed,
        )
        return self.bytes_written


def parse_args(argv: list[str]) -> RecordArguments:
    parser = argparse.ArgumentParser(
        description="Public market data recorder for Arcus and Binance"
    )
    _ = parser.add_argument(
        "--markets",
        default=DEFAULT_MARKETS,
        help=f"Comma-separated list of markets (default: {DEFAULT_MARKETS})",
    )
    _ = parser.add_argument(
        "--duration-seconds",
        type=float,
        default=DEFAULT_DURATION_SECONDS,
        help=f"Max duration to record in seconds (default: {DEFAULT_DURATION_SECONDS})",
    )
    _ = parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help=f"Max bytes to write (default: {DEFAULT_MAX_BYTES})",
    )
    _ = parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Path to write newline-delimited JSON events (default: {DEFAULT_OUTPUT_PATH})",
    )
    _ = parser.add_argument(
        "--arcus-ws",
        default=DEFAULT_ARCUS_WS,
        help=f"Arcus public websocket endpoint (default: {DEFAULT_ARCUS_WS})",
    )
    _ = parser.add_argument(
        "--binance-ws-base",
        default=DEFAULT_BINANCE_WS_BASE,
        help=f"Binance USD-M public websocket base (default: {DEFAULT_BINANCE_WS_BASE})",
    )
    args = RecordArguments()
    _ = parser.parse_args(argv, namespace=args)
    return args


async def async_main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    markets = [m.strip() for m in args.markets.split(",") if m.strip()]

    print(
        f"Starting public market recorder:\n  Markets: {', '.join(markets)}\n  Duration: {args.duration_seconds}s\n  Max bytes: {args.max_bytes}\n  Output: {args.output}",
        flush=True,
    )

    start_time = time.monotonic()
    recorder = PublicDataRecorder(
        markets=markets,
        duration_seconds=args.duration_seconds,
        max_bytes=args.max_bytes,
        output_path=args.output,
        arcus_ws=args.arcus_ws,
        binance_ws_base=args.binance_ws_base,
    )

    try:
        bytes_written = await recorder.run()
    except KeyboardInterrupt:
        print("\nRecording interrupted by user.", flush=True)
        bytes_written = recorder.bytes_written

    elapsed = time.monotonic() - start_time
    print(
        f"Recording summary:\n  Status: SUCCESS\n  Events recorded: {recorder.events_recorded}\n  Bytes written: {bytes_written} (limit: {args.max_bytes})\n  Duration: {elapsed:.2f}s (limit: {args.duration_seconds}s)\n  Output path: {recorder.output_path}",
        flush=True,
    )
    return 0


def main() -> None:
    exit_code = asyncio.run(async_main())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
