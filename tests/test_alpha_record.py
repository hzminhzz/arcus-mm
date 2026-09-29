from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest
import websockets

from arcus_bot.alpha.record import (
    PublicDataRecorder,
    parse_arcus_l2_frame,
    parse_binance_book_ticker,
    resolve_binance_symbol,
)
from arcus_bot.alpha.replay import load_jsonl, validate_point_in_time_order


async def _slow_recv() -> str:
    _ = await asyncio.Event().wait()
    return ""


def test_resolve_binance_symbol() -> None:
    assert resolve_binance_symbol("BTC-USD") == "BTCUSDT"
    assert resolve_binance_symbol("ETH-USD") == "ETHUSDT"
    assert resolve_binance_symbol("SOL-USD") == "SOLUSDT"
    assert resolve_binance_symbol("DOGEUSDT") == "DOGEUSDT"


def test_parse_binance_valid_frame() -> None:
    raw = (
        '{"e":"bookTicker","u":12345,"s":"BTCUSDT","b":"84000.50",'
        '"B":"1.500","a":"84001.00","A":"2.500","T":1790616606000,"E":1790616606000}'
    )
    event = parse_binance_book_ticker(raw, recv_time_ns=1000)
    assert event is not None
    assert event.recv_time_ns == 1000
    assert event.event_time_ms == 1790616606000
    assert event.symbol == "BTCUSDT"
    assert event.b == "84000.50"
    assert event.B == "1.500"
    assert event.a == "84001.00"
    assert event.A == "2.500"
    assert event.venue == "binance"


def test_parse_arcus_valid_frame() -> None:
    raw = (
        '{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD",'
        '"contents":{"bids":[["84000.0","0.5"]],"asks":[["84001.0","0.8"]],"timestamp":1790616606123000}}'
    )
    event = parse_arcus_l2_frame(raw, recv_time_ns=2000)
    assert event is not None
    assert event.recv_time_ns == 2000
    assert event.event_time_ms == 1790616606123
    assert event.symbol == "BTC-USD"
    assert event.b == "84000.0"
    assert event.B == "0.5"
    assert event.a == "84001.0"
    assert event.A == "0.8"
    assert event.venue == "arcus"


def test_binance_malformed_frame() -> None:
    assert parse_binance_book_ticker(b"NOT_VALID_JSON{") is None
    assert parse_binance_book_ticker("12345") is None
    assert parse_binance_book_ticker('{"e":"bookTicker","s":"BTCUSDT"}') is None
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"bad","a":"84001","B":"1","A":"1"}'
        )
        is None
    )
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"-100","a":"84001","B":"1","A":"1"}'
        )
        is None
    )
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"0","a":"84001","B":"1","A":"1"}'
        )
        is None
    )
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"84005","a":"84001","B":"1","A":"1"}'
        )
        is None
    )
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"84001","a":"84001","B":"1","A":"1"}'
        )
        is None
    )
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"84000","a":"84001","B":"-1","A":"1"}'
        )
        is None
    )
    assert (
        parse_binance_book_ticker(
            '{"s":"BTCUSDT","b":"84000","a":"84001","B":"1","A":"0"}'
        )
        is None
    )


def test_arcus_malformed_frame() -> None:
    assert parse_arcus_l2_frame(b"NOT_VALID_JSON{") is None
    assert (
        parse_arcus_l2_frame(
            '{"type":"channel_data","channel":"trades","id":"BTC-USD","contents":{}}'
        )
        is None
    )
    assert (
        parse_arcus_l2_frame(
            '{"type":"connected","connection_id":"12345"}'
        )
        is None
    )
    assert (
        parse_arcus_l2_frame(
            '{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD","contents":{"bids":[],"asks":[]}}'
        )
        is None
    )
    assert (
        parse_arcus_l2_frame(
            '{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD","contents":{"bids":[["84005","1"]],"asks":[["84001","1"]]}}'
        )
        is None
    )
    assert (
        parse_arcus_l2_frame(
            '{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD","contents":{"bids":[["bad","1"]],"asks":[["84001","1"]]}}'
        )
        is None
    )
    assert (
        parse_arcus_l2_frame(
            '{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD","contents":{"bids":[["84000"]],"asks":[["84001","1"]]}}'
        )
        is None
    )


def test_malformed_frame_adversarial_inputs() -> None:
    bad_inputs = [
        b"\x00\xff\xfe",
        '{"s": "BTCUSDT", "b": "Infinity", "a": "100", "B": "1", "A": "1"}',
        '{"s": "BTCUSDT", "b": "NaN", "a": "100", "B": "1", "A": "1"}',
        '{"s": "BTCUSDT", "b": "100", "a": "100", "B": "1", "A": "1"}',
        "[]",
        "null",
        "",
        "true",
    ]
    for bad in bad_inputs:
        assert parse_binance_book_ticker(bad) is None
        assert parse_arcus_l2_frame(bad) is None


@pytest.mark.anyio
async def test_max_bytes_size_limit_enforced(tmp_path: Path) -> None:
    out_file = tmp_path / "test_limit.jsonl"
    max_bytes = 350
    recorder = PublicDataRecorder(
        markets=["BTC-USD"],
        duration_seconds=5.0,
        max_bytes=max_bytes,
        output_path=out_file,
        arcus_ws=None,
    )

    mock_socket = AsyncMock()
    mock_socket.recv = AsyncMock(
        return_value=(
            '{"s":"BTCUSDT","b":"84000.00","B":"1.00",'
            '"a":"84001.00","A":"2.00","E":5000}'
        )
    )

    class MockAsyncContext:
        async def __aenter__(self) -> AsyncMock:
            return mock_socket

        async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            return None

    with patch("websockets.connect", return_value=MockAsyncContext()):
        _ = await recorder.run()

    assert recorder.bytes_written <= max_bytes
    assert recorder.bytes_written > 0
    assert out_file.is_file()
    assert out_file.stat().st_size <= max_bytes

    records = load_jsonl(out_file)
    assert len(records) > 0
    validate_point_in_time_order(records)


@pytest.mark.anyio
async def test_size_limit_exact_boundary(tmp_path: Path) -> None:
    out_file = tmp_path / "test_boundary.jsonl"
    sample_json = (
        '{"recv_time_ns":1000,"event_time_ms":5000,"symbol":"BTCUSDT",'
        '"b":"84000.00","B":"1.00","a":"84001.00","A":"2.00","venue":"binance"}\n'
    )
    one_line_len = len(sample_json.encode("utf-8"))

    recorder = PublicDataRecorder(
        markets=["BTC-USD"],
        duration_seconds=5.0,
        max_bytes=one_line_len,
        output_path=out_file,
        arcus_ws=None,
    )

    mock_socket = AsyncMock()
    mock_socket.recv = AsyncMock(
        return_value=(
            '{"s":"BTCUSDT","b":"84000.00","B":"1.00",'
            '"a":"84001.00","A":"2.00","E":5000}'
        )
    )

    class MockAsyncContext:
        async def __aenter__(self) -> AsyncMock:
            return mock_socket

        async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            return None

    with (
        patch("websockets.connect", return_value=MockAsyncContext()),
        patch("arcus_bot.alpha.record.time.monotonic_ns", return_value=1000),
    ):
        _ = await recorder.run()

    assert recorder.bytes_written == one_line_len
    assert recorder.events_recorded == 1
    assert out_file.stat().st_size == one_line_len


@pytest.mark.anyio
async def test_duration_limit_enforced(tmp_path: Path) -> None:
    out_file = tmp_path / "test_duration.jsonl"
    duration = 0.1
    recorder = PublicDataRecorder(
        markets=["BTC-USD"],
        duration_seconds=duration,
        max_bytes=100000,
        output_path=out_file,
        arcus_ws=None,
    )

    class MockAsyncContext:
        async def __aenter__(self) -> AsyncMock:
            mock_ws = AsyncMock()
            mock_ws.recv = _slow_recv
            mock_ws.close = AsyncMock()
            return mock_ws

        async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            return None

    with patch("websockets.connect", return_value=MockAsyncContext()):
        start = time.monotonic()
        _ = await recorder.run()
        elapsed = time.monotonic() - start

    assert elapsed >= duration
    assert elapsed < duration + 0.3


@pytest.mark.anyio
async def test_socket_disconnect_clean_teardown(tmp_path: Path) -> None:
    out_file = tmp_path / "test_disconnect.jsonl"
    recorder = PublicDataRecorder(
        markets=["BTC-USD"],
        duration_seconds=1.0,
        max_bytes=10000,
        output_path=out_file,
        arcus_ws=None,
    )

    disconnected = asyncio.Event()
    mock_socket = AsyncMock()

    async def disconnect() -> str:
        disconnected.set()
        raise websockets.ConnectionClosed(rcvd=None, sent=None)

    mock_socket.recv = disconnect

    class MockAsyncContext:
        async def __aenter__(self) -> AsyncMock:
            return mock_socket

        async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            return None

    with patch("websockets.connect", return_value=MockAsyncContext()):
        _ = await recorder.run()

    assert disconnected.is_set()
    assert cast(AsyncMock, mock_socket.close).await_count >= 1
    assert out_file.is_file()


@pytest.mark.anyio
async def test_clean_socket_teardown_no_leaks(tmp_path: Path) -> None:
    out_file = tmp_path / "test_teardown.jsonl"
    recorder = PublicDataRecorder(
        markets=["BTC-USD"],
        duration_seconds=0.1,
        max_bytes=10000,
        output_path=out_file,
        arcus_ws="wss://mock.arcus",
    )

    mock_ws = AsyncMock()
    mock_ws.recv = _slow_recv
    mock_ws.send = AsyncMock()
    mock_ws.close = AsyncMock()

    class MockAsyncContext:
        async def __aenter__(self) -> AsyncMock:
            return mock_ws

        async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            return None

    with patch("websockets.connect", return_value=MockAsyncContext()):
        _ = await recorder.run()

    assert cast(AsyncMock, mock_ws.close).await_count >= 1
    assert out_file.is_file()
