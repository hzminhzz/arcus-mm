from __future__ import annotations

import json
from decimal import Decimal

import anyio
import pytest

from arcus_bot.bots.market_maker.index import ContinuousMaker, freshness_reason
from arcus_bot.bots.market_maker.market import MARKET_MAPPINGS, MarketInfo
from arcus_bot.bots.market_maker.quoter import MakerQuoteConfig
from arcus_bot.bots.market_maker.runtime import MakerRuntime
from arcus_bot.cli.maker_config import parse_reference_feed
from arcus_bot.pricing.hyperliquid import (
    HyperliquidBookTicker,
    HyperliquidBookTickerFeed,
    normalize_hyperliquid_symbol,
    parse_active_asset_ctx,
    parse_l2_book,
)
from arcus_bot.sdk.account import AccountState
from arcus_bot.sdk.orderbook import OrderbookState
from arcus_bot.types import AccountRef, InputError, JsonObject, JsonValue, OrderState, ProtocolError


class _FakeMakerClient:
    orderbook: OrderbookState
    state: AccountState

    def __init__(self, market_id: int = 18) -> None:
        self.orderbook = OrderbookState()
        self.state = AccountState(market_id=market_id)
        self.state.orders_ready = True
        self.state.position_ready = True
        self.state.account_equity = Decimal("1000")
        self.state.account_updated_at_ns = 10_000_000_000

    async def subscribe(self, channel: str, identifier: str, **extra: JsonValue) -> None:
        _ = (channel, identifier, extra)

    async def subscribe_maker_channels(self) -> None:
        return None

    async def next_message(self) -> JsonObject:
        raise AssertionError("not used in step tests")

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None:
        _ = seconds
        return self.state.order_states.get(order_id)


def _stock_runtime(market_name: str, maximum_basis_bps: Decimal) -> MakerRuntime:
    mapping = MARKET_MAPPINGS[market_name]
    return MakerRuntime(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=mapping.market_id,
            market=mapping.market,
        ),
        quote_config=MakerQuoteConfig(
            order_size_usd=Decimal("40"),
            maximum_position_usd=Decimal("400"),
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("3"),
            latency_buffer_bps=Decimal("2"),
            inventory_skew_bps=Decimal("10"),
        ),
        submit=False,
        signing_key=None,
        run_id="test-hl",
        maximum_basis_bps=maximum_basis_bps,
        reference_feed="hyperliquid",
        reference_symbol=mapping.hyperliquid_symbol or "",
    )


def test_normalize_hyperliquid_symbol() -> None:
    assert normalize_hyperliquid_symbol("xyz:NVDA") == "xyz:NVDA"
    assert normalize_hyperliquid_symbol("xyz:nvda") == "xyz:NVDA"
    assert normalize_hyperliquid_symbol("XYZ:hood") == "xyz:HOOD"
    assert normalize_hyperliquid_symbol("btc") == "BTC"
    assert normalize_hyperliquid_symbol("ETH") == "ETH"


def test_parse_l2_book_valid_hip3_equity() -> None:
    raw = json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "xyz:NVDA",
            "time": 1790655071746,
            "levels": [
                [{"px": "228.28", "sz": "4.381", "n": 1}],
                [{"px": "228.32", "sz": "9.841", "n": 2}],
            ],
        },
    })
    ticker = parse_l2_book(raw, "xyz:NVDA", 10_000_000_000, known_oracle_px=Decimal("228.30"))
    assert ticker.symbol == "xyz:NVDA"
    assert ticker.bid == Decimal("228.28")
    assert ticker.ask == Decimal("228.32")
    assert ticker.bid_qty == Decimal("4.381")
    assert ticker.ask_qty == Decimal("9.841")
    assert ticker.mid == Decimal("228.30")
    assert ticker.fair_anchor == Decimal("228.30")
    assert ticker.oracle_px == Decimal("228.30")
    assert ticker.received_at_ns == 10_000_000_000


def test_parse_l2_book_rejects_crossed_prices() -> None:
    raw = json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "xyz:HOOD",
            "time": 1790655071746,
            "levels": [
                [{"px": "115.50", "sz": "10.0", "n": 1}],
                [{"px": "115.40", "sz": "10.0", "n": 1}],
            ],
        },
    })
    with pytest.raises(ProtocolError, match="uncrossed"):
        _ = parse_l2_book(raw, "xyz:HOOD", 10_000_000_000)


def test_parse_l2_book_rejects_negative_and_zero_prices() -> None:
    raw_zero = json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "xyz:AMZN",
            "time": 1790655071746,
            "levels": [
                [{"px": "0", "sz": "10.0", "n": 1}],
                [{"px": "245.0", "sz": "10.0", "n": 1}],
            ],
        },
    })
    with pytest.raises(ProtocolError, match="positive"):
        _ = parse_l2_book(raw_zero, "xyz:AMZN", 10_000_000_000)


def test_parse_l2_book_rejects_empty_levels() -> None:
    raw_empty = json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "xyz:CRCL",
            "time": 1790655071746,
            "levels": [[], []],
        },
    })
    with pytest.raises(ProtocolError, match="empty book levels"):
        _ = parse_l2_book(raw_empty, "xyz:CRCL", 10_000_000_000)


def test_parse_l2_book_rejects_symbol_mismatch() -> None:
    raw = json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "xyz:SNDK",
            "time": 1790655071746,
            "levels": [
                [{"px": "1698.1", "sz": "1.0", "n": 1}],
                [{"px": "1698.5", "sz": "1.0", "n": 1}],
            ],
        },
    })
    with pytest.raises(ProtocolError, match="does not match expected"):
        _ = parse_l2_book(raw, "xyz:NVDA", 10_000_000_000)


def test_parse_l2_book_rejects_divergent_oracle() -> None:
    raw = json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "xyz:DRAM",
            "time": 1790655071746,
            "levels": [
                [{"px": "60.0", "sz": "1.0", "n": 1}],
                [{"px": "60.2", "sz": "1.0", "n": 1}],
            ],
        },
    })
    with pytest.raises(ProtocolError, match="diverges from oracle"):
        _ = parse_l2_book(raw, "xyz:DRAM", 10_000_000_000, known_oracle_px=Decimal("80.0"))


def test_parse_active_asset_ctx() -> None:
    raw = json.dumps({
        "channel": "activeAssetCtx",
        "data": {
            "coin": "xyz:NVDA",
            "ctx": {
                "funding": "0.00000625",
                "openInterest": "625120.822",
                "prevDayPx": "224.19",
                "dayNtlVlm": "116878617.66",
                "premium": "-0.0000197049",
                "oraclePx": "228.37",
                "markPx": "228.36",
            },
        },
    })
    oracle = parse_active_asset_ctx(raw, "xyz:NVDA")
    assert oracle == Decimal("228.37")
    assert parse_active_asset_ctx(raw, "xyz:HOOD") is None


def test_feed_to_market_mapping_resolution() -> None:
    hood = MARKET_MAPPINGS["HOOD-USD"]
    assert hood.resolve_feed() == ("hyperliquid", "xyz:HOOD")
    assert hood.resolve_feed("auto") == ("hyperliquid", "xyz:HOOD")

    nvda = MARKET_MAPPINGS["NVDA-USD"]
    assert nvda.resolve_feed() == ("hyperliquid", "xyz:NVDA")

    amzn = MARKET_MAPPINGS["AMZN-USD"]
    assert amzn.resolve_feed() == ("hyperliquid", "xyz:AMZN")
    assert amzn.resolve_feed("binance") == ("binance", "AMZNUSDT")

    btc = MARKET_MAPPINGS["BTC-USD"]
    assert btc.resolve_feed() == ("binance", "BTCUSDT")
    assert btc.resolve_feed("hyperliquid") == ("hyperliquid", "BTC")

    with pytest.raises(InputError, match="does not support Binance"):
        _ = hood.resolve_feed("binance")

    zec = MARKET_MAPPINGS["ZEC-USD"]
    with pytest.raises(InputError, match="does not support Hyperliquid"):
        _ = zec.resolve_feed("hyperliquid")


def test_cli_parse_reference_feed_options() -> None:
    mappings = (
        MARKET_MAPPINGS["BTC-USD"],
        MARKET_MAPPINGS["HOOD-USD"],
        MARKET_MAPPINGS["NVDA-USD"],
    )
    feed_map_auto = parse_reference_feed("auto", mappings)
    assert feed_map_auto["BTC-USD"] == ("binance", "BTCUSDT")
    assert feed_map_auto["HOOD-USD"] == ("hyperliquid", "xyz:HOOD")
    assert feed_map_auto["NVDA-USD"] == ("hyperliquid", "xyz:NVDA")

    feed_map_override = parse_reference_feed("BTC-USD=hyperliquid,HOOD-USD=hyperliquid", mappings)
    assert feed_map_override["BTC-USD"] == ("hyperliquid", "BTC")
    assert feed_map_override["HOOD-USD"] == ("hyperliquid", "xyz:HOOD")

    with pytest.raises(InputError, match="unsupported reference feed"):
        _ = parse_reference_feed("coinbase", mappings)


@pytest.mark.parametrize("market_name,expected_hl_symbol", [
    ("HOOD-USD", "xyz:HOOD"),
    ("NVDA-USD", "xyz:NVDA"),
    ("AMZN-USD", "xyz:AMZN"),
])
def test_both_sided_quote_generation_for_stocks(market_name: str, expected_hl_symbol: str) -> None:
    mapping = MARKET_MAPPINGS[market_name]
    runtime = _stock_runtime(market_name, Decimal("50"))
    feed = HyperliquidBookTickerFeed(expected_hl_symbol)

    price_base = Decimal("115.35") if market_name == "HOOD-USD" else Decimal("228.35")
    feed.latest = HyperliquidBookTicker(
        symbol=expected_hl_symbol,
        bid=price_base,
        ask=price_base + Decimal("0.02"),
        received_at_ns=10_000_000_000,
        bid_qty=Decimal("10.0"),
        ask_qty=Decimal("10.0"),
        oracle_px=price_base + Decimal("0.01"),
    )

    client = _FakeMakerClient(market_id=mapping.market_id)
    client.orderbook.best_bid = price_base
    client.orderbook.best_ask = price_base + Decimal("0.02")
    client.orderbook.received_at_ns = 10_000_000_000

    maker = ContinuousMaker[HyperliquidBookTickerFeed](
        runtime=runtime,
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        feed=feed,
        orders=None,
    )

    for i in range(1, 5):
        maker.basis.add_sample(
            price_base + Decimal("0.01"),
            price_base + Decimal("0.01"),
            10_000_000_000 - (5 - i) * 1_000_000_000,
        )

    quotes = anyio.run(maker.step, 10_000_000_000)
    assert quotes is not None
    assert len(quotes) == 2
    buy_quote = [q for q in quotes if q.side == "BUY"][0]
    sell_quote = [q for q in quotes if q.side == "SELL"][0]
    assert buy_quote.price < sell_quote.price
    assert buy_quote.quantity >= mapping.min_order_size
    assert sell_quote.quantity >= mapping.min_order_size


def test_basis_rejection_for_hyperliquid() -> None:
    runtime = _stock_runtime("HOOD-USD", Decimal("20"))
    feed = HyperliquidBookTickerFeed("xyz:HOOD")
    feed.latest = HyperliquidBookTicker(
        symbol="xyz:HOOD",
        bid=Decimal("115.35"),
        ask=Decimal("115.37"),
        received_at_ns=10_000_000_000,
        bid_qty=Decimal("10.0"),
        ask_qty=Decimal("10.0"),
        oracle_px=Decimal("115.36"),
    )
    client = _FakeMakerClient(market_id=18)
    client.orderbook.best_bid = Decimal("120.00")
    client.orderbook.best_ask = Decimal("120.02")
    client.orderbook.received_at_ns = 10_000_000_000

    maker = ContinuousMaker[HyperliquidBookTickerFeed](
        runtime=runtime,
        market=MarketInfo(mapping=MARKET_MAPPINGS["HOOD-USD"], status="ONLINE"),
        client=client,
        feed=feed,
        orders=None,
    )

    quotes = anyio.run(maker.step, 10_000_000_000)
    assert quotes is None
    assert maker.last_status == "Arcus/Hyperliquid basis exceeded its configured limit"


def test_freshness_reason_with_hyperliquid_feed() -> None:
    runtime = _stock_runtime("HOOD-USD", Decimal("50"))
    now_ns = 10_000_000_000

    missing = freshness_reason(now_ns, None, now_ns, runtime, feed_name="Hyperliquid")
    assert missing == "Hyperliquid reference is not available"

    stale_ticker = HyperliquidBookTicker(
        symbol="xyz:HOOD",
        bid=Decimal("115.0"),
        ask=Decimal("115.1"),
        received_at_ns=now_ns - 5_000_000_000,
    )
    stale = freshness_reason(now_ns, stale_ticker, now_ns, runtime, feed_name="Hyperliquid")
    assert stale == "Hyperliquid reference is stale"


def test_position_and_inventory_reducing_quotes_preserved() -> None:
    mapping = MARKET_MAPPINGS["HOOD-USD"]
    runtime = _stock_runtime("HOOD-USD", Decimal("50"))
    feed = HyperliquidBookTickerFeed("xyz:HOOD")
    feed.latest = HyperliquidBookTicker(
        symbol="xyz:HOOD",
        bid=Decimal("100.00"),
        ask=Decimal("100.02"),
        received_at_ns=10_000_000_000,
        bid_qty=Decimal("10.0"),
        ask_qty=Decimal("10.0"),
        oracle_px=Decimal("100.01"),
    )
    client = _FakeMakerClient(market_id=18)
    client.orderbook.best_bid = Decimal("100.00")
    client.orderbook.best_ask = Decimal("100.02")
    client.orderbook.received_at_ns = 10_000_000_000

    maker = ContinuousMaker[HyperliquidBookTickerFeed](
        runtime=runtime,
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        feed=feed,
        orders=None,
    )
    for i in range(1, 5):
        maker.basis.add_sample(Decimal("100.01"), Decimal("100.01"), 10_000_000_000 - (5 - i) * 1_000_000_000)

    flat_quotes = anyio.run(maker.step, 10_000_000_000)
    assert flat_quotes is not None

    client.state.position = Decimal("3.0")
    long_quotes = anyio.run(maker.step, 10_000_000_000 + 1_000_000_000)
    assert long_quotes is not None

    flat_buy = [q for q in flat_quotes if q.side == "BUY"][0]
    long_buy = [q for q in long_quotes if q.side == "BUY"][0]
    flat_sell = [q for q in flat_quotes if q.side == "SELL"][0]
    long_sell = [q for q in long_quotes if q.side == "SELL"][0]

    assert long_buy.price <= flat_buy.price
    assert long_sell.price <= flat_sell.price
