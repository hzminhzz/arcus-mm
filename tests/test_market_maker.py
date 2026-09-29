"""Deterministic tests for the continuous Arcus market maker."""

import hashlib
import json
import sys
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from time import monotonic_ns
from types import SimpleNamespace

import anyio
import pytest

from arcus_bot.alpha.replay import evaluate_replay
from arcus_bot.cli.maker import main as maker_main
from arcus_bot.bots.market_maker.market import (
    MARKET_MAPPINGS,
    MarketInfo,
    parse_market_snapshot,
)
from arcus_bot.bots.market_maker.order_manager import MakerOrderManager
from arcus_bot.bots.market_maker.index import (
    ContinuousMaker,
    freshness_reason,
)
from arcus_bot.bots.market_maker.quoter import (
    BasisEstimator,
    MakerQuoteConfig,
    Quote,
    QuoteContext,
    calculate_quotes,
)
from arcus_bot.bots.market_maker.runtime import MakerRuntime
from arcus_bot.cli.maker_config import parse_options, verify_authentic_go_report
from arcus_bot.pricing.binance import BinanceBookTickerFeed, parse_book_ticker
from arcus_bot.sdk.account import AccountState
from arcus_bot.sdk.orderbook import OrderbookState
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import (
    AccountRef,
    JSON_ADAPTER,
    InputError,
    OrderConfig,
    OrderRules,
    OrderState,
    JsonObject,
    JsonValue,
    ProtocolError,
)


def market_snapshot(market_id: int, market: str, base: str) -> str:
    """Build one current-shaped markets snapshot for a known testnet market."""
    mapping = MARKET_MAPPINGS[market]
    row = {
        "marketDisplayName": market,
        "marketId": market_id,
        "status": "ONLINE",
        "baseAsset": base,
        "quoteAsset": "USD",
        "type": "PERPETUAL",
        "tickSize": str(mapping.tick_size),
        "stepSize": str(mapping.step_size),
        "minOrderSize": str(mapping.min_order_size),
        "minOrderNotional": str(mapping.min_order_notional),
        "maxOrderSize": str(mapping.max_order_size),
        "markPrice": "100",
    }
    return json.dumps(
        {
            "type": "subscribed",
            "channel": "markets",
            "contents": {"isSnapshot": True, "markets": {str(market_id): row}},
        }
    )


def test_recursive_json_adapter_parses_nested_arcus_frames() -> None:
    # Given a nested Arcus account message.
    raw = '{"channel":"orders","contents":{"openOrders":[{"orderId":"o1"}]}}'

    # When the shared protocol adapter parses it.
    parsed = JSON_ADAPTER.validate_json(raw)

    # Then nested arrays and objects remain typed JSON values.
    assert isinstance(parsed, dict)
    contents = parsed["contents"]
    assert isinstance(contents, dict)
    open_orders = contents["openOrders"]
    assert isinstance(open_orders, list)
    assert open_orders == [{"orderId": "o1"}]


def test_binance_book_ticker_parses_bbo_and_receive_time() -> None:
    # Given a BTCUSDT best-bid/ask update with decimal strings.
    raw = '{"e":"bookTicker","s":"BTCUSDT","b":"100.0","a":"101.0"}'

    # When the configured symbol is parsed.
    ticker = parse_book_ticker(raw, "BTCUSDT", 123)

    # Then the feed keeps precise prices and the supplied monotonic timestamp.
    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("100")
    assert ticker.ask == Decimal("101")
    assert ticker.mid == Decimal("100.5")
    assert ticker.received_at_ns == 123
    assert ticker.bid_qty is None
    assert ticker.ask_qty is None


def test_binance_book_ticker_parses_top_quantities() -> None:
    # Given a BTCUSDT bookTicker update with valid top quantities.
    raw = '{"e":"bookTicker","s":"BTCUSDT","b":"100.0","a":"101.0","B":"1.5","A":"2.5"}'

    # When parsed.
    ticker = parse_book_ticker(raw, "BTCUSDT", 123)

    # Then positive finite quantities are retained.
    assert ticker.bid_qty == Decimal("1.5")
    assert ticker.ask_qty == Decimal("2.5")


@pytest.mark.parametrize(
    ("raw", "expected_bid_qty", "expected_ask_qty"),
    [
        ('{"s":"BTCUSDT","b":"100","a":"101","B":"0","A":"1.0"}', None, Decimal("1.0")),
        ('{"s":"BTCUSDT","b":"100","a":"101","B":"-1.5","A":"1.0"}', None, Decimal("1.0")),
        ('{"s":"BTCUSDT","b":"100","a":"101","B":"nan","A":"1.0"}', None, Decimal("1.0")),
        ('{"s":"BTCUSDT","b":"100","a":"101","B":"inf","A":"1.0"}', None, Decimal("1.0")),
        ('{"s":"BTCUSDT","b":"100","a":"101","B":"invalid","A":"1.0"}', None, Decimal("1.0")),
    ],
)
def test_invalid_sizes_handled_gracefully(
    raw: str, expected_bid_qty: Decimal | None, expected_ask_qty: Decimal | None
) -> None:
    # Given malformed, non-positive, or non-finite size fields.
    # When parsing the event.
    ticker = parse_book_ticker(raw, "BTCUSDT", 123)

    # Then prices are preserved and invalid quantities fall back to None.
    assert ticker.bid == Decimal("100")
    assert ticker.ask == Decimal("101")
    assert ticker.bid_qty == expected_bid_qty
    assert ticker.ask_qty == expected_ask_qty


@pytest.mark.parametrize(
    ("raw", "symbol"),
    [
        ('{"s":"ETHUSDT","b":"100","a":"101"}', "BTCUSDT"),
        ('{"s":"BTCUSDT","b":"101","a":"100"}', "BTCUSDT"),
        ("not-json", "BTCUSDT"),
    ],
)
def test_binance_book_ticker_rejects_invalid_market_data(raw: str, symbol: str) -> None:
    # Given malformed, crossed, or misrouted market data.
    # When parsing the configured feed.
    # Then the strategy receives a protocol failure instead of a stale price.
    with pytest.raises(ProtocolError):
        _ = parse_book_ticker(raw, symbol, 123)


def test_market_snapshot_validates_explicit_btc_mapping() -> None:
    # Given the BTC-USD testnet metadata for market 1.
    raw = market_snapshot(1, "BTC-USD", "BTC")

    # When the explicit BTC mapping is checked.
    info = parse_market_snapshot(raw, MARKET_MAPPINGS["BTC-USD"])

    # Then it is online, perpetual, and linked to the verified Binance symbol.
    assert info.mapping.market_id == 1
    assert info.mapping.binance_symbol == "BTCUSDT"
    assert info.status == "ONLINE"
    assert info.tick_size_for(Decimal("100")) == Decimal("0.1")


def test_market_snapshot_rejects_wrong_arcus_market_id() -> None:
    # Given an ETH row carrying the wrong Arcus market ID.
    raw = market_snapshot(1, "ETH-USD", "ETH")

    # When validating the explicit ETH mapping.
    # Then the bot fails closed rather than trading the wrong market.
    with pytest.raises(ProtocolError, match="omitted ETH-USD"):
        _ = parse_market_snapshot(raw, MARKET_MAPPINGS["ETH-USD"])


def test_basis_estimator_uses_median_and_rejects_outliers() -> None:
    # Given three in-bound basis samples and one sample outside the configured band.
    estimator = BasisEstimator(
        window_ns=10_000_000_000,
        maximum_basis_bps=Decimal("50"),
        minimum_samples=3,
    )
    estimator.add_sample(Decimal("100.02"), Decimal("100"), 1_000_000_000)
    estimator.add_sample(Decimal("100.03"), Decimal("100"), 2_000_000_000)
    estimator.add_sample(Decimal("100.01"), Decimal("100"), 3_000_000_000)
    estimator.add_sample(Decimal("101"), Decimal("100"), 4_000_000_000)

    # When fair price is evaluated at the last reference midpoint.
    fair = estimator.fair_price(Decimal("100"), 4_000_000_000)

    # Then the rolling median basis is applied and the outlier is ignored.
    assert fair == Decimal("100.02")


def test_basis_estimator_expires_samples_outside_window() -> None:
    # Given a basis sample older than the configured window.
    estimator = BasisEstimator(
        window_ns=1_000_000_000,
        maximum_basis_bps=Decimal("100"),
        minimum_samples=1,
    )
    estimator.add_sample(Decimal("100.1"), Decimal("100"), 1_000_000_000)

    # When the sample is evaluated after its window expires.
    fair = estimator.fair_price(Decimal("100"), 2_000_000_001)

    # Then no fair price is produced from expired basis data.
    assert fair is None


def test_quoter_respects_edges_ticks_and_hard_position_cap() -> None:
    # Given explicit economics, a flat position, and an uncrossed Arcus BBO.
    mapping = MARKET_MAPPINGS["BTC-USD"]
    info = MarketInfo(mapping=mapping, status="ONLINE")
    settings = MakerQuoteConfig(
        order_size_usd=Decimal("20"),
        maximum_position_usd=Decimal("100"),
        maker_fee_bps=Decimal("1"),
        minimum_edge_bps=Decimal("2"),
        latency_buffer_bps=Decimal("1"),
        inventory_skew_bps=Decimal("0"),
    )
    context = QuoteContext(
        market=info,
        config=settings,
        fair_price=Decimal("100"),
        best_bid=Decimal("99.9"),
        best_ask=Decimal("100.1"),
        position=Decimal(0),
    )

    # When calculating the two-sided quote.
    quotes = calculate_quotes(context)

    # Then both sides are passive, tick-aligned, and under the position cap.
    assert [(quote.side, quote.price) for quote in quotes] == [
        ("BUY", Decimal("99.9")),
        ("SELL", Decimal("100.1")),
    ]
    assert all(quote.quantity * quote.price <= settings.order_size_usd for quote in quotes)
    assert quotes[0].quantity == Decimal("0.2")
    assert quotes[1].quantity == Decimal("0.19980019")


def test_quoter_per_order_cap_survives_dust_reduction() -> None:
    # Given a reducing sell that would otherwise exceed the order-size cap to avoid dust.
    mapping = MARKET_MAPPINGS["BTC-USD"]
    info = MarketInfo(mapping=mapping, status="ONLINE")
    settings = MakerQuoteConfig(
        order_size_usd=Decimal("10"),
        maximum_position_usd=Decimal("100"),
        maker_fee_bps=Decimal(0),
        minimum_edge_bps=Decimal("1"),
        latency_buffer_bps=Decimal(0),
        inventory_skew_bps=Decimal(0),
    )
    context = QuoteContext(
        market=info,
        config=settings,
        fair_price=Decimal("100"),
        best_bid=Decimal("99.9"),
        best_ask=Decimal("100.1"),
        position=Decimal("0.10001"),
    )

    # When calculating quotes, the hard notional cap takes precedence over dust cleanup.
    quotes = calculate_quotes(context)

    # Then no submitted order can exceed the configured per-order notional.
    sell_quote = next(quote for quote in quotes if quote.side == "SELL")
    assert all(quote.quantity * quote.price <= settings.order_size_usd for quote in quotes)
    assert sell_quote.quantity * sell_quote.price <= settings.order_size_usd


def test_quoter_only_quotes_reducing_side_at_position_cap() -> None:
    # Given a long position exactly at its configured USD cap.
    mapping = MARKET_MAPPINGS["BTC-USD"]
    info = MarketInfo(mapping=mapping, status="ONLINE")
    settings = MakerQuoteConfig(
        order_size_usd=Decimal("100"),
        maximum_position_usd=Decimal("100"),
        maker_fee_bps=Decimal(0),
        minimum_edge_bps=Decimal("1"),
        latency_buffer_bps=Decimal(0),
        inventory_skew_bps=Decimal(0),
    )
    context = QuoteContext(
        market=info,
        config=settings,
        fair_price=Decimal("100"),
        best_bid=Decimal("99.9"),
        best_ask=Decimal("100.1"),
        position=Decimal("1"),
    )

    # When calculating risk-limited quotes.
    quotes = calculate_quotes(context)

    # Then only a sell quote remains and it cannot increase short exposure.
    assert len(quotes) == 1
    assert quotes[0].side == "SELL"
    assert quotes[0].quantity <= Decimal("1")


def test_account_state_projects_fill_until_position_sequence_catches_up() -> None:
    # Given a flat Arcus snapshot at sequence 10 and a later partial buy fill.
    state = AccountState(market_id=1)
    state.require_order_sequence = True
    state.apply(
        {
            "type": "subscribed",
            "channel": "positions",
            "contents": {
                "lastSequenceId": 10,
                "positions": {"1": {"side": "FLAT", "size": "0", "sequenceNumber": 10}},
            },
        },
        received_at_ns=10,
    )
    state.apply(
        {
            "type": "channel_data",
            "channel": "orders",
            "contents": {
                "marketId": 1,
                "orderId": "maker-1",
                "side": "BUY",
                "state": "PARTIALLY_FILLED",
                "originalSize": "0.1",
                "remainingSize": "0.07",
                "avgFillPrice": "100",
                "sequenceNumber": 11,
            },
        },
        received_at_ns=11,
    )

    # When the order event arrives before the position update.
    projected_before = state.effective_position
    state.apply(
        {
            "type": "channel_data",
            "channel": "positions",
            "contents": {
                "isSnapshot": False,
                "lastSequenceId": 11,
                "positions": {
                    "1": {
                        "marketId": 1,
                        "side": "LONG",
                        "size": "0.03",
                        "sequenceNumber": 11,
                    }
                },
            },
        },
        received_at_ns=12,
    )

    # Then projected exposure is never lost or counted twice.
    assert projected_before == Decimal("0.03")
    assert state.position == Decimal("0.03")
    assert state.effective_position == Decimal("0.03")
    assert not state.pending_position_fills


def test_account_state_counts_unique_realtime_fill_notional() -> None:
    # Given a subscribed fill snapshot followed by realtime fill events.
    state = AccountState(market_id=1)
    prior_fill: JsonObject = {
        "tradeId": "trade-1",
        "orderId": "order-1",
        "side": "BUY",
        "price": "100",
        "size": "0.1",
    }
    state.apply(
        {
            "type": "subscribed",
            "channel": "userFills",
            "contents": {"fills": [prior_fill]},
        }
    )

    # When a historical event repeats and one new trade arrives.
    state.apply(
        {
            "type": "channel_data",
            "channel": "userFills",
            "contents": prior_fill,
        }
    )
    state.apply(
        {
            "type": "channel_data",
            "channel": "userFills",
            "contents": {
                "tradeId": "trade-2",
                "orderId": "order-2",
                "side": "SELL",
                "price": "101",
                "size": "0.2",
            },
        }
    )

    # Then only unique realtime turnover counts toward this process's cap.
    assert state.cumulative_traded_notional_usd == Decimal("20.2")
    assert state.fill_notional_by_order == {"order-2": Decimal("20.2")}


def test_account_state_tracks_equity_update_time() -> None:
    # Given a live account-equity update received at a known monotonic time.
    state = AccountState(market_id=1)

    # When the account channel updates equity.
    state.apply(
        {
            "type": "subscribed",
            "channel": "account",
            "contents": {"accountEquity": "100.5", "freeCollateral": "80"},
        },
        received_at_ns=123_000_000,
    )

    # Then the loss guard has both the value and its freshness timestamp.
    assert state.account_equity == Decimal("100.5")
    assert state.account_updated_at_ns == 123_000_000


def test_account_order_snapshot_only_tracks_selected_market() -> None:
    # Given an account-wide snapshot containing a BTC quote and an unrelated HYPE order.
    state = AccountState(market_id=1)
    state.apply(
        {
            "type": "subscribed",
            "channel": "orders",
            "contents": {
                "openOrders": [
                    {"marketId": 1, "orderId": "btc-order"},
                    {"marketId": 6, "orderId": "hype-order"},
                ]
            },
        }
    )

    # Then only the selected BTC market is eligible for maker cancellation.
    assert state.open_orders == {"btc-order"}


def test_order_payload_uses_dynamic_increments_and_client_id() -> None:
    # Given a market whose active tick/step differ from the cycle defaults.
    config = OrderConfig(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=1,
            market="BTC-USD",
        ),
        tick_size=Decimal("0.5"),
        step_size=Decimal("0.001"),
    )

    # When a market-specific post-only payload is signed.
    body, signed = ArcusOrders.build_place_payload(
        config,
        "BUY",
        Decimal("25000.1"),
        Decimal("0.0002"),
        1_234_567_891_234,
        OrderRules(
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.0001"),
            client_id="am-1-test-b",
        ),
    )

    # Then the signer uses those increments and binds the client ID.
    assert body["clientId"] == "am-1-test-b"
    assert body["price"] == "25000.1"
    assert body["quantity"] == "0.0002"
    assert b'"c":"am-1-test-b"' in signed
    assert b'"p":250001' in signed
    assert b'"q":2' in signed


def test_order_payload_sets_good_til_time_one_month_in_future() -> None:
    # Given an order signing context at timestamp T.
    config = OrderConfig(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=1,
            market="BTC-USD",
        ),
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.0001"),
    )
    timestamp_ns = 1_234_567_891_234_000
    expected_us = timestamp_ns // 1000 + 31 * 86_400_000_000

    # When building the order payload.
    body, signed = ArcusOrders.build_place_payload(
        config,
        "BUY",
        Decimal("25000"),
        Decimal("0.0002"),
        timestamp_ns,
        OrderRules(
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.0001"),
            client_id="am-1-test",
        ),
    )

    # Then goodTilTime is at least one month in future as required by Arcus.
    assert body["goodTilTime"] == str(expected_us)
    assert f'"g":{expected_us * 1000}'.encode() in signed


def _runtime(submit: bool) -> MakerRuntime:
    """Build one deterministic test runtime."""
    return MakerRuntime(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=1,
            market="BTC-USD",
        ),
        quote_config=MakerQuoteConfig(
            order_size_usd=Decimal("20"),
            maximum_position_usd=Decimal("100"),
            maker_fee_bps=Decimal("1"),
            minimum_edge_bps=Decimal("2"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("5"),
        ),
        submit=submit,
        signing_key="local-test-key" if submit else None,
        run_id="test-run",
        maximum_basis_bps=Decimal("50"),
        duration_seconds=60 if submit else 0,
        run_deadline_ns=monotonic_ns() + 60_000_000_000 if submit else None,
        run_expiration_time_us=time.time_ns() // 1_000 + 60_000_000
        if submit
        else None,
        max_traded_notional_usd=Decimal("100") if submit else None,
        max_loss_usd=Decimal("10") if submit else None,
    )


class _FakeMakerClient:
    """Keep real account/orderbook state while simulating Arcus lifecycle events."""

    orderbook: OrderbookState
    state: AccountState

    def __init__(self) -> None:
        self.orderbook = OrderbookState()
        self.state = AccountState(market_id=1)
        self.state.orders_ready = True
        self.state.position_ready = True
        self.state.account_equity = Decimal("100")
        self.state.account_updated_at_ns = 100_000_000_000

    async def subscribe(
        self,
        channel: str,
        identifier: str,
        **extra: JsonValue,
    ) -> None:
        _ = (channel, identifier, extra)

    async def subscribe_maker_channels(self) -> None:
        return None

    async def next_message(self) -> JsonObject:
        raise AssertionError("the stale-pause test should not read the socket")

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None:
        _ = seconds
        return self.state.order_states.get(order_id)


class _FakeMakerOrders:
    """Acknowledge cancellation and deliver its terminal account event."""

    client: _FakeMakerClient
    cancelled: list[str]
    placed: list[tuple[str, Decimal, Decimal]]

    def __init__(self, client: _FakeMakerClient) -> None:
        self.client = client
        self.cancelled = []
        self.placed = []

    async def place(
        self,
        side: str,
        price: Decimal,
        quantity: Decimal,
        rules: OrderRules | None = None,
    ) -> str:
        _ = rules
        self.placed.append((side, price, quantity))
        return f"replacement-{len(self.placed)}"

    async def cancel(self, order_id: str) -> None:
        self.cancelled.append(order_id)
        self.client.state.apply(
            {
                "type": "channel_data",
                "channel": "orders",
                "contents": {
                    "marketId": 1,
                    "orderId": order_id,
                    "side": "BUY",
                    "state": "CANCELED",
                    "status": "CANCELED",
                    "originalSize": "0.2",
                    "remainingSize": "0.2",
                    "price": "99",
                    "sequenceNumber": 1,
                },
            }
        )


def test_startup_refuses_existing_orders_without_canceling() -> None:
    # Given a live BTC maker account with an existing order in its selected market.
    client = _FakeMakerClient()
    client.state.apply(
        {
            "type": "subscribed",
            "channel": "orders",
            "contents": {
                "openOrders": [
                    {"marketId": 1, "orderId": "btc-order"},
                    {"marketId": 6, "orderId": "hype-order"},
                ]
            },
        }
    )
    orders = _FakeMakerOrders(client)
    maker = ContinuousMaker(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        feed=BinanceBookTickerFeed("BTCUSDT"),
        orders=orders,
    )

    # When a live run starts, it refuses ownership before submitting or canceling.
    with pytest.raises(ProtocolError, match="refusing to take ownership"):
        anyio.run(maker.run)

    # Then the pre-existing market order remains untouched.
    assert not orders.cancelled
    assert client.state.open_orders == {"btc-order"}


def test_stale_reference_pause_cancels_and_confirms_owned_quotes() -> None:
    # Given a live maker with one owned Arcus order and a stale reference price.
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client)
    client.state.open_orders.add("order-1")
    mapping = MARKET_MAPPINGS["BTC-USD"]
    maker = ContinuousMaker(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        feed=BinanceBookTickerFeed("BTCUSDT"),
        orders=orders,
    )
    maker.order_manager.tracked_orders["order-1"] = Quote(
        side="BUY",
        price=Decimal("82900"),
        quantity=Decimal("0.0001"),
    )
    maker.order_manager.tracked_orders["order-1"] = calculate_quotes(
        QuoteContext(
            market=MarketInfo(mapping=mapping, status="ONLINE"),
            config=_runtime(submit=True).quote_config,
            fair_price=Decimal("100"),
            best_bid=Decimal("99.9"),
            best_ask=Decimal("100.1"),
            position=Decimal(0),
        )
    )[0]

    # When the maker enters its stale-data pause.
    anyio.run(maker.pause, "Binance reference is stale")

    # Then cancellation is requested and terminal state clears the tracked order.
    assert orders.cancelled == ["order-1"]
    assert not orders.placed
    assert client.state.order_states["order-1"].status == "CANCELED"
    assert not client.state.open_orders
    assert not maker.order_manager.tracked_orders


def test_reconcile_keeps_an_unchanged_order_without_duplicate_placement() -> None:
    # Given an open owned bid that already matches the desired quote.
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client)
    client.state.open_orders.add("order-1")
    client.state.order_states["order-1"] = OrderState(
        status="OPEN",
        filled_quantity=Decimal(0),
        average_fill_price=Decimal(0),
        side="BUY",
    )
    quote = Quote(side="BUY", price=Decimal("99.9"), quantity=Decimal("0.2"))
    manager = MakerOrderManager(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=orders,
        tracked_orders={"order-1": quote},
    )

    # When Arcus state is reconciled against the same desired quote.
    anyio.run(manager.reconcile, (quote,), Decimal("100"), Decimal("99.9"), Decimal("100.1"))

    # Then the existing order remains live and no duplicate is placed.
    assert not orders.cancelled
    assert not orders.placed
    assert client.state.open_orders == {"order-1"}
    assert manager.tracked_orders == {"order-1": quote}


def test_reconcile_confirms_cancel_before_replacement() -> None:
    # Given an owned resting bid whose price no longer matches the desired quote.
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client)
    client.state.open_orders.add("order-1")
    client.state.order_states["order-1"] = OrderState(
        status="OPEN",
        filled_quantity=Decimal(0),
        average_fill_price=Decimal(0),
        side="BUY",
    )
    current = Quote(side="BUY", price=Decimal("99.9"), quantity=Decimal("0.2"))
    replacement = Quote(side="BUY", price=Decimal("99.8"), quantity=Decimal("0.2"))
    manager = MakerOrderManager(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=orders,
        tracked_orders={"order-1": current},
    )

    # When a changed desired quote is reconciled.
    anyio.run(
        manager.reconcile,
        (replacement,),
        Decimal("100"),
        Decimal("99.9"),
        Decimal("100.1"),
    )

    # Then the old order is terminal before the manager considers a replacement.
    assert orders.cancelled == ["order-1"]
    assert not orders.placed
    assert not client.state.open_orders
    assert not manager.tracked_orders


def test_reconcile_holds_changed_quote_until_minimum_rest_expires() -> None:
    # Given a healthy open bid that has not rested for the configured minimum.
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client)
    client.state.open_orders.add("order-1")
    client.state.order_states["order-1"] = OrderState(
        status="OPEN",
        filled_quantity=Decimal(0),
        average_fill_price=Decimal(0),
        side="BUY",
    )
    current = Quote(side="BUY", price=Decimal("99.9"), quantity=Decimal("0.2"))
    replacement = Quote(side="BUY", price=Decimal("99.8"), quantity=Decimal("0.2"))
    runtime = replace(_runtime(submit=True), minimum_order_rest_ms=5_000)
    manager = MakerOrderManager(
        runtime=runtime,
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=orders,
        tracked_orders={"order-1": current},
        placed_at_ns={"order-1": monotonic_ns()},
    )

    # When the desired price changes before the rest timer expires.
    anyio.run(
        manager.reconcile,
        (replacement,),
        Decimal("100"),
        Decimal("99.8"),
        Decimal("100.1"),
    )

    # Then the still-desired side remains resting without cancel/replacement churn.
    assert not orders.cancelled
    assert not orders.placed
    assert manager.tracked_orders == {"order-1": current}
    assert client.state.open_orders == {"order-1"}


def test_cancel_open_orders_ignores_minimum_rest_interval() -> None:
    # Given an owned healthy quote whose minimum rest interval has not elapsed.
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client)
    client.state.open_orders.add("order-1")
    client.state.order_states["order-1"] = OrderState(
        status="OPEN",
        filled_quantity=Decimal(0),
        average_fill_price=Decimal(0),
        side="BUY",
    )
    quote = Quote(side="BUY", price=Decimal("99.9"), quantity=Decimal("0.2"))
    manager = MakerOrderManager(
        runtime=replace(_runtime(submit=True), minimum_order_rest_ms=60_000),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=orders,
        tracked_orders={"order-1": quote},
        placed_at_ns={"order-1": monotonic_ns()},
    )

    # When the caller explicitly cancels all orders for safety.
    anyio.run(manager.cancel_open_orders)

    # Then safety cancellation is immediate and terminally confirmed.
    assert orders.cancelled == ["order-1"]
    assert not client.state.open_orders
    assert not manager.tracked_orders


def test_projected_position_reserves_fillable_order_quantity() -> None:
    # Given a long position with an additional active bid near its USD cap.
    client = _FakeMakerClient()
    client.state.position = Decimal("0.9")
    manager = MakerOrderManager(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=_FakeMakerOrders(client),
        tracked_orders={
            "order-1": Quote(
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal("0.2"),
            )
        },
    )

    # When maximum possible long exposure is projected.
    exceeds = manager.projected_position_exceeds_limit(Decimal("100"))

    # Then the still-fillable quantity is included in the hard cap.
    assert exceeds


def test_freshness_gate_rejects_missing_and_stale_feeds() -> None:
    # Given the configured feed-age limits and no book snapshot.
    runtime = _runtime(submit=False)

    # When freshness is evaluated with missing and aged inputs.
    missing_reference = freshness_reason(10_000_000_000, None, 9_500_000_000, runtime)
    stale_book = freshness_reason(
        10_000_000_000,
        parse_book_ticker(
            '{"s":"BTCUSDT","b":"100","a":"101"}',
            "BTCUSDT",
            9_500_000_000,
        ),
        0,
        runtime,
    )

    # Then neither case can authorize quote generation.
    assert missing_reference == "Binance reference is not available"
    assert stale_book == "Arcus orderbook is stale"


def test_freshness_gate_accepts_fresh_independently_arriving_feeds() -> None:
    # Given both feeds are fresh but arrived at different times within their age limits.
    runtime = _runtime(submit=False)
    reference = parse_book_ticker(
        '{"s":"BTCUSDT","b":"100","a":"101"}',
        "BTCUSDT",
        9_100_000_000,
    )

    # When the Arcus snapshot arrived 900 ms after the Binance update.
    reason = freshness_reason(10_000_000_000, reference, 10_000_000_000, runtime)

    # Then freshness depends on age, not simultaneous packet arrival.
    assert reason is None


def test_pair_skew_exceeded() -> None:
    # Given fresh feeds whose receive timestamps differ by more than maximum_pair_skew_ms.
    runtime = _runtime(submit=False)
    assert runtime.maximum_pair_skew_ms == 1_000
    # Reference at 8.9s, book at 10.0s (skew = 1.1s > 1.0s limit, both <= 2.0s and 1.0s max age)
    reference = parse_book_ticker(
        '{"s":"BTCUSDT","b":"100","a":"101"}',
        "BTCUSDT",
        8_900_000_000,
    )
    reason = freshness_reason(10_000_000_000, reference, 10_000_000_000, runtime)

    # Then pair skew limit triggers fail-closed pause.
    assert reason == "reference and book pair skew exceeds limit"

    # Also test when book is older than reference by > maximum_pair_skew_ms
    # (Reference at 10.0s, book at 8.9s) -> though max book age is 1000ms so book age would trigger first if now is 10.0s.
    # If now is 10.0s, book age 1.1s is > 1.0s max book age.
    # But if runtime has maximum_book_age_ms=2000, skew of 1.1s triggers skew.
    runtime_high_book_age = replace(runtime, maximum_book_age_ms=2_000)
    ref_new = parse_book_ticker(
        '{"s":"BTCUSDT","b":"100","a":"101"}',
        "BTCUSDT",
        10_000_000_000,
    )
    reason2 = freshness_reason(10_000_000_000, ref_new, 8_900_000_000, runtime_high_book_age)
    assert reason2 == "reference and book pair skew exceeds limit"


def test_mainnet_market_data_and_orders_require_submit_opt_in() -> None:
    # Given fully specified risk/economic inputs but only the mainnet selector.
    argv = [
        "--markets",
        "BTC-USD",
        "--order-size-usd",
        "10",
        "--max-position-usd",
        "20",
        "--maker-fee-bps",
        "1.5",
        "--minimum-edge-bps",
        "2",
        "--latency-buffer-bps",
        "1",
        "--inventory-skew-bps",
        "5",
        "--max-basis-bps",
        "50",
        "--mainnet",
    ]

    # When parsing a mainnet request without explicit order submission.
    # Then it fails before any network connection can be opened.
    with pytest.raises(InputError, match="--mainnet requires --submit"):
        _ = parse_options(argv)


def test_task_group_error_is_rendered_with_original_exception(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a live runner whose child task fails with an actionable cause.
    async def fail_run(options: object) -> None:
        _ = options
        raise BaseExceptionGroup(
            "unhandled errors in a TaskGroup",
            [OSError("Binance connection reset")],
        )

    monkeypatch.setattr("arcus_bot.cli.maker._run", fail_run)
    monkeypatch.setattr(
        "arcus_bot.cli.maker.parse_options",
        lambda: SimpleNamespace(log_level="INFO"),
    )
    def ignore_log_level(level: str) -> None:
        _ = level

    monkeypatch.setattr("arcus_bot.cli.maker.configure_logging", ignore_log_level)

    # When the command-line entrypoint catches the task-group failure.
    result = maker_main()

    # Then the original nested cause is written to stderr and exit is nonzero.
    captured = capsys.readouterr()
    assert result == 1
    assert "OSError('Binance connection reset')" in captured.err


def test_inventory_skew_cannot_hide_negative_fee_edge_against_fair() -> None:
    # A long inventory skews the center downward, but cannot make a losing sell valid.
    info = MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE")
    settings = MakerQuoteConfig(
        order_size_usd=Decimal("100"),
        maximum_position_usd=Decimal("100"),
        maker_fee_bps=Decimal("1.5"),
        minimum_edge_bps=Decimal("2"),
        latency_buffer_bps=Decimal("1"),
        inventory_skew_bps=Decimal("35"),
    )
    context = QuoteContext(
        market=info,
        config=settings,
        fair_price=Decimal("83000"),
        best_bid=Decimal("82990"),
        best_ask=Decimal("83010"),
        position=Decimal("0.00120481"),
    )

    quotes = calculate_quotes(context)

    # The skewed center yields a sell at 82990.1, below fair before fees.
    assert all(quote.side != "SELL" for quote in quotes)
    for quote in quotes:
        edge = ((context.fair_price - quote.price) if quote.side == "BUY" else
                (quote.price - context.fair_price)) * Decimal(10_000) / context.fair_price
        assert edge - settings.maker_fee_bps >= settings.minimum_edge_bps + settings.latency_buffer_bps


def test_inventory_skew_keeps_profitable_unwinding_sell() -> None:
    info = MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE")
    context = QuoteContext(
        market=info,
        config=MakerQuoteConfig(
            order_size_usd=Decimal("40"),
            maximum_position_usd=Decimal("40"),
            maker_fee_bps=Decimal("1.5"),
            minimum_edge_bps=Decimal("2"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("10"),
        ),
        fair_price=Decimal("83000"),
        best_bid=Decimal("83050"),
        best_ask=Decimal("83100"),
        position=Decimal("0.00048207"),
    )
    quotes = calculate_quotes(context)
    assert len(quotes) == 1
    assert quotes[0].side == "SELL"
    assert quotes[0].price >= Decimal("83037.35")


def test_per_order_cap_prevents_dust_cleanup_from_oversizing() -> None:
    # Given a long position slightly larger than standard order size by a dust amount.
    mapping = MARKET_MAPPINGS["BTC-USD"]
    info = MarketInfo(mapping=mapping, status="ONLINE")
    settings = MakerQuoteConfig(
        order_size_usd=Decimal("40"),
        maximum_position_usd=Decimal("400"),
        maker_fee_bps=Decimal("0"),
        minimum_edge_bps=Decimal("3"),
        latency_buffer_bps=Decimal("2"),
        inventory_skew_bps=Decimal("0"),
    )
    fair = Decimal("83075")
    # 40 / 83075 = 0.00048149 BTC.
    # Suppose existing position was bought earlier at 82933.7 for 0.00048207 BTC.
    # The remainder if selling desired_quantity would be 0.00000058 BTC (< min_order_size 0.0001).
    context = QuoteContext(
        market=info,
        config=settings,
        fair_price=fair,
        best_bid=Decimal("83050"),
        best_ask=Decimal("83100"),
        position=Decimal("0.00048207"),
    )

    # When calculating the quote.
    quotes = calculate_quotes(context)
    sell_quote = next(q for q in quotes if q.side == "SELL")

    # Then the hard notional cap wins over flattening the final dust amount.
    assert sell_quote.quantity < context.position
    assert sell_quote.quantity * sell_quote.price <= settings.order_size_usd


def test_projected_position_allows_reducing_side_when_over_limit() -> None:
    # Given a long position that exceeds the maximum USD limit ($100 cap in _runtime).
    client = _FakeMakerClient()
    client.state.position = Decimal("0.002")  # ~$166 at $83,000, exceeding $100 limit
    manager = MakerOrderManager(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=_FakeMakerOrders(client),
    )

    fair = Decimal("83000")
    # BUY should be blocked since it increases long exposure beyond limit.
    assert manager.projected_position_exceeds_limit(fair, side="BUY", candidate_quantity=Decimal("0.00012"))
    # SELL should be allowed because it reduces the excessive long position.
    assert not manager.projected_position_exceeds_limit(fair, side="SELL", candidate_quantity=Decimal("0.00012"))


def test_reconcile_refuses_unmanaged_orders_without_canceling() -> None:
    # Both an external order and a desired quote exist on the selected market.
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client)
    client.state.open_orders.add("external-order-1")
    manager = MakerOrderManager(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=orders,
    )
    desired = Quote(side="BUY", price=Decimal("82950"), quantity=Decimal("0.0002"))

    with pytest.raises(ProtocolError, match="unmanaged open orders; refusing to take ownership"):
        anyio.run(
            manager.reconcile,
            (desired,),
            Decimal("83000"),
            Decimal("82990"),
            Decimal("83010"),
        )

    assert not orders.cancelled
    assert not orders.placed
    assert client.state.open_orders == {"external-order-1"}
    assert not manager.tracked_orders


def test_row_position_handles_signed_arcus_sizes() -> None:
    # Arcus sends size as negative for SHORT positions (e.g. "-0.0415")
    state = AccountState(market_id=1)
    state.apply(
        {
            "type": "subscribed",
            "channel": "positions",
            "contents": {
                "positions": {
                    "1": {"side": "SHORT", "size": "-0.0415"}
                }
            },
        }
    )
    assert state.position == Decimal("-0.0415")


def _valid_go_report(tmp_path: Path) -> JsonObject:
    """Build a source-backed GO for runtime wiring tests, not empirical certification."""
    now_ms = int(time.time() * 1000)
    public: list[JsonObject] = [
        {
            "recv_time_ns": i + 1,
            "event_time_ms": now_ms - 2_000 + i * 10,
            "symbol": "BTCUSDT",
            "venue": "binance",
            "b": "100",
            "a": "102",
            "B": "1",
            "A": "1",
        }
        for i in range(120)
    ]
    fills: list[JsonObject] = [
        {
            "recv_time_ns": i + 1,
            "event_time_ms": now_ms - 1_800 + i * 10,
            "symbol": "BTCUSDT",
            "venue": "binance",
            "policy": policy,
            "side": "BUY",
            "price": price,
            "quantity": "1",
            "fee": "0",
            "source": "observed",
            "provenance": "observed",
            "action_latency_source": "measured",
            "action_latency_ms": 1,
            "action_sent_time_ms": now_ms - 1_801 + i * 10,
        }
        for policy, price in (("baseline", "100"), ("candidate", "99"))
        for i in range(30)
    ]
    public_path = tmp_path / "public.jsonl"
    fills_path = tmp_path / "fills.jsonl"
    for path, records in ((public_path, public), (fills_path, fills)):
        _ = path.write_text("".join(json.dumps(record) + "\n" for record in records))
    report = evaluate_replay(public, fills)
    assert report["decision"] == "GO"
    report["generated_at_ms"] = now_ms
    report["data_provenance"] = {
        "public_path": str(public_path.resolve()),
        "public_sha256": hashlib.sha256(public_path.read_bytes()).hexdigest(),
        "fills_path": str(fills_path.resolve()),
        "fills_sha256": hashlib.sha256(fills_path.read_bytes()).hexdigest(),
    }
    accepted, reason, _ = verify_authentic_go_report(report_data=report)
    assert accepted, reason
    return report


def _make_test_maker(
    candidate_mode: str = "off",
    max_alpha_bps: Decimal | None = None,
    alpha_report_data: JsonObject | None = None,
    submit: bool = False,
) -> ContinuousMaker:
    client = _FakeMakerClient()
    orders = _FakeMakerOrders(client) if submit else None
    mapping = MARKET_MAPPINGS["BTC-USD"]
    bps = Decimal("0") if max_alpha_bps is None else max_alpha_bps
    runtime = replace(
        _runtime(submit=submit),
        candidate_mode=candidate_mode,
        max_alpha_bps=bps,
        alpha_report_data=alpha_report_data,
    )
    feed = BinanceBookTickerFeed("BTCUSDT")
    maker = ContinuousMaker(
        runtime=runtime,
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        feed=feed,
        orders=orders,
    )
    return maker


@pytest.mark.parametrize("mode", ["off", "shadow"])
def test_cli_options_reach_market_runtime(
    mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Exercise the CLI parser and the actual per-market runtime construction.
    argv = [
        "--markets", "BTC-USD",
        "--order-size-usd", "100",
        "--max-position-usd", "500",
        "--maker-fee-bps", "1.5",
        "--minimum-edge-bps", "2",
        "--latency-buffer-bps", "1",
        "--inventory-skew-bps", "10",
        "--max-basis-bps", "10000",
        "--dry-run",
        *(["--candidate-mode", mode] if mode == "shadow" else []),
    ]
    options = parse_options(argv)
    seen: list[MakerRuntime] = []

    async def fake_market_info(mapping: object, mainnet: bool) -> MarketInfo:
        assert mapping == MARKET_MAPPINGS["BTC-USD"]
        assert not mainnet
        return MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE")

    async def capture_runtime(runtime: MakerRuntime, market: MarketInfo) -> None:
        assert market.mapping == MARKET_MAPPINGS["BTC-USD"]
        seen.append(runtime)

    monkeypatch.setattr("arcus_bot.cli.maker.fetch_market_info", fake_market_info)
    monkeypatch.setattr("arcus_bot.cli.maker.run_market", capture_runtime)
    monkeypatch.setattr(sys, "argv", ["arcus-maker", *argv])
    assert maker_main() == 0

    assert len(seen) == 1
    assert seen[0].candidate_mode == mode
    assert seen[0].max_alpha_bps == options.max_alpha_bps
    assert seen[0].alpha_report_path == options.alpha_report_path
    assert seen[0].alpha_report_data == options.alpha_report_data
    assert not seen[0].submit
    assert ContinuousMaker(
        runtime=seen[0],
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=_FakeMakerClient(),
        feed=BinanceBookTickerFeed("BTCUSDT"),
        orders=None,
    ).candidate_mode == mode


def test_candidate_off_equals_baseline() -> None:
    now_ns = 100_000_000_000
    maker_off = _make_test_maker(candidate_mode="off")
    maker_off.client.orderbook.best_bid = Decimal("83000")
    maker_off.client.orderbook.best_ask = Decimal("83010")
    maker_off.client.orderbook.received_at_ns = now_ns
    maker_off.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}', "BTCUSDT", now_ns
    )
    for i in range(1, 5):
        maker_off.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    quotes_off = anyio.run(maker_off.step, now_ns)

    baseline_fair = Decimal("83005")
    baseline_quotes = maker_off.order_manager.quotes(
        baseline_fair, Decimal("83000"), Decimal("83010")
    )
    assert quotes_off is not None
    assert quotes_off == baseline_quotes
    assert len(quotes_off) > 0


def test_candidate_shadow_mode() -> None:
    now_ns = 100_000_000_000
    maker_shadow = _make_test_maker(candidate_mode="shadow", submit=True)
    maker_shadow.client.orderbook.best_bid = Decimal("83000")
    maker_shadow.client.orderbook.best_ask = Decimal("83010")
    maker_shadow.client.orderbook.received_at_ns = now_ns
    maker_shadow.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}', "BTCUSDT", now_ns
    )
    for i in range(1, 5):
        maker_shadow.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    quotes_shadow = anyio.run(maker_shadow.step, now_ns)

    baseline_fair = Decimal("83005")
    baseline_quotes = maker_shadow.order_manager.quotes(
        baseline_fair, Decimal("83000"), Decimal("83010")
    )
    assert quotes_shadow is not None
    assert quotes_shadow == baseline_quotes

    assert "SHADOW BTC-USD" in maker_shadow.last_shadow_log
    assert "baseline=" in maker_shadow.last_shadow_log
    assert "candidate=" in maker_shadow.last_shadow_log
    assert "offset=+2.5000" in maker_shadow.last_shadow_log
    assert "health=healthy" in maker_shadow.last_shadow_log

    assert maker_shadow.orders is not None
    assert len(maker_shadow.order_manager.tracked_orders) >= 1
    for tracked_q in maker_shadow.order_manager.tracked_orders.values():
        assert tracked_q in baseline_quotes
        assert tracked_q.price in (Decimal("82971.7"), Decimal("83038.3"))


def test_candidate_bounded_mode(tmp_path: Path) -> None:
    now_ns = 100_000_000_000
    # Part 1: Valid authentic GO report -> applies clamped candidate offset
    maker_bounded = _make_test_maker(
        candidate_mode="bounded",
        max_alpha_bps=Decimal("5"),
        alpha_report_data=_valid_go_report(tmp_path),
    )
    maker_bounded.client.orderbook.best_bid = Decimal("83000")
    maker_bounded.client.orderbook.best_ask = Decimal("83010")
    maker_bounded.client.orderbook.received_at_ns = now_ns
    maker_bounded.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}', "BTCUSDT", now_ns
    )
    for i in range(1, 5):
        maker_bounded.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    quotes_bounded = anyio.run(maker_bounded.step, now_ns)

    baseline_fair = Decimal("83005")
    baseline_quotes = maker_bounded.order_manager.quotes(
        baseline_fair, Decimal("83000"), Decimal("83010")
    )
    assert quotes_bounded is not None
    assert quotes_bounded != baseline_quotes

    # Part 2: Report absent -> falls back to baseline fair price
    maker_no_report = _make_test_maker(
        candidate_mode="bounded",
        max_alpha_bps=Decimal("5"),
        alpha_report_data=None,
    )
    maker_no_report.client.orderbook.best_bid = Decimal("83000")
    maker_no_report.client.orderbook.best_ask = Decimal("83010")
    maker_no_report.client.orderbook.received_at_ns = now_ns
    maker_no_report.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}', "BTCUSDT", now_ns
    )
    for i in range(1, 5):
        maker_no_report.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    quotes_fallback = anyio.run(maker_no_report.step, now_ns)
    assert quotes_fallback is not None
    assert quotes_fallback == baseline_quotes
    assert "no alpha evaluation report provided" in maker_no_report.last_non_go_reason

    # Part 3: CLI option validation: bounded requires authentic GO report
    with pytest.raises(InputError, match="bounded candidate mode requires a valid authentic GO"):
        _ = parse_options([
            "--markets", "BTC-USD",
            "--order-size-usd", "20",
            "--max-position-usd", "100",
            "--maker-fee-bps", "1",
            "--minimum-edge-bps", "2",
            "--latency-buffer-bps", "1",
            "--inventory-skew-bps", "5",
            "--max-basis-bps", "50",
            "--candidate-mode", "bounded",
        ])


def test_bounded_candidate_mode(tmp_path: Path) -> None:
    test_candidate_bounded_mode(tmp_path)


def test_candidate_stale_falls_back_to_baseline(tmp_path: Path) -> None:
    now_ns = 100_000_000_000
    maker = _make_test_maker(
        candidate_mode="bounded",
        max_alpha_bps=Decimal("5"),
        alpha_report_data=_valid_go_report(tmp_path),
    )
    # Stale candidate feed (>1s old)
    cand_feed = BinanceBookTickerFeed("BTCUSDT")
    cand_feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns - 5_000_000_000,
    )
    maker.candidate_feed = cand_feed
    maker.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns,
    )
    maker.client.orderbook.best_bid = Decimal("83000")
    maker.client.orderbook.best_ask = Decimal("83010")
    maker.client.orderbook.received_at_ns = now_ns
    for i in range(1, 5):
        maker.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    quotes = anyio.run(maker.step, now_ns)

    baseline_fair = Decimal("83005")
    baseline_quotes = maker.order_manager.quotes(
        baseline_fair, Decimal("83000"), Decimal("83010")
    )
    assert quotes is not None
    assert quotes == baseline_quotes
    assert maker.last_non_go_reason == "candidate feed is stale"


def test_candidate_no_go_falls_back_to_baseline(tmp_path: Path) -> None:
    now_ns = 100_000_000_000
    no_go_report = dict(_valid_go_report(tmp_path), decision="NO_GO", reason="Adverse cost stress failed")
    maker = _make_test_maker(
        candidate_mode="bounded",
        max_alpha_bps=Decimal("5"),
        alpha_report_data=no_go_report,
    )
    maker.client.orderbook.best_bid = Decimal("83000")
    maker.client.orderbook.best_ask = Decimal("83010")
    maker.client.orderbook.received_at_ns = now_ns
    maker.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns,
    )
    for i in range(1, 5):
        maker.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    quotes = anyio.run(maker.step, now_ns)

    baseline_fair = Decimal("83005")
    baseline_quotes = maker.order_manager.quotes(
        baseline_fair, Decimal("83000"), Decimal("83010")
    )
    assert quotes is not None
    assert quotes == baseline_quotes
    assert "alpha report decision is NO_GO" in maker.last_non_go_reason


def test_baseline_stale_pauses_quotes(tmp_path: Path) -> None:
    now_ns = 100_000_000_000
    maker = _make_test_maker(
        candidate_mode="bounded",
        max_alpha_bps=Decimal("5"),
        alpha_report_data=_valid_go_report(tmp_path),
        submit=True,
    )
    maker.client.state.open_orders.add("open-order-1")
    maker.order_manager.tracked_orders["open-order-1"] = Quote(
        side="BUY",
        price=Decimal("83000"),
        quantity=Decimal("0.0001"),
    )
    cand_feed = BinanceBookTickerFeed("BTCUSDT")
    cand_feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns,
    )
    maker.candidate_feed = cand_feed
    # Stale baseline feed (>2s old)
    maker.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns - 4_000_000_000,
    )
    maker.client.orderbook.best_bid = Decimal("83000")
    maker.client.orderbook.best_ask = Decimal("83010")
    maker.client.orderbook.received_at_ns = now_ns

    result = anyio.run(maker.step, now_ns)

    assert result is None
    assert maker.last_status == "Binance reference is stale"
    assert isinstance(maker.orders, _FakeMakerOrders)
    assert "open-order-1" in maker.orders.cancelled


def test_live_submission_supports_continuous_mode_and_rejects_negative_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given baseline CLI arguments with --submit and continuous 24/7 settings.
    monkeypatch.setenv("ARCUS_API_SIGNING_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
    base_args = [
        "--markets", "BTC-USD",
        "--account-address", "0x1234567890abcdef1234567890abcdef12345678",
        "--order-size-usd", "10",
        "--max-position-usd", "40",
        "--maker-fee-bps", "0",
        "--minimum-edge-bps", "3",
        "--latency-buffer-bps", "2",
        "--inventory-skew-bps", "10",
        "--max-basis-bps", "25",
        "--submit",
    ]

    # When duration is omitted or zero, it parses as continuous (duration_seconds=0).
    options = parse_options(base_args)
    assert options.duration_seconds == 0
    assert options.max_traded_notional_usd is None
    assert options.max_loss_usd is None

    # When duration is negative, it is rejected.
    with pytest.raises(InputError, match="--duration-seconds cannot be negative"):
        _ = parse_options([*base_args, "--duration-seconds", "-1"])


def test_live_submission_restricts_to_btc_usd() -> None:
    # Given live submission arguments attempting to trade ETH-USD.
    argv = [
        "--markets", "ETH-USD",
        "--account-address", "0x1234567890abcdef1234567890abcdef12345678",
        "--order-size-usd", "10",
        "--max-position-usd", "40",
        "--maker-fee-bps", "0",
        "--minimum-edge-bps", "3",
        "--latency-buffer-bps", "2",
        "--inventory-skew-bps", "10",
        "--max-basis-bps", "25",
        "--duration-seconds", "60",
        "--max-traded-notional-usd", "100",
        "--max-loss-usd", "10",
        "--submit",
    ]

    # When parsing a non-BTC live submission.
    # Then it is rejected immediately.
    with pytest.raises(InputError, match="--submit is restricted to BTC-USD"):
        _ = parse_options(argv)


def test_live_submission_loss_limit_cannot_exceed_max_position() -> None:
    # Given live submission arguments with max loss greater than max position.
    argv = [
        "--markets", "BTC-USD",
        "--account-address", "0x1234567890abcdef1234567890abcdef12345678",
        "--order-size-usd", "10",
        "--max-position-usd", "40",
        "--maker-fee-bps", "0",
        "--minimum-edge-bps", "3",
        "--latency-buffer-bps", "2",
        "--inventory-skew-bps", "10",
        "--max-basis-bps", "25",
        "--duration-seconds", "60",
        "--max-traded-notional-usd", "100",
        "--max-loss-usd", "50",
        "--submit",
    ]

    # When parsing options.
    # Then loss limit cannot be configured looser than total position.
    with pytest.raises(InputError, match="--max-loss-usd cannot exceed --max-position-usd"):
        _ = parse_options(argv)


def test_live_step_fails_closed_when_turnover_cap_reached() -> None:
    # Given a live maker session whose cumulative traded notional hits the cap.
    now_ns = 100_000_000_000
    runtime = replace(_runtime(submit=True), max_traded_notional_usd=Decimal("50"))
    maker = _make_test_maker(submit=True)
    maker.runtime = runtime
    maker.order_manager.runtime = runtime
    maker.client.state.cumulative_traded_notional_usd = Decimal("50")
    maker.client.orderbook.best_bid = Decimal("83000")
    maker.client.orderbook.best_ask = Decimal("83010")
    maker.client.orderbook.received_at_ns = now_ns
    maker.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns,
    )
    for i in range(1, 5):
        maker.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    # When executing a quote step.
    # Then it raises a ProtocolError and refuses further orders.
    with pytest.raises(ProtocolError, match="maximum cumulative traded notional reached"):
        _ = anyio.run(maker.step, now_ns)


def test_live_step_fails_closed_when_loss_limit_reached() -> None:
    # Given a live maker session where account equity drops by the loss limit.
    now_ns = 100_000_000_000
    runtime = replace(_runtime(submit=True), max_loss_usd=Decimal("5"))
    maker = _make_test_maker(submit=True)
    maker.runtime = runtime
    maker.order_manager.runtime = runtime
    maker.starting_equity = Decimal("100")
    maker.client.state.account_equity = Decimal("94.5")
    maker.client.state.account_updated_at_ns = now_ns
    maker.client.orderbook.best_bid = Decimal("83000")
    maker.client.orderbook.best_ask = Decimal("83010")
    maker.client.orderbook.received_at_ns = now_ns
    maker.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns,
    )
    for i in range(1, 5):
        maker.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    # When executing a quote step.
    # Then it raises a ProtocolError and halts.
    with pytest.raises(ProtocolError, match="maximum account-equity drawdown reached"):
        _ = anyio.run(maker.step, now_ns)


def test_live_step_fails_closed_when_account_equity_is_stale() -> None:
    # Given a live maker session whose account equity has not updated within 5 seconds.
    now_ns = 100_000_000_000
    runtime = _runtime(submit=True)
    maker = _make_test_maker(submit=True)
    maker.runtime = runtime
    maker.order_manager.runtime = runtime
    maker.client.state.account_equity = Decimal("100")
    maker.client.state.account_updated_at_ns = now_ns - 6_000_000_000
    maker.client.orderbook.best_bid = Decimal("83000")
    maker.client.orderbook.best_ask = Decimal("83010")
    maker.client.orderbook.received_at_ns = now_ns
    maker.feed.latest = parse_book_ticker(
        '{"s":"BTCUSDT","b":"83000","a":"83010","B":"3.0","A":"1.0"}',
        "BTCUSDT",
        now_ns,
    )
    for i in range(1, 5):
        maker.basis.add_sample(
            Decimal("83005"), Decimal("83005"), now_ns - (5 - i) * 1_000_000_000
        )

    # When executing a quote step.
    # Then it raises a ProtocolError and pauses quotes.
    with pytest.raises(ProtocolError, match="Arcus account-equity update is stale"):
        _ = anyio.run(maker.step, now_ns)
