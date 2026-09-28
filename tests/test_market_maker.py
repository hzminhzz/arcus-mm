"""Deterministic tests for the continuous Arcus market maker."""

import json
from dataclasses import replace
from decimal import Decimal
from time import monotonic_ns
from types import SimpleNamespace

import anyio
import pytest

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
from arcus_bot.cli.maker_config import parse_options
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
    assert [(quote.side, quote.price, quote.quantity) for quote in quotes] == [
        ("BUY", Decimal("99.9"), Decimal("0.2")),
        ("SELL", Decimal("100.1"), Decimal("0.2")),
    ]


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


def test_startup_cancellation_only_targets_selected_market() -> None:
    # Given a BTC maker state whose snapshot includes an unrelated HYPE order.
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
    manager = MakerOrderManager(
        runtime=_runtime(submit=True),
        market=MarketInfo(mapping=MARKET_MAPPINGS["BTC-USD"], status="ONLINE"),
        client=client,
        orders=orders,
    )

    # When startup cleanup runs for this market.
    anyio.run(manager.cancel_existing_market_orders)

    # Then only the BTC order is passed to Arcus cancellation.
    assert orders.cancelled == ["btc-order"]


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
