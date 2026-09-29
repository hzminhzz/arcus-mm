from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from arcus_bot.bots.market_maker.market import MARKET_MAPPINGS, MarketInfo
from arcus_bot.bots.market_maker.order_manager import MakerOrderManager
from arcus_bot.bots.market_maker.quoter import MakerQuoteConfig, Quote, QuoteContext, calculate_quotes
from arcus_bot.bots.market_maker.runtime import MakerRuntime
from arcus_bot.sdk.account import AccountState
from arcus_bot.sdk.orderbook import OrderbookState
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import AccountRef, JSON_ADAPTER, JsonObject, JsonValue, OrderConfig, OrderRules, OrderState


class _FakeMakerClient:
    orderbook: OrderbookState
    state: AccountState

    def __init__(self, market_id: int = 8) -> None:
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
        raise AssertionError("not used in mock tests")

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None:
        _ = (order_id, seconds)
        return OrderState(
            status="CANCELED",
            filled_quantity=Decimal("0"),
            average_fill_price=Decimal("0"),
        )


@dataclass(slots=True)
class _PlacedRecord:
    order_id: str
    side: str
    price: Decimal
    quantity: Decimal
    rules: OrderRules | None


class _RecordingOrders:
    placed: list[_PlacedRecord]
    cancelled: list[str]

    def __init__(self) -> None:
        self.placed = []
        self.cancelled = []

    async def place(
        self,
        side: str,
        price: Decimal,
        quantity: Decimal,
        rules: OrderRules | None = None,
    ) -> str:
        order_id = f"mock-order-{len(self.placed) + 1}"
        self.placed.append(_PlacedRecord(
            order_id=order_id,
            side=side,
            price=price,
            quantity=quantity,
            rules=rules,
        ))
        return order_id

    async def cancel(self, order_id: str) -> None:
        self.cancelled.append(order_id)


def _make_manager(
    position: Decimal,
    max_position_usd: Decimal | None = None,
    emergency_ratio: Decimal | None = None,
) -> tuple[MakerOrderManager, _FakeMakerClient, _RecordingOrders]:
    mapping = MARKET_MAPPINGS["ZEC-USD"]
    cap_usd = Decimal("500") if max_position_usd is None else max_position_usd
    ratio = Decimal("1.20") if emergency_ratio is None else emergency_ratio
    runtime = MakerRuntime(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=mapping.market_id,
            market=mapping.market,
        ),
        quote_config=MakerQuoteConfig(
            order_size_usd=Decimal("50"),
            maximum_position_usd=cap_usd,
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("3"),
            latency_buffer_bps=Decimal("2"),
            inventory_skew_bps=Decimal("10"),
        ),
        submit=True,
        signing_key="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        run_id="test",
        maximum_basis_bps=Decimal("50"),
        emergency_flatten_ratio=ratio,
        emergency_flatten_buffer_bps=Decimal("10"),
    )
    client = _FakeMakerClient(market_id=mapping.market_id)
    client.state.position = position
    orders = _RecordingOrders()
    manager = MakerOrderManager(
        runtime=runtime,
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        orders=orders,
    )
    return manager, client, orders


def test_order_signing_payload_reduce_only_and_tif() -> None:
    config = OrderConfig(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=8,
            market="ZEC-USD",
        ),
        tick_size=Decimal("0.001"),
        step_size=Decimal("0.000001"),
    )

    body_ioc, signed_ioc_bytes = ArcusOrders.build_place_payload(
        config=config,
        side="BUY",
        price=Decimal("1370.000"),
        quantity=Decimal("0.350000"),
        timestamp_ns=10_000_000_000,
        rules=OrderRules(
            tick_size=Decimal("0.001"),
            step_size=Decimal("0.000001"),
            client_id="emerg-1",
            reduce_only=True,
            time_in_force="IOC",
        ),
    )
    parsed_ioc = JSON_ADAPTER.validate_json(signed_ioc_bytes)
    assert isinstance(parsed_ioc, dict)
    assert parsed_ioc.get("r") == 1
    assert parsed_ioc.get("t") == 1
    assert body_ioc["reduceOnly"] is True
    assert body_ioc["timeInForce"] == "IOC"
    assert body_ioc["clientId"] == "emerg-1"

    body_alo, signed_alo_bytes = ArcusOrders.build_place_payload(
        config=config,
        side="SELL",
        price=Decimal("1370.000"),
        quantity=Decimal("0.350000"),
        timestamp_ns=10_000_000_000,
        rules=OrderRules(
            tick_size=Decimal("0.001"),
            step_size=Decimal("0.000001"),
            client_id="maker-1",
            reduce_only=False,
            time_in_force="ALO",
        ),
    )
    parsed_alo = JSON_ADAPTER.validate_json(signed_alo_bytes)
    assert isinstance(parsed_alo, dict)
    assert parsed_alo.get("r") == 0
    assert parsed_alo.get("t") == 3
    assert body_alo["reduceOnly"] is False
    assert body_alo["timeInForce"] == "ALO"


def test_calculate_quotes_tags_inventory_reducing_side() -> None:
    mapping = MARKET_MAPPINGS["ZEC-USD"]
    config = MakerQuoteConfig(
        order_size_usd=Decimal("50"),
        maximum_position_usd=Decimal("500"),
        maker_fee_bps=Decimal("0"),
        minimum_edge_bps=Decimal("3"),
        latency_buffer_bps=Decimal("2"),
        inventory_skew_bps=Decimal("10"),
    )
    info = MarketInfo(mapping=mapping, status="ONLINE")

    short_ctx = QuoteContext(
        market=info,
        config=MakerQuoteConfig(
            order_size_usd=Decimal("50"),
            maximum_position_usd=Decimal("500"),
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("1"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("2"),
        ),
        fair_price=Decimal("1370"),
        best_bid=Decimal("1368"),
        best_ask=Decimal("1372"),
        position=Decimal("-0.1"),
    )
    short_quotes = calculate_quotes(short_ctx)
    buy_quote = [q for q in short_quotes if q.side == "BUY"][0]
    sell_quote = [q for q in short_quotes if q.side == "SELL"][0]
    assert buy_quote.reduce_only is True
    assert sell_quote.reduce_only is False

    long_ctx = QuoteContext(
        market=info,
        config=MakerQuoteConfig(
            order_size_usd=Decimal("50"),
            maximum_position_usd=Decimal("500"),
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("1"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("2"),
        ),
        fair_price=Decimal("1370"),
        best_bid=Decimal("1368"),
        best_ask=Decimal("1372"),
        position=Decimal("0.1"),
    )
    long_quotes = calculate_quotes(long_ctx)
    buy_quote_long = [q for q in long_quotes if q.side == "BUY"][0]
    sell_quote_long = [q for q in long_quotes if q.side == "SELL"][0]
    assert buy_quote_long.reduce_only is False
    assert sell_quote_long.reduce_only is True

    flat_ctx = QuoteContext(
        market=info,
        config=config,
        fair_price=Decimal("1370"),
        best_bid=Decimal("1369"),
        best_ask=Decimal("1371"),
        position=Decimal("0"),
    )
    flat_quotes = calculate_quotes(flat_ctx)
    for q in flat_quotes:
        assert q.reduce_only is False


@pytest.mark.anyio
async def test_circuit_breaker_cancels_increasing_orders_when_exposure_exceeded() -> None:
    manager, client, orders = _make_manager(
        position=Decimal("-0.45"),
        max_position_usd=Decimal("500"),
    )
    fair_price = Decimal("1370")

    manager.tracked_orders["resting-sell-order"] = Quote(
        side="SELL",
        price=Decimal("1372"),
        quantity=Decimal("0.036"),
    )
    manager.placed_at_ns["resting-sell-order"] = 10_000_000_000
    client.state.open_orders.add("resting-sell-order")

    desired = manager.quotes(fair_price, Decimal("1369"), Decimal("1371"))
    await manager.reconcile(desired, fair_price, Decimal("1369"), Decimal("1371"))

    assert "resting-sell-order" in orders.cancelled
    assert "resting-sell-order" not in manager.tracked_orders


@pytest.mark.anyio
async def test_emergency_flattener_places_marketable_ioc_order() -> None:
    manager, _client, orders = _make_manager(
        position=Decimal("-0.703315"),
        max_position_usd=Decimal("500"),
        emergency_ratio=Decimal("1.20"),
    )
    fair_price = Decimal("1370")
    best_bid = Decimal("1369")
    best_ask = Decimal("1371")

    desired = manager.quotes(fair_price, best_bid, best_ask)
    await manager.reconcile(desired, fair_price, best_bid, best_ask)

    assert len(orders.placed) == 1
    placed_order = orders.placed[0]
    assert placed_order.side == "BUY"
    assert placed_order.rules is not None
    assert placed_order.rules.reduce_only is True
    assert placed_order.rules.time_in_force == "IOC"
    assert placed_order.price > best_ask
    max_allowed = Decimal("500") / fair_price
    expected_excess = Decimal("-0.703315").copy_abs() - max_allowed
    diff = abs(placed_order.quantity - expected_excess)
    assert diff < Decimal("0.0001")


@pytest.mark.anyio
async def test_circuit_breaker_forbids_exposure_increasing_candidate_placement() -> None:
    manager, client, orders = _make_manager(
        position=Decimal("-0.40"),
        max_position_usd=Decimal("500"),
        emergency_ratio=Decimal("1.50"),
    )
    # Set config edge and skew so reducing quote has positive edge
    manager.runtime = MakerRuntime(
        account=manager.runtime.account,
        quote_config=MakerQuoteConfig(
            order_size_usd=Decimal("50"),
            maximum_position_usd=Decimal("500"),
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("1"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("2"),
        ),
        submit=True,
        signing_key="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        run_id="test",
        maximum_basis_bps=Decimal("50"),
        emergency_flatten_ratio=Decimal("1.50"),
        emergency_flatten_buffer_bps=Decimal("10"),
    )
    fair_price = Decimal("1370")
    client.orderbook.best_bid = Decimal("1368")
    client.orderbook.best_ask = Decimal("1372")

    desired = manager.quotes(fair_price, Decimal("1368"), Decimal("1372"))
    await manager.reconcile(desired, fair_price, Decimal("1368"), Decimal("1372"))

    assert len(orders.placed) == 1
    placed = orders.placed[0]
    assert placed.side == "BUY"
    assert placed.rules is not None
    assert placed.rules.reduce_only is True


def test_cli_supports_per_market_overrides() -> None:
    from arcus_bot.cli.maker_config import parse_options
    argv = [
        "--markets", "BTC-USD,ZEC-USD,HOOD-USD,NVDA-USD",
        "--order-size-usd", "BTC-USD=35,ZEC-USD=15,HOOD-USD=20,NVDA-USD=30",
        "--max-position-usd", "BTC-USD=350,ZEC-USD=150,HOOD-USD=200,NVDA-USD=300",
        "--maker-fee-bps", "0",
        "--minimum-edge-bps", "1.5",
        "--latency-buffer-bps", "1.0",
        "--inventory-skew-bps", "BTC-USD=15,ZEC-USD=20,HOOD-USD=30,NVDA-USD=10",
        "--max-basis-bps", "50",
        "--dry-run",
    ]
    options = parse_options(argv)
    assert len(options.quote_configs) == 4
    assert options.quote_configs["BTC-USD"].order_size_usd == Decimal("35")
    assert options.quote_configs["BTC-USD"].maximum_position_usd == Decimal("350")
    assert options.quote_configs["BTC-USD"].inventory_skew_bps == Decimal("15")

    assert options.quote_configs["ZEC-USD"].order_size_usd == Decimal("15")
    assert options.quote_configs["ZEC-USD"].maximum_position_usd == Decimal("150")
    assert options.quote_configs["ZEC-USD"].inventory_skew_bps == Decimal("20")

    assert options.quote_configs["HOOD-USD"].order_size_usd == Decimal("20")
    assert options.quote_configs["HOOD-USD"].maximum_position_usd == Decimal("200")
    assert options.quote_configs["HOOD-USD"].inventory_skew_bps == Decimal("30")

    assert options.quote_configs["NVDA-USD"].order_size_usd == Decimal("30")
    assert options.quote_configs["NVDA-USD"].maximum_position_usd == Decimal("300")
    assert options.quote_configs["NVDA-USD"].inventory_skew_bps == Decimal("10")
