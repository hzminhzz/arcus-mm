from __future__ import annotations

from decimal import Decimal

import pytest

from arcus_bot.bots.market_maker.market import MARKET_MAPPINGS, MarketInfo
from arcus_bot.bots.market_maker.order_manager import MakerOrderManager
from arcus_bot.bots.market_maker.quoter import MakerQuoteConfig, QuoteContext, calculate_quotes
from arcus_bot.bots.market_maker.runtime import MakerRuntime
from arcus_bot.sdk.account import AccountState
from arcus_bot.sdk.orderbook import OrderbookState
from arcus_bot.types import AccountRef, JsonObject, JsonValue, OrderRules, OrderState, ProtocolError


class _MockClient:
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
        raise AssertionError("not used in mock tests")

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None:
        _ = (order_id, seconds)
        return OrderState(
            status="FILLED",
            filled_quantity=Decimal("0.1"),
            average_fill_price=Decimal("120.0"),
        )


class _MockOrders:
    async def place(self, side: str, price: Decimal, quantity: Decimal, rules: OrderRules | None = None) -> str:
        _ = (side, price, quantity, rules)
        return "order-123"

    async def cancel(self, order_id: str) -> None:
        raise ProtocolError(f"Arcus rejected cancelOrder: order {order_id} already filled")


def test_quoter_caps_reduce_only_quantity_to_position() -> None:
    info = MarketInfo(mapping=MARKET_MAPPINGS["HOOD-USD"], status="ONLINE")
    config = MakerQuoteConfig(
        order_size_usd=Decimal("50"),
        maximum_position_usd=Decimal("500"),
        maker_fee_bps=Decimal("0"),
        minimum_edge_bps=Decimal("1"),
        latency_buffer_bps=Decimal("1"),
        inventory_skew_bps=Decimal("10"),
    )
    ctx = QuoteContext(
        market=info,
        config=config,
        fair_price=Decimal("120"),
        best_bid=Decimal("119.8"),
        best_ask=Decimal("120.2"),
        position=Decimal("0.15"),
    )
    quotes = calculate_quotes(ctx)
    sell_quote = [q for q in quotes if q.side == "SELL"][0]

    assert sell_quote.reduce_only is True
    assert sell_quote.quantity <= Decimal("0.15")


def test_quoter_sub_minimum_dust_not_marked_reduce_only() -> None:
    info = MarketInfo(mapping=MARKET_MAPPINGS["HOOD-USD"], status="ONLINE")
    config = MakerQuoteConfig(
        order_size_usd=Decimal("50"),
        maximum_position_usd=Decimal("500"),
        maker_fee_bps=Decimal("0"),
        minimum_edge_bps=Decimal("1"),
        latency_buffer_bps=Decimal("1"),
        inventory_skew_bps=Decimal("10"),
    )
    ctx = QuoteContext(
        market=info,
        config=config,
        fair_price=Decimal("120"),
        best_bid=Decimal("119.8"),
        best_ask=Decimal("120.2"),
        position=Decimal("0.05"),
    )
    quotes = calculate_quotes(ctx)
    for q in quotes:
        assert q.reduce_only is False


def test_orderbook_clears_instantly_on_empty_bids_or_asks() -> None:
    ob = OrderbookState()
    ob.apply({
        "channel": "l2Orderbook",
        "contents": {
            "bids": [["120.0", "1.0"]],
            "asks": [["120.1", "1.0"]],
        }
    })
    b, a = ob.current()
    assert b == Decimal("120.0")
    assert a == Decimal("120.1")

    ob.apply({
        "channel": "l2Orderbook",
        "contents": {
            "bids": [],
            "asks": [["120.1", "1.0"]],
        }
    })
    with pytest.raises(ProtocolError, match="unavailable"):
        _ = ob.current()


@pytest.mark.anyio
async def test_cancel_and_confirm_handles_already_filled_order() -> None:
    mapping = MARKET_MAPPINGS["HOOD-USD"]
    runtime = MakerRuntime(
        account=AccountRef("0x1234", 0, mapping.market_id, mapping.market),
        quote_config=MakerQuoteConfig(
            order_size_usd=Decimal("50"),
            maximum_position_usd=Decimal("500"),
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("1"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("10"),
        ),
        submit=True,
        signing_key="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        run_id="test",
        maximum_basis_bps=Decimal("50"),
    )
    client = _MockClient(market_id=mapping.market_id)
    orders = _MockOrders()
    manager = MakerOrderManager(
        runtime=runtime,
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        orders=orders,
    )
    await manager.cancel_and_confirm("order-123")


@pytest.mark.anyio
async def test_emergency_flattener_aborts_safely_on_crossed_or_zero_bbo() -> None:
    mapping = MARKET_MAPPINGS["HOOD-USD"]
    runtime = MakerRuntime(
        account=AccountRef("0x1234", 0, mapping.market_id, mapping.market),
        quote_config=MakerQuoteConfig(
            order_size_usd=Decimal("50"),
            maximum_position_usd=Decimal("100"),
            maker_fee_bps=Decimal("0"),
            minimum_edge_bps=Decimal("1"),
            latency_buffer_bps=Decimal("1"),
            inventory_skew_bps=Decimal("10"),
        ),
        submit=True,
        signing_key="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        run_id="test",
        maximum_basis_bps=Decimal("50"),
        emergency_flatten_ratio=Decimal("1.10"),
    )
    client = _MockClient(market_id=mapping.market_id)
    client.state.position = Decimal("2.0")
    orders = _MockOrders()
    manager = MakerOrderManager(
        runtime=runtime,
        market=MarketInfo(mapping=mapping, status="ONLINE"),
        client=client,
        orders=orders,
    )
    desired = ()
    await manager.reconcile(desired, Decimal("120"), Decimal("121"), Decimal("119"))
