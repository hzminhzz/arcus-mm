"""Behavior tests for multi-order grid planning and execution."""

from decimal import Decimal

import anyio
import pytest

from arcus_bot.bots.grid import (
    GridSettings,
    MakerGridBot,
    calculate_grid_quote,
)
from arcus_bot.sdk.account import AccountState
from arcus_bot.types import AccountRef, Config, JsonObject, OrderState


@pytest.fixture
def config() -> Config:
    """Provide a BTC grid with enough room for more than one order."""
    return Config(
        account=AccountRef(
            address="0x1234567890abcdef1234567890abcdef12345678",
            account_index=0,
            market_id=1,
            market="BTC-USD",
        ),
        side="BUY",
        quantity=Decimal("0.1"),
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.001"),
        take_profit_percent=Decimal("0.02"),
        max_order_notional=Decimal("1000"),
        max_total_volume=Decimal("5000"),
        cycles=1,
        wait_seconds=0,
        entry_timeout_seconds=30,
        mainnet=False,
    )


def test_buy_grid_places_next_exit_below_existing_exit(config: Config) -> None:
    # Given a long grid with a resting close order at 100.5.
    settings = GridSettings(
        max_orders=4,
        grid_step_percent=Decimal("0.5"),
        stop_price=Decimal("-1"),
        pause_price=Decimal("-1"),
    )

    # When calculating the next passive entry.
    quote = calculate_grid_quote(
        config,
        settings,
        Decimal("100"),
        Decimal("100.1"),
        (Decimal("100.5"),),
    )

    # Then its projected close is at least 0.5% below the existing close.
    assert quote is not None
    assert quote.side == "BUY"
    assert quote.take_profit_price == Decimal("99.9")
    assert quote.entry_price == Decimal("99.8")


def test_sell_grid_places_next_exit_above_existing_exit(config: Config) -> None:
    # Given a short grid with a resting close order at 102.
    sell_config = Config(
        account=config.account,
        side="SELL",
        quantity=config.quantity,
        tick_size=config.tick_size,
        step_size=config.step_size,
        take_profit_percent=config.take_profit_percent,
        max_order_notional=config.max_order_notional,
        max_total_volume=config.max_total_volume,
        cycles=config.cycles,
        wait_seconds=config.wait_seconds,
        entry_timeout_seconds=config.entry_timeout_seconds,
        mainnet=config.mainnet,
    )
    settings = GridSettings(
        max_orders=4,
        grid_step_percent=Decimal("0.5"),
        stop_price=Decimal("-1"),
        pause_price=Decimal("-1"),
    )

    # When calculating the next passive entry.
    quote = calculate_grid_quote(
        sell_config,
        settings,
        Decimal("100"),
        Decimal("100.1"),
        (Decimal("102"),),
    )

    # Then its projected close is at least 0.5% above the existing close.
    assert quote is not None
    assert quote.side == "SELL"
    assert quote.take_profit_price == Decimal("102.6")
    assert quote.entry_price == Decimal("102.7")


class _StopAfterFourMessages(Exception):
    """Stop after the strategy has a fourth chance to exceed its order bound."""


class _FakeOrderbook:
    """Expose a stable test book."""

    def current(self) -> tuple[Decimal, Decimal]:
        return Decimal("100"), Decimal("100.1")


class _FakeClient:
    """Feed a finite number of market updates to the real strategy loop."""

    state: AccountState
    orderbook: _FakeOrderbook
    messages: int

    def __init__(self) -> None:
        self.state = AccountState(market_id=1)
        self.state.position_ready = True
        self.orderbook = _FakeOrderbook()
        self.messages = 0

    async def subscribe_maker_channels(self) -> None:
        return None

    async def next_message(self) -> JsonObject:
        self.messages += 1
        if self.messages in {1, 2}:
            entry_id = "ord-1" if self.messages == 1 else "ord-3"
            self.state.order_states[entry_id] = OrderState(
                status="FILLED",
                filled_quantity=Decimal("0.1"),
                average_fill_price=Decimal("100"),
            )
        if self.messages == 4:
            raise _StopAfterFourMessages
        return {}

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None:
        assert seconds > 0
        return self.state.order_states.get(order_id)


class _FakeOrders:
    """Record entry orders and terminally cancel them during test cleanup."""

    state: AccountState
    placed: list[tuple[str, Decimal, Decimal]]
    next_id: int

    def __init__(self, state: AccountState) -> None:
        self.state = state
        self.placed = []
        self.next_id = 0

    async def place(self, side: str, price: Decimal, quantity: Decimal) -> str:
        self.next_id += 1
        self.placed.append((side, price, quantity))
        return f"ord-{self.next_id}"

    async def cancel(self, order_id: str) -> None:
        self.state.order_states[order_id] = OrderState(
            status="CANCELED",
            filled_quantity=Decimal(0),
            average_fill_price=Decimal(0),
        )


def test_grid_pairs_fills_and_respects_order_bound(config: Config) -> None:
    # Given a flat account, a four-order limit, and two filled entries.
    client = _FakeClient()
    orders = _FakeOrders(client.state)
    settings = GridSettings(
        max_orders=4,
        grid_step_percent=Decimal(0),
        stop_price=Decimal("-1"),
        pause_price=Decimal("-1"),
    )
    bot = MakerGridBot(config, settings, client, orders)

    # When the stream advances through four placement opportunities.
    with pytest.raises(_StopAfterFourMessages):
        _ = anyio.run(bot.run)

    # Then each fill has an exit and the grid does not post a fourth entry.
    assert orders.placed == [
        ("BUY", Decimal("100"), Decimal("0.1")),
        ("SELL", Decimal("100.1"), Decimal("0.1")),
        ("BUY", Decimal("100"), Decimal("0.1")),
        ("SELL", Decimal("100.1"), Decimal("0.1")),
        ("BUY", Decimal("100"), Decimal("0.1")),
    ]
    assert not bot.entries
    assert len(bot.exits) == 2
