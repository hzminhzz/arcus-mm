"""Run a directional, bounded grid of passive entry and take-profit orders."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from time import monotonic
from typing import Literal, Protocol

import anyio

from arcus_bot.bots.cycle.quoter import (
    passive_entry_price,
    passive_take_profit_price,
)
from arcus_bot.sdk.account import TERMINAL_STATES
from arcus_bot.sdk.account import AccountState
from arcus_bot.types import Config, InputError, JsonObject, OrderState, ProtocolError

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GridSettings:
    """Controls the number and spacing of live grid orders."""

    max_orders: int
    grid_step_percent: Decimal
    stop_price: Decimal
    pause_price: Decimal


@dataclass(frozen=True, slots=True)
class GridQuote:
    """One passive entry and its projected take-profit level."""

    side: Literal["BUY", "SELL"]
    entry_price: Decimal
    take_profit_price: Decimal


@dataclass(frozen=True, slots=True)
class PendingEntry:
    """An exchange entry order and its projected close price."""

    side: Literal["BUY", "SELL"]
    take_profit_price: Decimal
    placed_at: float


@dataclass(frozen=True, slots=True)
class PendingExit:
    """An exchange take-profit order and its remaining quantity."""

    side: Literal["BUY", "SELL"]
    price: Decimal
    quantity: Decimal


class GridOrderbook(Protocol):
    """Read-only best bid and ask required by the grid."""

    def current(self) -> tuple[Decimal, Decimal]: ...


class GridClient(Protocol):
    """Exchange stream operations used by the strategy."""

    state: AccountState

    @property
    def orderbook(self) -> GridOrderbook: ...

    async def subscribe_maker_channels(self) -> None: ...

    async def next_message(self) -> JsonObject: ...

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None: ...


class GridOrders(Protocol):
    """Signed order operations used by the strategy."""

    async def place(self, side: str, price: Decimal, quantity: Decimal) -> str: ...

    async def cancel(self, order_id: str) -> None: ...


def calculate_grid_quote(
    config: Config,
    settings: GridSettings,
    best_bid: Decimal,
    best_ask: Decimal,
    take_profit_levels: tuple[Decimal, ...],
) -> GridQuote | None:
    """Choose the next entry whose projected exit respects the grid spacing."""
    match config.side:
        case "BUY":
            side: Literal["BUY", "SELL"] = "BUY"
        case "SELL":
            side = "SELL"
        case _:
            raise InputError("side must be BUY or SELL")

    entry_price = passive_entry_price(side, best_bid, best_ask)
    if settings.grid_step_percent > 0 and take_profit_levels:
        step = settings.grid_step_percent / Decimal(100)
        if side == "BUY":
            highest_allowed_exit = min(take_profit_levels) * (1 - step)
            highest_aligned_exit = (
                highest_allowed_exit / config.tick_size
            ).to_integral_value(rounding=ROUND_FLOOR) * config.tick_size
            unaligned_entry = highest_aligned_exit / (
                1 + config.take_profit_percent / Decimal(100)
            )
            entry_price = min(
                entry_price,
                (unaligned_entry / config.tick_size).to_integral_value(
                    rounding=ROUND_FLOOR
                )
                * config.tick_size,
            )
        else:
            lowest_allowed_exit = max(take_profit_levels) * (1 + step)
            lowest_aligned_exit = (
                lowest_allowed_exit / config.tick_size
            ).to_integral_value(rounding=ROUND_CEILING) * config.tick_size
            unaligned_entry = lowest_aligned_exit / (
                1 - config.take_profit_percent / Decimal(100)
            )
            entry_price = max(
                entry_price,
                (unaligned_entry / config.tick_size).to_integral_value(
                    rounding=ROUND_CEILING
                )
                * config.tick_size,
            )

    if (
        entry_price <= 0
        or entry_price * config.quantity < Decimal(5)
        or entry_price * config.quantity > config.max_order_notional
    ):
        return None
    target = entry_price * (
        1 + config.take_profit_percent / Decimal(100)
        if side == "BUY"
        else 1 - config.take_profit_percent / Decimal(100)
    )
    take_profit_price = (
        (target / config.tick_size).to_integral_value(
            rounding=ROUND_CEILING if side == "BUY" else ROUND_FLOOR
        )
        * config.tick_size
    )
    if take_profit_price * config.quantity > config.max_order_notional:
        return None
    if settings.grid_step_percent > 0 and take_profit_levels:
        step = settings.grid_step_percent / Decimal(100)
        if side == "BUY" and take_profit_price > min(take_profit_levels) * (1 - step):
            return None
        if side == "SELL" and take_profit_price < max(take_profit_levels) * (1 + step):
            return None
    return GridQuote(side, entry_price, take_profit_price)


class MakerGridBot:
    """Keep bounded entry orders working and pair each filled entry with an exit."""

    config: Config
    settings: GridSettings
    client: GridClient
    orders: GridOrders
    entries: dict[str, PendingEntry]
    exits: dict[str, PendingExit]
    total_volume: Decimal
    stop_entries: bool
    next_entry_at: float

    def __init__(
        self,
        config: Config,
        settings: GridSettings,
        client: GridClient,
        orders: GridOrders,
    ) -> None:
        self.config = config
        self.settings = settings
        self.client = client
        self.orders = orders
        self.entries = {}
        self.exits = {}
        self.total_volume = Decimal(0)
        self.stop_entries = False
        self.next_entry_at = 0.0

    def _take_profit_levels(self) -> tuple[Decimal, ...]:
        """Return close levels reserved by live entries and exits."""
        return tuple(
            [entry.take_profit_price for entry in self.entries.values()]
            + [exit_order.price for exit_order in self.exits.values()]
        )

    async def _finish_entry(self, order_id: str) -> None:
        """Replace a terminal entry with one take-profit for its aggregate fill."""
        pending = self.entries.pop(order_id)
        state = self.client.state.order_states[order_id]
        if state.filled_quantity == 0:
            logger.info("Entry order %s ended %s without a fill", order_id, state.status)
            return

        best_bid, best_ask = self.client.orderbook.current()
        exit_price = passive_take_profit_price(
            pending.side,
            state.average_fill_price,
            best_bid,
            best_ask,
            self.config.tick_size,
            self.config.take_profit_percent,
        )
        if exit_price * state.filled_quantity > self.config.max_order_notional:
            raise ProtocolError("take-profit notional exceeds --max-order-notional")
        exit_side: Literal["BUY", "SELL"] = (
            "SELL" if pending.side == "BUY" else "BUY"
        )
        exit_id = await self.orders.place(
            exit_side,
            exit_price,
            state.filled_quantity,
        )
        self.exits[exit_id] = PendingExit(
            side=exit_side,
            price=exit_price,
            quantity=state.filled_quantity,
        )
        self.total_volume += state.filled_quantity * state.average_fill_price
        logger.info(
            "Entry %s filled %s at %s; exit %s rests at %s",
            order_id,
            state.filled_quantity,
            state.average_fill_price,
            exit_id,
            exit_price,
        )

    async def _cancel_entry(self, order_id: str) -> None:
        """Cancel an expired or partially filled entry before managing its fill."""
        await self.orders.cancel(order_id)
        state = await self.client.wait_terminal(order_id, 10)
        if state is None or state.status not in TERMINAL_STATES:
            raise ProtocolError(f"entry cancellation was not confirmed: {order_id}")
        await self._finish_entry(order_id)

    async def _reconcile_orders(self, now: float) -> None:
        """Convert entry fills to exits and recover cancelled partial exits."""
        for order_id, pending in tuple(self.entries.items()):
            state = self.client.state.order_states.get(order_id)
            if state is not None and state.status in TERMINAL_STATES:
                await self._finish_entry(order_id)
            elif now - pending.placed_at >= self.config.entry_timeout_seconds:
                await self._cancel_entry(order_id)
            elif state is not None and state.filled_quantity > 0:
                await self._cancel_entry(order_id)

        for order_id, pending in tuple(self.exits.items()):
            state = self.client.state.order_states.get(order_id)
            if state is None or state.status not in TERMINAL_STATES:
                continue
            self.total_volume += state.filled_quantity * state.average_fill_price
            remaining = pending.quantity - state.filled_quantity
            del self.exits[order_id]
            if remaining > 0 and state.status == "CANCELED":
                replacement_id = await self.orders.place(
                    pending.side,
                    pending.price,
                    remaining,
                )
                self.exits[replacement_id] = PendingExit(
                    side=pending.side,
                    price=pending.price,
                    quantity=remaining,
                )
            elif remaining > 0:
                raise ProtocolError(
                    f"take-profit order {order_id} ended {state.status} with {remaining} unclosed"
                )

    def _price_triggered(self, price: Decimal, threshold: Decimal) -> bool:
        """Return whether the configured directional threshold has been reached."""
        if threshold <= 0:
            return False
        match self.config.side:
            case "BUY":
                return price >= threshold
            case "SELL":
                return price <= threshold
            case _:
                raise InputError("side must be BUY or SELL")

    async def _cancel_entries(self) -> None:
        """Cancel every still-working entry and preserve all exit orders."""
        for order_id in tuple(self.entries):
            state = self.client.state.order_states.get(order_id)
            if state is not None and state.status in TERMINAL_STATES:
                await self._finish_entry(order_id)
            else:
                await self._cancel_entry(order_id)

    async def run(self) -> Decimal:
        """Run until stopped by price/volume limits after working exits are flat."""
        await self.client.subscribe_maker_channels()
        if not self.client.state.position_ready:
            raise ProtocolError("position snapshot was not received")
        if self.client.state.position != 0 or self.client.state.open_orders:
            raise InputError("start on a dedicated flat subaccount with no open orders")

        try:
            while True:
                now = monotonic()
                await self._reconcile_orders(now)
                best_bid, best_ask = self.client.orderbook.current()
                mid_price = (best_bid + best_ask) / Decimal(2)
                if self._price_triggered(mid_price, self.settings.stop_price):
                    self.stop_entries = True

                volume_limit = self.total_volume >= self.config.max_total_volume
                if self.stop_entries or volume_limit:
                    self.stop_entries = True
                    await self._cancel_entries()
                    if (
                        not self.entries
                        and not self.exits
                        and self.client.state.effective_position == 0
                    ):
                        return self.total_volume
                elif (
                    not self._price_triggered(mid_price, self.settings.pause_price)
                    and now >= self.next_entry_at
                    and len(self.exits) + 2 * len(self.entries) + 2
                    <= self.settings.max_orders
                ):
                    quote = calculate_grid_quote(
                        self.config,
                        self.settings,
                        best_bid,
                        best_ask,
                        self._take_profit_levels(),
                    )
                    if quote is not None:
                        planned_volume = (
                            quote.entry_price * self.config.quantity
                            + quote.take_profit_price * self.config.quantity
                        )
                        if self.total_volume + planned_volume > self.config.max_total_volume:
                            self.stop_entries = True
                        else:
                            order_id = await self.orders.place(
                                quote.side,
                                quote.entry_price,
                                self.config.quantity,
                            )
                            self.entries[order_id] = PendingEntry(
                                side=quote.side,
                                take_profit_price=quote.take_profit_price,
                                placed_at=now,
                            )
                            self.next_entry_at = now + self.config.wait_seconds
                            logger.info(
                                "Grid entry %s rests %s %s at %s",
                                order_id,
                                quote.side,
                                self.config.quantity,
                                quote.entry_price,
                            )
                _ = await self.client.next_message()
        finally:
            with anyio.CancelScope(shield=True):
                await self._cancel_entries()
