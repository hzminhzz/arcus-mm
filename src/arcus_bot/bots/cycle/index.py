"""Orchestrate bounded Arcus maker entry and take-profit cycles."""

import logging
from decimal import Decimal

import anyio

from arcus_bot.bots.cycle.quoter import (
    estimated_round_trip_notional,
    passive_entry_price,
    passive_take_profit_price,
)
from arcus_bot.sdk.account import TERMINAL_STATES
from arcus_bot.sdk.client import ArcusClient
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import Config, InputError, OrderState, ProtocolError

logger = logging.getLogger(__name__)


class MakerCycleBot:
    """Run capped post-only entries and exits on a dedicated subaccount."""

    config: Config
    client: ArcusClient
    orders: ArcusOrders

    def __init__(self, config: Config, client: ArcusClient, orders: ArcusOrders) -> None:
        self.config = config
        self.client = client
        self.orders = orders

    async def _place_entry(self, side: str) -> OrderState:
        """Place one passive entry and cancel it if its fill timer expires."""
        best_bid, best_ask = self.client.orderbook.current()
        price = passive_entry_price(side, best_bid, best_ask)
        notional = price * self.config.quantity
        if notional < Decimal("5"):
            raise InputError("entry notional must be at least $5")
        if notional > self.config.max_order_notional:
            raise InputError("entry notional exceeds --max-order-notional")

        order_id = await self.orders.place(side, price, self.config.quantity)
        state = await self.client.wait_terminal(order_id, self.config.entry_timeout_seconds)
        if state is None or state.status not in TERMINAL_STATES:
            await self.orders.cancel(order_id)
            state = await self.client.wait_terminal(order_id, 10)
        if state is None or state.status not in TERMINAL_STATES:
            raise ProtocolError("entry cancellation was not confirmed")
        if state.status in {"REJECTED", "MARGIN_CANCELED"}:
            raise ProtocolError(f"entry order ended as {state.status}")
        return state

    async def run(self) -> Decimal:
        """Run the configured number of single-position maker cycles."""
        await self.client.subscribe_bot_channels()
        if not self.client.state.position_ready:
            raise ProtocolError("position snapshot was not received")
        if self.client.state.position != 0 or self.client.state.open_orders:
            raise InputError("use a dedicated flat subaccount with no open orders in this market")

        total_volume = Decimal(0)
        entry_side = self.config.side
        exit_side = "SELL" if entry_side == "BUY" else "BUY"
        for cycle in range(self.config.cycles):
            best_bid, best_ask = self.client.orderbook.current()
            estimated_volume = estimated_round_trip_notional(
                passive_entry_price(entry_side, best_bid, best_ask),
                self.config.quantity,
            )
            if total_volume + estimated_volume > self.config.max_total_volume:
                raise InputError("next cycle would exceed --max-total-volume")

            logger.info("Cycle %s/%s: placing %s ALO entry", cycle + 1, self.config.cycles, entry_side)
            entry = await self._place_entry(entry_side)
            if entry.filled_quantity == 0:
                logger.info("Entry expired unfilled; stopping without another cycle")
                break

            await self.client.wait_for_position(
                Decimal(1) if entry_side == "BUY" else Decimal(-1),
                entry.filled_quantity,
            )
            best_bid, best_ask = self.client.orderbook.current()
            exit_price = passive_take_profit_price(
                entry_side,
                entry.average_fill_price,
                best_bid,
                best_ask,
                self.config.tick_size,
                self.config.take_profit_percent,
            )
            if exit_price * entry.filled_quantity > self.config.max_order_notional:
                raise InputError("exit notional exceeds --max-order-notional; manage position manually")
            exit_id = await self.orders.place(exit_side, exit_price, entry.filled_quantity)
            logger.info(
                "Entry filled %s at %s; resting %s ALO exit at %s",
                entry.filled_quantity,
                entry.average_fill_price,
                exit_side,
                exit_price,
            )
            exit_state = await self.client.wait_terminal(exit_id, self.config.entry_timeout_seconds)
            if exit_state is None or exit_state.status != "FILLED":
                raise ProtocolError(
                    "take-profit is not confirmed FILLED; reconcile the order and position"
                )
            await self.client.wait_for_position(Decimal(0), Decimal(0))
            total_volume += (
                entry.filled_quantity * entry.average_fill_price
                + exit_state.filled_quantity * exit_state.average_fill_price
            )
            logger.info("Cycle complete; observed matched notional so far: $%s", total_volume)
            if cycle + 1 < self.config.cycles:
                await anyio.sleep(self.config.wait_seconds)
        return total_volume
