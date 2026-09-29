"""Arcus quote reconciliation, inventory reservation, and cancellation."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from time import monotonic_ns
from typing import Final

from arcus_bot.bots.market_maker.market import MarketInfo
from arcus_bot.bots.market_maker.quoter import (
    Quote,
    QuoteContext,
    Side,
    calculate_quotes,
)
from arcus_bot.bots.market_maker.runtime import MakerClient, MakerOrderActions, MakerRuntime
from arcus_bot.types import OrderRules, ProtocolError

_ZERO: Final = Decimal(0)


@dataclass(slots=True)
class MakerOrderManager:
    """Own and reconcile the ALO orders for one Arcus market."""

    runtime: MakerRuntime
    market: MarketInfo
    client: MakerClient
    orders: MakerOrderActions | None
    tracked_orders: dict[str, Quote] = field(default_factory=dict)
    placed_at_ns: dict[str, int] = field(default_factory=dict)

    def quotes(
        self,
        fair_price: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
    ) -> tuple[Quote, ...]:
        """Calculate candidates using Arcus position as the source of truth."""
        position = self.client.state.effective_position if self.runtime.submit else Decimal(0)
        return calculate_quotes(
            QuoteContext(
                market=self.market,
                config=self.runtime.quote_config,
                fair_price=fair_price,
                best_bid=best_bid,
                best_ask=best_ask,
                position=position,
            )
        )

    async def reconcile(
        self,
        desired_quotes: tuple[Quote, ...],
        fair_price: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
    ) -> None:
        """Cancel and confirm changed orders before placing replacements."""
        desired_by_side = {quote.side: quote for quote in desired_quotes}
        unknown_orders = self.client.state.open_orders - self.tracked_orders.keys()
        if unknown_orders:
            raise ProtocolError(
                "Arcus reported unmanaged open orders; refusing to take ownership"
            )
        if self._position_update_pending():
            return

        for order_id, current in tuple(self.tracked_orders.items()):
            state = self.client.state.order_states.get(order_id)
            if state is not None and state.status in {
                "FILLED",
                "CANCELED",
                "MARGIN_CANCELED",
                "REJECTED",
            }:
                reported_fill = self.client.state.fill_notional_by_order.get(
                    order_id, _ZERO
                )
                expected_fill = state.average_fill_price * state.filled_quantity
                if expected_fill > reported_fill:
                    del desired_by_side[current.side]
                    continue
                del self.tracked_orders[order_id]
                _ = self.placed_at_ns.pop(order_id, None)
                continue
            remaining = current.quantity
            if state is not None:
                remaining = max(Decimal(0), current.quantity - state.filled_quantity)
            candidate = desired_by_side.get(current.side)
            if (
                candidate is not None
                and candidate.price == current.price
                and candidate.quantity == remaining
            ):
                del desired_by_side[current.side]
                continue
            placed_at = self.placed_at_ns.get(order_id)
            if (
                candidate is not None
                and placed_at is not None
                and monotonic_ns() - placed_at
                < self.runtime.minimum_order_rest_ms * 1_000_000
            ):
                del desired_by_side[current.side]
                continue
            await self.cancel_and_confirm(order_id)
            del self.tracked_orders[order_id]
            _ = self.placed_at_ns.pop(order_id, None)
            return

        if self.orders is None:
            raise ProtocolError("maker order adapter disappeared during reconciliation")
        sides: tuple[Side, ...] = ("BUY", "SELL")
        for side in sides:
            if side not in desired_by_side:
                continue
            refreshed = self.quotes(fair_price, best_bid, best_ask)
            candidate = next((quote for quote in refreshed if quote.side == side), None)
            if candidate is None:
                continue
            if self.projected_position_exceeds_limit(
                fair_price, side=side, candidate_quantity=candidate.quantity
            ):
                continue
            maximum_turnover = self.runtime.max_traded_notional_usd
            if (
                maximum_turnover is not None
                and self.committed_traded_notional()
                + candidate.price * candidate.quantity
                > maximum_turnover
            ):
                continue
            client_id = (
                f"am-{self.market.mapping.market_id}-{self.runtime.run_id}-{side[0].lower()}"
            )
            order_id = await self.orders.place(
                side,
                candidate.price,
                candidate.quantity,
                OrderRules(
                    tick_size=self.market.tick_size_for(candidate.price),
                    step_size=self.market.mapping.step_size,
                    client_id=client_id,
                ),
            )
            self.tracked_orders[order_id] = candidate
            self.placed_at_ns[order_id] = monotonic_ns()
            return

    def _position_update_pending(self) -> bool:
        """Wait for Arcus position state before replacing a filled quote."""
        if not self.client.state.require_order_sequence:
            return False
        position_sequence = self.client.state.position_sequence
        return any(
            state.filled_quantity > 0
            and state.sequence_number > position_sequence
            for state in self.client.state.order_states.values()
        )

    def committed_traded_notional(self) -> Decimal:
        """Include fills, fill updates awaiting trade events, and working quotes."""
        committed = self.client.state.cumulative_traded_notional_usd
        for order_id, quote in self.tracked_orders.items():
            state = self.client.state.order_states.get(order_id)
            filled_quantity = state.filled_quantity if state is not None else _ZERO
            remaining = max(_ZERO, quote.quantity - filled_quantity)
            committed += remaining * quote.price
            if state is not None:
                expected_fill = state.average_fill_price * filled_quantity
                reported_fill = self.client.state.fill_notional_by_order.get(
                    order_id, _ZERO
                )
                committed += max(_ZERO, expected_fill - reported_fill)
        return committed

    def projected_position_exceeds_limit(
        self,
        fair_price: Decimal,
        side: Side | None = None,
        candidate_quantity: Decimal | None = None,
    ) -> bool:
        """Check whether placing this candidate order would exceed position limits."""
        candidate_qty = _ZERO if candidate_quantity is None else candidate_quantity
        position = self.client.state.effective_position
        maximum = self.runtime.quote_config.maximum_position_usd / fair_price

        working_buys = _ZERO
        working_sells = _ZERO
        for order_id, quote in self.tracked_orders.items():
            state = self.client.state.order_states.get(order_id)
            remaining = quote.quantity
            if state is not None:
                remaining = max(_ZERO, remaining - state.filled_quantity)
            if quote.side == "BUY":
                working_buys += remaining
            elif quote.side == "SELL":
                working_sells += remaining

        match side:
            case "BUY":
                projected_long = position + working_buys + candidate_qty
                return projected_long > maximum
            case "SELL":
                projected_short = position - working_sells - candidate_qty
                return projected_short < -maximum
            case None:
                maximum_long = position + working_buys + candidate_qty
                maximum_short = position - working_sells - candidate_qty
                return maximum_long > maximum or maximum_short < -maximum

    async def cancel_open_orders(self) -> None:
        """Cancel and confirm every order known on this owned market."""
        order_ids = set(self.tracked_orders) | self.client.state.open_orders
        for order_id in tuple(order_ids):
            await self.cancel_and_confirm(order_id)
            _ = self.tracked_orders.pop(order_id, None)
            _ = self.placed_at_ns.pop(order_id, None)

    async def cancel_and_confirm(self, order_id: str) -> None:
        """Wait for a terminal Arcus lifecycle event after a cancel request."""
        if self.orders is None:
            raise ProtocolError("cannot cancel without an Arcus order adapter")
        await self.orders.cancel(order_id)
        state = await self.client.wait_terminal(order_id, 10)
        if state is None or state.status not in {
            "FILLED",
            "CANCELED",
            "MARGIN_CANCELED",
            "REJECTED",
        }:
            raise ProtocolError(f"Arcus cancellation was not confirmed for order {order_id}")
