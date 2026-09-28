"""Arcus quote reconciliation, inventory reservation, and cancellation."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from arcus_bot.bots.market_maker.market import MarketInfo
from arcus_bot.bots.market_maker.quoter import (
    Quote,
    QuoteContext,
    Side,
    calculate_quotes,
)
from arcus_bot.bots.market_maker.runtime import MakerClient, MakerOrderActions, MakerRuntime
from arcus_bot.types import OrderRules, ProtocolError


@dataclass(slots=True)
class MakerOrderManager:
    """Own and reconcile the ALO orders for one Arcus market."""

    runtime: MakerRuntime
    market: MarketInfo
    client: MakerClient
    orders: MakerOrderActions | None
    tracked_orders: dict[str, Quote] = field(default_factory=dict)

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
            await self.cancel_open_orders()
            raise ProtocolError(
                "Arcus reported unmanaged open orders; canceled and stopped for reconciliation"
            )

        for order_id, current in tuple(self.tracked_orders.items()):
            state = self.client.state.order_states.get(order_id)
            if state is not None and state.status in {
                "FILLED",
                "CANCELED",
                "MARGIN_CANCELED",
                "REJECTED",
            }:
                del self.tracked_orders[order_id]
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
            await self.cancel_and_confirm(order_id)
            del self.tracked_orders[order_id]
            return

        if self.orders is None:
            raise ProtocolError("maker order adapter disappeared during reconciliation")
        sides: tuple[Side, ...] = ("BUY", "SELL")
        for side in sides:
            if side not in desired_by_side:
                continue
            if self.projected_position_exceeds_limit(fair_price):
                await self.cancel_open_orders()
                return
            refreshed = self.quotes(fair_price, best_bid, best_ask)
            candidate = next((quote for quote in refreshed if quote.side == side), None)
            if candidate is None:
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
            return

    def projected_position_exceeds_limit(self, fair_price: Decimal) -> bool:
        """Check account position plus every still-fillable owned ALO order."""
        position = self.client.state.effective_position
        maximum = self.runtime.quote_config.maximum_position_usd / fair_price
        maximum_long = position
        maximum_short = position
        for order_id, quote in self.tracked_orders.items():
            state = self.client.state.order_states.get(order_id)
            remaining = quote.quantity
            if state is not None:
                remaining = max(Decimal(0), remaining - state.filled_quantity)
            match quote.side:
                case "BUY":
                    maximum_long += remaining
                case "SELL":
                    maximum_short -= remaining
        return maximum_long > maximum or maximum_short < -maximum

    async def cancel_open_orders(self) -> None:
        """Cancel and confirm every open order in this dedicated market."""
        order_ids = self.client.state.open_orders | self.tracked_orders.keys()
        for order_id in tuple(order_ids):
            await self.cancel_and_confirm(order_id)
            _ = self.tracked_orders.pop(order_id, None)

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
