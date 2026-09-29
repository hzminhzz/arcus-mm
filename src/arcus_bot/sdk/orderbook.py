"""Minimal L2 book state for quoting and monitoring."""

from decimal import Decimal, InvalidOperation
from time import monotonic_ns

from arcus_bot.types import JsonObject, ProtocolError


class OrderbookState:
    """Mutable book cache updated from Arcus full snapshots."""

    best_bid: Decimal | None
    best_ask: Decimal | None
    received_at_ns: int

    def __init__(self) -> None:
        self.best_bid = None
        self.best_ask = None
        self.received_at_ns = 0

    def apply(self, message: JsonObject, received_at_ns: int | None = None) -> None:
        """Replace the cached top of book from an Arcus L2 frame."""
        if message.get("channel") != "l2Orderbook":
            return
        contents_value = message.get("contents")
        if not isinstance(contents_value, dict):
            return
        bids = contents_value.get("bids")
        asks = contents_value.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list):
            return
        if not bids or not asks:
            self.best_bid = None
            self.best_ask = None
            return
        bid = bids[0]
        ask = asks[0]
        if (
            isinstance(bid, list)
            and bid
            and isinstance(bid[0], str)
            and isinstance(ask, list)
            and ask
            and isinstance(ask[0], str)
        ):
            try:
                best_bid = Decimal(bid[0])
                best_ask = Decimal(ask[0])
            except InvalidOperation as error:
                raise ProtocolError("Arcus returned a non-decimal orderbook price") from error
            if (
                not best_bid.is_finite()
                or not best_ask.is_finite()
                or best_bid <= 0
                or best_ask <= best_bid
            ):
                raise ProtocolError("Arcus returned an invalid or crossed order book")
            self.best_bid = best_bid
            self.best_ask = best_ask
            self.received_at_ns = (
                monotonic_ns() if received_at_ns is None else received_at_ns
            )

    def current(self) -> tuple[Decimal, Decimal]:
        """Return a complete, uncrossed BBO snapshot."""
        if self.best_bid is None or self.best_ask is None:
            raise ProtocolError("order book is unavailable")
        if (
            not self.best_bid.is_finite()
            or not self.best_ask.is_finite()
            or self.best_bid <= 0
            or self.best_ask <= self.best_bid
        ):
            raise ProtocolError("Arcus returned an invalid or crossed order book")
        return self.best_bid, self.best_ask
