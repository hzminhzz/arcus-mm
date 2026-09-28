"""Position, order, fill, and account snapshots from Arcus channels."""

from collections import deque
from decimal import Decimal, InvalidOperation
from time import monotonic_ns

from arcus_bot.types import Fill, JsonObject, JsonValue, OrderState, ProtocolError

TERMINAL_STATES = frozenset({"FILLED", "CANCELED", "MARGIN_CANCELED", "REJECTED"})


def _decimal(data: JsonObject, *keys: str) -> Decimal:
    """Read the first present decimal field from an Arcus message."""
    for key in keys:
        value = data.get(key)
        if isinstance(value, str | int | float):
            try:
                return Decimal(str(value))
            except InvalidOperation as error:
                raise ProtocolError(f"invalid decimal field {key}") from error
    raise ProtocolError(f"missing decimal field {keys[0]}")


def _row_position(row: JsonObject) -> Decimal:
    """Convert an Arcus position row into a signed base-asset size."""
    side = row.get("side")
    size = _decimal(row, "size")
    match side:
        case "LONG":
            return size
        case "SHORT":
            return -size
        case "FLAT":
            return Decimal(0)
        case _:
            raise ProtocolError("unknown position side in Arcus position update")


class AccountState:
    """Mutable account cache; mutation is required for live event updates."""

    market_id: int
    position: Decimal
    position_ready: bool
    position_sequence: int
    position_updated_at_ns: int
    pending_position_fills: deque[tuple[int, str, Decimal]]
    orders_ready: bool
    require_order_sequence: bool

    def __init__(self, market_id: int) -> None:
        self.market_id = market_id
        self.position = Decimal(0)
        self.position_ready = False
        self.position_sequence = 0
        self.position_updated_at_ns = 0
        self.pending_position_fills = deque()
        self.orders_ready = False
        self.require_order_sequence = False
        self.open_orders: set[str] = set()
        self.order_states: dict[str, OrderState] = {}
        self.fills: deque[Fill] = deque(maxlen=100)
        self.account_equity: Decimal | None = None
        self.free_collateral: Decimal | None = None

    @property
    def effective_position(self) -> Decimal:
        """Include order fills not yet covered by an Arcus position sequence."""
        position = self.position
        for _, side, size in self.pending_position_fills:
            match side:
                case "BUY":
                    position += size
                case "SELL":
                    position -= size
                case _:
                    raise ProtocolError(f"unknown order side in pending fill: {side}")
        return position

    def apply(self, message: JsonObject, received_at_ns: int | None = None) -> None:
        """Apply one account-scoped channel frame to cached state."""
        channel = message.get("channel")
        contents_value = message.get("contents")
        if not isinstance(channel, str) or not isinstance(contents_value, dict):
            return
        contents = contents_value
        message_type = message.get("type")
        received_at = monotonic_ns() if received_at_ns is None else received_at_ns

        if channel == "positions":
            self._apply_positions(message_type, contents, received_at)
        elif channel == "orders":
            self._apply_orders(message_type, contents)
        elif channel == "userFills":
            self._apply_fills(message_type, contents)
        elif channel == "account":
            self._apply_account(message_type, contents)

    def _apply_positions(
        self,
        message_type: JsonValue,
        contents: JsonObject,
        received_at_ns: int,
    ) -> None:
        """Apply a position snapshot or sequenced update for this market."""
        row: JsonObject | None = None
        if message_type == "subscribed":
            positions = contents.get("positions")
            if isinstance(positions, dict):
                candidate = positions.get(str(self.market_id))
                if isinstance(candidate, dict):
                    row = candidate
            self.position_ready = True
        elif self.require_order_sequence and message_type == "channel_data":
            positions = contents.get("positions")
            if isinstance(positions, dict):
                candidate = positions.get(str(self.market_id))
                if isinstance(candidate, dict):
                    row = candidate
            if row is None and contents.get("marketId") == self.market_id:
                row = contents
        elif not self.require_order_sequence and contents.get("marketId") == self.market_id:
            row = contents
        else:
            return

        if row is not None:
            self.position = _row_position(row)
            row_sequence = row.get("sequenceNumber")
            if isinstance(row_sequence, int):
                self.position_sequence = max(self.position_sequence, row_sequence)
        snapshot_sequence = contents.get("lastSequenceId")
        if isinstance(snapshot_sequence, int):
            self.position_sequence = max(self.position_sequence, snapshot_sequence)
        self.position_updated_at_ns = received_at_ns
        while (
            self.pending_position_fills
            and self.pending_position_fills[0][0] <= self.position_sequence
        ):
            _ = self.pending_position_fills.popleft()

    def _apply_orders(self, message_type: JsonValue, contents: JsonObject) -> None:
        """Rebuild live orders from snapshots and lifecycle updates."""
        if message_type == "subscribed":
            open_orders = contents.get("openOrders")
            if self.require_order_sequence and not isinstance(open_orders, list):
                open_orders = contents.get("orders")
            if isinstance(open_orders, list):
                self.open_orders = {
                    order_id
                    for row in open_orders
                    if isinstance(row, dict)
                    and row.get("marketId") == self.market_id
                    and isinstance((order_id := row.get("orderId")), str)
                }
                self.orders_ready = True
            return
        if message_type != "channel_data" or contents.get("marketId") != self.market_id:
            return
        order_id = contents.get("orderId")
        status = contents.get("state", contents.get("status"))
        if not isinstance(order_id, str) or not isinstance(status, str):
            return
        original = _decimal(contents, "originalSize")
        remaining = _decimal(contents, "remainingSize")
        fill_price = _decimal(contents, "avgFillPrice", "price")
        filled_quantity = max(Decimal(0), original - remaining)
        previous = self.order_states.get(order_id)
        previous_filled = previous.filled_quantity if previous is not None else Decimal(0)
        sequence_value = contents.get("sequenceNumber")
        sequence_number = (
            sequence_value
            if isinstance(sequence_value, int) and not isinstance(sequence_value, bool)
            else 0
        )
        side = contents.get("side")
        fill_delta = max(Decimal(0), filled_quantity - previous_filled)
        if self.require_order_sequence:
            if fill_delta > 0 and sequence_number == 0:
                raise ProtocolError("Arcus fill update omitted its account sequence number")
            if fill_delta > 0 and sequence_number > self.position_sequence:
                if not isinstance(side, str) or side not in {"BUY", "SELL"}:
                    raise ProtocolError("Arcus fill update omitted a recognized order side")
                self.pending_position_fills.append((sequence_number, side, fill_delta))
        self.order_states[order_id] = OrderState(
            status=status,
            filled_quantity=filled_quantity,
            average_fill_price=fill_price,
            side=side if self.require_order_sequence and isinstance(side, str) else None,
            sequence_number=sequence_number if self.require_order_sequence else 0,
        )
        if status in TERMINAL_STATES:
            self.open_orders.discard(order_id)
        else:
            self.open_orders.add(order_id)

    def _apply_fills(self, message_type: JsonValue, contents: JsonObject) -> None:
        """Append recent fills, including liquidity role and fee when present."""
        if message_type not in {"subscribed", "channel_data"}:
            return
        fill_rows: list[JsonObject] = []
        if message_type == "subscribed":
            rows = contents.get("fills")
            if isinstance(rows, list):
                fill_rows = [row for row in rows if isinstance(row, dict)]
        else:
            fill_rows = [contents]
        for row in fill_rows:
            trade_id = row.get("tradeId")
            order_id = row.get("orderId")
            side = row.get("side")
            if (
                not isinstance(trade_id, str)
                or not isinstance(order_id, str)
                or not isinstance(side, str)
            ):
                continue
            price = _decimal(row, "fillPrice", "price")
            size = _decimal(row, "fillSize", "size")
            fee_value = row.get("fee")
            fee = Decimal(str(fee_value)) if isinstance(fee_value, str | int | float) else None
            role_value = row.get("role")
            role = role_value if isinstance(role_value, str) else None
            self.fills.appendleft(
                Fill(
                    trade_id=trade_id,
                    order_id=order_id,
                    side=side,
                    price=price,
                    size=size,
                    fee=fee,
                    role=role,
                )
            )

    def _apply_account(self, message_type: JsonValue, contents: JsonObject) -> None:
        """Cache equity and collateral fields from account snapshots."""
        if message_type not in {"subscribed", "channel_data"}:
            return
        equity_value = contents.get("accountEquity", contents.get("equity"))
        collateral_value = contents.get("freeCollateral")
        if isinstance(equity_value, str):
            self.account_equity = Decimal(equity_value)
        if isinstance(collateral_value, str):
            self.free_collateral = Decimal(collateral_value)
