"""Arcus WebSocket transport and shared account/market stream state."""

from __future__ import annotations

import json
from decimal import Decimal

import anyio
from websockets.asyncio.client import ClientConnection

from arcus_bot.sdk.account import AccountState, TERMINAL_STATES
from arcus_bot.sdk.orderbook import OrderbookState
from arcus_bot.types import (
    AccountRef,
    JSON_ADAPTER,
    JsonObject,
    JsonValue,
    OrderState,
    ProtocolError,
)


class ArcusClient:
    """One read/write WebSocket connection with cached public/account streams."""

    socket: ClientConnection
    account: AccountRef
    orderbook: OrderbookState
    state: AccountState
    request_id: int

    def __init__(self, socket: ClientConnection, account: AccountRef) -> None:
        self.socket = socket
        self.account = account
        self.orderbook = OrderbookState()
        self.state = AccountState(account.market_id)
        self.request_id = 0

    async def receive(self) -> JsonObject:
        """Receive and validate one WebSocket JSON frame."""
        raw = await self.socket.recv()
        text = raw.decode() if isinstance(raw, bytes) else raw
        parsed = JSON_ADAPTER.validate_json(text)
        if isinstance(parsed, dict):
            return parsed
        raise ProtocolError("expected a JSON object from Arcus")

    async def next_message(self) -> JsonObject:
        """Read a frame and update cached orderbook and account state."""
        message = await self.receive()
        self.orderbook.apply(message)
        self.state.apply(message)
        return message

    async def subscribe(self, channel: str, identifier: str, **extra: JsonValue) -> None:
        """Subscribe and consume the channel's initial snapshot."""
        request: JsonObject = {"type": "subscribe", "channel": channel, "id": identifier}
        request.update(extra)
        await self.socket.send(json.dumps(request, separators=(",", ":")))
        with anyio.fail_after(15):
            while True:
                message = await self.next_message()
                if message.get("type") == "subscribed" and message.get("channel") == channel:
                    return

    async def subscribe_bot_channels(self) -> None:
        """Subscribe to market, order, and position channels used by the bot."""
        await self.subscribe("l2Orderbook", self.account.market)
        for channel in ("orders", "positions"):
            await self.subscribe(
                channel,
                self.account.address,
                accountIndex=self.account.account_index,
                market=self.account.market,
            )

    async def subscribe_maker_channels(self) -> None:
        """Subscribe to market, account, order, and fill streams for the maker."""
        self.state.require_order_sequence = True
        await self.subscribe("l2Orderbook", self.account.market)
        for channel in ("orders", "positions", "userFills"):
            await self.subscribe(
                channel,
                self.account.address,
                accountIndex=self.account.account_index,
                market=self.account.market,
            )
        await self.subscribe(
            "account",
            self.account.address,
            accountIndex=self.account.account_index,
        )

    async def subscribe_monitor_channels(self) -> None:
        """Subscribe to market and account channels used by the monitor."""
        await self.subscribe("l2Orderbook", self.account.market)
        for channel in ("orders", "positions", "userFills"):
            await self.subscribe(
                channel,
                self.account.address,
                accountIndex=self.account.account_index,
                market=self.account.market,
            )
        await self.subscribe(
            "account",
            self.account.address,
            accountIndex=self.account.account_index,
        )

    async def rpc(
        self,
        method: str,
        payload: JsonObject,
        api_key: str,
        timestamp: int,
        signature: str,
    ) -> JsonObject:
        """Send a signed order RPC and wait for its correlated response."""
        self.request_id += 1
        request: JsonObject = {
            "type": method,
            "payload": payload,
            "apiKey": api_key,
            "timestamp": str(timestamp),
            "signature": signature,
        }
        envelope: JsonObject = {"type": "post", "id": self.request_id, "request": request}
        await self.socket.send(json.dumps(envelope, separators=(",", ":")))
        with anyio.fail_after(15):
            while True:
                message = await self.next_message()
                if message.get("id") != self.request_id:
                    continue
                status = message.get("status")
                if status in {400, 401, 403, 429}:
                    raise ProtocolError(f"Arcus rejected {method}: {message}")
                return message

    async def wait_terminal(self, order_id: str, seconds: int) -> OrderState | None:
        """Wait for a terminal order event while processing all channels."""
        with anyio.move_on_after(seconds):
            while True:
                state = self.state.order_states.get(order_id)
                if state is not None and state.status in TERMINAL_STATES:
                    return state
                _ = await self.next_message()
        return self.state.order_states.get(order_id)

    async def wait_for_position(self, sign: Decimal, minimum_size: Decimal) -> None:
        """Wait for a position update to confirm the expected exposure."""
        with anyio.fail_after(15):
            while True:
                if sign == 0 and self.state.position == 0:
                    return
                if sign != 0 and self.state.position * sign >= minimum_size:
                    return
                _ = await self.next_message()
