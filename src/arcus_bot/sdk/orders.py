"""Typed Arcus order signing and order-operation adapter."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Final

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from arcus_bot.bots.cycle.quoter import aligned_units
from arcus_bot.sdk.client import ArcusClient
from arcus_bot.types import (
    Config,
    InputError,
    JsonObject,
    OrderConfig,
    OrderRules,
    ProtocolError,
)

ALO_CODE: Final = 3
TIF_CODE: Final = {"IOC": 1, "GTC": 2, "ALO": 3}
SIDE_CODE: Final = {"BUY": 0, "SELL": 1}


class ArcusOrders:
    """Sign and submit order operations for one account."""

    private_key: Ed25519PrivateKey
    client: ArcusClient
    config: Config | OrderConfig
    api_key: str

    def __init__(
        self,
        client: ArcusClient,
        config: Config | OrderConfig,
        private_key_hex: str,
    ) -> None:
        try:
            self.private_key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(private_key_hex))
        except (ValueError, TypeError) as error:
            raise InputError("ARCUS_API_SIGNING_KEY must be a 32-byte hex key") from error
        self.client = client
        self.config = config
        self.api_key = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ).hex()

    @staticmethod
    def build_place_payload(
        config: Config | OrderConfig,
        side: str,
        price: Decimal,
        quantity: Decimal,
        timestamp_ns: int,
        rules: OrderRules | None = None,
    ) -> tuple[JsonObject, bytes]:
        """Build the decimal REST-style body and typed order-sign payload."""
        if side not in SIDE_CODE:
            raise InputError("side must be BUY or SELL")
        reduce_only = rules.reduce_only if rules is not None else False
        time_in_force = rules.time_in_force if rules is not None else "ALO"
        tif_code = TIF_CODE.get(time_in_force, ALO_CODE)
        good_til_us = timestamp_ns // 1000 + (31 * 86_400_000_000 if time_in_force != "IOC" else 300_000_000)
        account = config.account
        tick_size = rules.tick_size if rules is not None else config.tick_size
        step_size = rules.step_size if rules is not None else config.step_size
        client_id = rules.client_id if rules is not None else None
        signed: JsonObject = {
            "ad": account.address.lower(),
            "ai": account.account_index,
            "ct": timestamp_ns,
            "g": good_til_us * 1000,
            "m": account.market_id,
            "op": 1,
            "p": aligned_units(price, tick_size, "price"),
            "q": aligned_units(quantity, step_size, "quantity"),
            "r": 1 if reduce_only else 0,
            "s": SIDE_CODE[side],
            "t": tif_code,
            "v": 1,
        }
        if client_id is not None:
            signed["c"] = client_id
        body: JsonObject = {
            "address": account.address,
            "accountIndex": account.account_index,
            "marketId": account.market_id,
            "orderSide": side,
            "orderType": "LIMIT",
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": time_in_force,
            "goodTilTime": str(good_til_us),
            "timestamp": timestamp_ns,
            "reduceOnly": bool(reduce_only),
        }
        if client_id is not None:
            body["clientId"] = client_id
        return body, json.dumps(signed, separators=(",", ":"), sort_keys=True).encode()

    async def place(
        self,
        side: str,
        price: Decimal,
        quantity: Decimal,
        rules: OrderRules | None = None,
    ) -> str:
        """Submit one ALO order and return its server order ID."""
        timestamp = time.time_ns()
        body, signed = self.build_place_payload(
            self.config,
            side,
            price,
            quantity,
            timestamp,
            rules,
        )
        response = await self.client.rpc(
            "placeOrder",
            body,
            self.api_key,
            timestamp,
            self.private_key.sign(signed).hex(),
        )
        result = response.get("result", response.get("contents", response))
        if isinstance(result, dict):
            order_id = result.get("orderId")
            if isinstance(order_id, str):
                return order_id
        raise ProtocolError(f"placeOrder acknowledgement omitted orderId: {response}")

    async def cancel(self, order_id: str) -> None:
        """Cancel one order by server-assigned ID."""
        timestamp = time.time_ns()
        account = self.config.account
        signed: JsonObject = {
            "ad": account.address.lower(),
            "ai": account.account_index,
            "ct": timestamp,
            "id": order_id,
            "m": account.market_id,
            "op": 2,
            "v": 1,
        }
        payload: JsonObject = {
            "address": account.address,
            "accountIndex": account.account_index,
            "marketId": account.market_id,
            "kind": "orderId",
            "orderId": order_id,
            "timestamp": timestamp,
        }
        _ = await self.client.rpc(
            "cancelOrder",
            payload,
            self.api_key,
            timestamp,
            self.private_key.sign(
                json.dumps(signed, separators=(",", ":"), sort_keys=True).encode()
            ).hex(),
        )
