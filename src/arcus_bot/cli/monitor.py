"""Read-only Arcus account and market terminal monitor."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from decimal import Decimal

import anyio
import websockets

from arcus_bot.sdk.client import ArcusClient
from arcus_bot.types import AccountRef, ProtocolError

TESTNET_WS = "wss://api.testnet.arcus.xyz/v1/ws"
MAINNET_WS = "wss://api.arcus.xyz/v1/ws"


@dataclass(slots=True)
class MonitorArguments(argparse.Namespace):
    address: str = ""
    account_index: int = 0
    market_id: int = 0
    market: str = ""
    mainnet: bool = False


class TerminalMonitor:
    """Render live book, orders, fills, and position state."""

    client: ArcusClient
    environment: str
    started_at: float
    last_channel: str
    tty: bool

    def __init__(self, client: ArcusClient, mainnet: bool) -> None:
        self.client = client
        self.environment = "MAINNET" if mainnet else "TESTNET"
        self.started_at = time.time()
        self.last_message_at: float | None = None
        self.last_channel = "starting"
        self.tty = sys.stdout.isatty()

    def render(self) -> None:
        """Draw a refreshed dashboard or emit a plain status line."""
        account = self.client.state
        last_frame = (
            f"{int(time.time() - self.last_message_at)}s ago"
            if self.last_message_at is not None
            else "waiting"
        )
        try:
            bid, ask = self.client.orderbook.current()
            spread = ask - bid
            market_line = f"bid {bid} | ask {ask} | spread {spread}"
        except ProtocolError:
            market_line = "book waiting for snapshot"

        position_line = f"{account.position:+} {self.client.account.market}"
        equity = self._display(account.account_equity)
        collateral = self._display(account.free_collateral)
        open_lines = [
            f"{order_id}: {account.order_states[order_id].status}"
            if order_id in account.order_states
            else f"{order_id}: OPEN"
            for order_id in sorted(account.open_orders)
        ]
        fill_lines = [
            f"{fill.side} {fill.size} @ {fill.price} | {fill.role or 'role pending'} "
            + f"| fee {fill.fee if fill.fee is not None else 'pending'}"
            for fill in account.fills
        ]
        lines = [
            f"ARCUS BOT MONITOR [{self.environment}] {self.client.account.market}",
            f"Stream: connected | {self.last_channel} {last_frame} | uptime: {int(time.time() - self.started_at)}s",
            f"Book: {market_line}",
            f"Position: {position_line}",
            f"Account equity: {equity} | free collateral: {collateral}",
            "Open orders:",
            *(open_lines or ["  none"]),
            "Recent fills:",
            *(fill_lines or ["  none"]),
            "Press Ctrl+C to disconnect the read-only monitor.",
        ]
        if self.tty:
            print("\033[2J\033[H" + "\n".join(lines), flush=True)
        else:
            print(" | ".join(lines[:5]), flush=True)

    @staticmethod
    def _display(value: Decimal | None) -> str:
        """Format an optional account balance for the dashboard."""
        return f"${value}" if value is not None else "not received"


async def run_monitor(account: AccountRef, mainnet: bool) -> None:
    """Connect to Arcus read-only channels and render live state."""
    url = MAINNET_WS if mainnet else TESTNET_WS
    async with websockets.connect(url, open_timeout=15, ping_interval=20, ping_timeout=20) as socket:
        client = ArcusClient(socket, account)
        await client.subscribe_monitor_channels()
        if not client.state.position_ready:
            raise ProtocolError("position snapshot was not received")
        monitor = TerminalMonitor(client, mainnet)
        monitor.render()
        while True:
            with anyio.move_on_after(1):
                message = await client.next_message()
                monitor.last_message_at = time.time()
                channel = message.get("channel")
                if isinstance(channel, str):
                    monitor.last_channel = channel
            monitor.render()


def main() -> int:
    """Run the read-only account and market monitor."""
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--address", required=True)
    _ = parser.add_argument("--account-index", type=int, default=0)
    _ = parser.add_argument("--market-id", type=int, required=True)
    _ = parser.add_argument("--market", required=True, help="Arcus market symbol, e.g. BTC-USD")
    _ = parser.add_argument("--mainnet", action="store_true", help="Use mainnet instead of testnet")
    args: MonitorArguments = parser.parse_args(namespace=MonitorArguments())
    address = args.address.removeprefix("0x").removeprefix("0X")
    if len(address) != 40:
        print("Input error: address must contain exactly 40 hexadecimal digits", file=sys.stderr)
        return 2
    try:
        _ = int(address, 16)
    except ValueError:
        print("Input error: address must be hexadecimal", file=sys.stderr)
        return 2
    if args.account_index not in range(10) or args.market_id not in range(65_536):
        print("Input error: account index must be 0-9 and market ID must be 0-65535", file=sys.stderr)
        return 2
    account = AccountRef(
        address=f"0x{address.lower()}",
        account_index=args.account_index,
        market_id=args.market_id,
        market=args.market.upper(),
    )
    try:
        anyio.run(run_monitor, account, args.mainnet)
    except KeyboardInterrupt:
        return 0
    except (ProtocolError, TimeoutError, OSError, websockets.WebSocketException) as error:
        print(f"Arcus monitor stopped: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
