"""Trading bot command-line entrypoint."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal

import anyio
import websockets

from arcus_bot.bots.cycle.index import MakerCycleBot
from arcus_bot.bots.grid import GridSettings, MakerGridBot
from arcus_bot.sdk.client import ArcusClient
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import AccountRef, Config, InputError, ProtocolError
from arcus_bot.utils.logger import configure_logging

TESTNET_WS = "wss://api.testnet.arcus.xyz/v1/ws"
MAINNET_WS = "wss://api.arcus.xyz/v1/ws"


@dataclass(slots=True)
class BotArguments(argparse.Namespace):
    strategy: Literal["grid", "cycle"] = "grid"
    address: str = ""
    account_index: int = 0
    market_id: int = 0
    market: str = ""
    side: str = "BUY"
    quantity: str = ""
    tick_size: str = ""
    step_size: str = ""
    take_profit_percent: str = "0.02"
    max_order_notional: str = "1000"
    max_total_volume: str = "5000"
    cycles: int = 1
    wait_seconds: int = 450
    entry_timeout_seconds: int = 300
    max_orders: int = 4
    grid_step: str = "0.5"
    stop_price: str = "-1"
    pause_price: str = "-1"
    log_level: str = "INFO"
    submit: bool = False
    mainnet: bool = False


@dataclass(frozen=True, slots=True)
class BotOptions:
    config: Config
    strategy: Literal["grid", "cycle"]
    grid_settings: GridSettings
    submit: bool
    log_level: str


def _positive_decimal(value: str, name: str) -> Decimal:
    """Parse one finite, positive decimal CLI value."""
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InputError(f"{name} must be a decimal number") from error
    if not parsed.is_finite() or parsed <= 0:
        raise InputError(f"{name} must be finite and positive")
    return parsed


def _nonnegative_decimal(value: str, name: str) -> Decimal:
    """Parse one finite, non-negative decimal CLI value."""
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InputError(f"{name} must be a decimal number") from error
    if not parsed.is_finite() or parsed < 0:
        raise InputError(f"{name} must be finite and non-negative")
    return parsed


def _threshold_decimal(value: str, name: str) -> Decimal:
    """Parse a disabled (-1) or finite positive price threshold."""
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise InputError(f"{name} must be a decimal number") from error
    if not parsed.is_finite() or parsed == 0 or parsed < -1:
        raise InputError(f"{name} must be -1 (disabled) or finite and positive")
    return parsed


def parse_config(argv: list[str] | None = None) -> BotOptions:
    """Parse CLI inputs into strategy and process options."""
    parser = argparse.ArgumentParser(description="Run an Arcus multi-limit grid strategy.")
    _ = parser.add_argument("--strategy", choices=("grid", "cycle"), default="grid")
    _ = parser.add_argument("--address", required=True)
    _ = parser.add_argument("--account-index", type=int, required=True)
    _ = parser.add_argument("--market-id", type=int, required=True)
    _ = parser.add_argument("--market", required=True, help="Arcus market symbol, e.g. BTC-USD")
    _ = parser.add_argument("--side", choices=("BUY", "SELL"), default="BUY")
    _ = parser.add_argument("--quantity", required=True)
    _ = parser.add_argument("--tick-size", required=True)
    _ = parser.add_argument("--step-size", required=True)
    _ = parser.add_argument("--take-profit-percent", default="0.02")
    _ = parser.add_argument("--max-order-notional", default="1000")
    _ = parser.add_argument("--max-total-volume", default="5000")
    _ = parser.add_argument("--cycles", type=int, default=1)
    _ = parser.add_argument(
        "--wait-seconds",
        type=int,
        default=450,
        help="minimum seconds between new entry orders",
    )
    _ = parser.add_argument(
        "--entry-timeout-seconds",
        type=int,
        default=300,
        help="cancel an unfilled or partially filled entry after this many seconds",
    )
    _ = parser.add_argument(
        "--max-orders",
        type=int,
        default=4,
        help="maximum active entries and exits; each entry reserves an exit slot",
    )
    _ = parser.add_argument(
        "--grid-step",
        default="0.5",
        help="minimum percent spacing between projected close-order prices; 0 disables",
    )
    _ = parser.add_argument(
        "--stop-price",
        default="-1",
        help="stop adding entries at this directional price; -1 disables",
    )
    _ = parser.add_argument(
        "--pause-price",
        default="-1",
        help="pause new entries at this directional price; -1 disables",
    )
    _ = parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    _ = parser.add_argument("--submit", action="store_true", help="Submit real orders (default is dry-run)")
    _ = parser.add_argument("--mainnet", action="store_true", help="Use mainnet instead of testnet")
    args: BotArguments = parser.parse_args(argv, namespace=BotArguments())

    address = args.address.removeprefix("0x").removeprefix("0X")
    if len(address) != 40:
        raise InputError("address must contain exactly 40 hexadecimal digits")
    try:
        _ = int(address, 16)
    except ValueError as error:
        raise InputError("address must be hexadecimal") from error
    if args.account_index not in range(10) or args.market_id not in range(65_536):
        raise InputError("account index must be 0-9 and market ID must be 0-65535")
    if args.cycles not in range(1, 101):
        raise InputError("--cycles must be between 1 and 100")
    if args.wait_seconds < 0 or args.entry_timeout_seconds <= 0:
        raise InputError("wait must be non-negative and timeout must be positive")
    if args.max_orders not in range(2, 101):
        raise InputError("--max-orders must be between 2 and 100")
    match args.strategy:
        case "grid":
            if args.cycles != 1:
                raise InputError("--cycles applies only to --strategy cycle")
        case "cycle":
            pass
    grid_step = _nonnegative_decimal(args.grid_step, "grid step")
    stop_price = _threshold_decimal(args.stop_price, "stop price")
    pause_price = _threshold_decimal(args.pause_price, "pause price")
    take_profit = _positive_decimal(args.take_profit_percent, "take-profit percent")
    if take_profit >= 100:
        raise InputError("take-profit percent must be below 100")
    account = AccountRef(
        address=f"0x{address.lower()}",
        account_index=args.account_index,
        market_id=args.market_id,
        market=args.market.upper(),
    )
    return BotOptions(
        config=Config(
            account=account,
            side=args.side,
            quantity=_positive_decimal(args.quantity, "quantity"),
            tick_size=_positive_decimal(args.tick_size, "tick size"),
            step_size=_positive_decimal(args.step_size, "step size"),
            take_profit_percent=take_profit,
            max_order_notional=_positive_decimal(args.max_order_notional, "max order notional"),
            max_total_volume=_positive_decimal(args.max_total_volume, "max total volume"),
            cycles=args.cycles,
            wait_seconds=args.wait_seconds,
            entry_timeout_seconds=args.entry_timeout_seconds,
            mainnet=args.mainnet,
        ),
        strategy=args.strategy,
        grid_settings=GridSettings(
            max_orders=args.max_orders,
            grid_step_percent=grid_step,
            stop_price=stop_price,
            pause_price=pause_price,
        ),
        submit=args.submit,
        log_level=args.log_level,
    )


async def _run_live(options: BotOptions) -> None:
    """Open a signed Arcus session and run the selected strategy."""
    config = options.config
    signing_key = os.environ.get("ARCUS_API_SIGNING_KEY", "")
    if not signing_key:
        raise InputError("set ARCUS_API_SIGNING_KEY before using --submit")
    url = MAINNET_WS if config.mainnet else TESTNET_WS
    async with websockets.connect(url, open_timeout=15, ping_interval=20, ping_timeout=20) as socket:
        client = ArcusClient(socket, config.account)
        orders = ArcusOrders(client, config, signing_key)
        match options.strategy:
            case "grid":
                volume = await MakerGridBot(
                    config,
                    options.grid_settings,
                    client,
                    orders,
                ).run()
            case "cycle":
                volume = await MakerCycleBot(config, client, orders).run()
        print(f"Run finished; observed matched notional: ${volume}.")


def main() -> int:
    """Run a local dry-run preview or explicitly submit a bounded strategy."""
    try:
        options = parse_config()
        config = options.config
        configure_logging(options.log_level)
        if not options.submit:
            match options.strategy:
                case "grid":
                    print(
                        f"Dry run: {config.side} grid, quantity {config.quantity}, max {options.grid_settings.max_orders} active orders, {config.wait_seconds}s between entries, {options.grid_settings.grid_step_percent}% grid step, {config.entry_timeout_seconds}s entry timeout, take-profit {config.take_profit_percent}%, max order ${config.max_order_notional}, max total volume ${config.max_total_volume}."
                    )
                case "cycle":
                    print(
                        f"Dry run: {config.cycles} {config.side} maker cycle(s), quantity "
                        + f"{config.quantity}, take-profit {config.take_profit_percent}%, max order "
                        + f"${config.max_order_notional}, max total volume ${config.max_total_volume}."
                    )
            print("No account connected and no order sent. Add --submit to connect.")
            return 0
        anyio.run(_run_live, options)
        return 0
    except InputError as error:
        print(f"Input error: {error}", file=sys.stderr)
        return 2
    except (ProtocolError, TimeoutError, OSError, websockets.WebSocketException) as error:
        detail = str(error) or "no additional error detail"
        print(
            f"Arcus session stopped ({type(error).__name__}): {detail}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
