"""Run the continuous Arcus market maker."""

from __future__ import annotations

import logging
import sys
import time
import uuid

import anyio
import websockets

from arcus_bot.bots.market_maker.index import run_market
from arcus_bot.bots.market_maker.market import MarketInfo, fetch_market_info
from arcus_bot.bots.market_maker.runtime import MakerRuntime
from arcus_bot.cli.maker_config import MakerOptions, parse_options
from arcus_bot.types import AccountRef, InputError, ProtocolError
from arcus_bot.utils.logger import configure_logging
logger = logging.getLogger(__name__)


async def _run(options: MakerOptions) -> None:
    """Validate live market metadata, then run selected markets."""
    market_info: list[MarketInfo] = []
    for mapping in options.markets:
        market_info.append(await fetch_market_info(mapping, options.mainnet))
    run_deadline_ns = (
        time.monotonic_ns() + options.duration_seconds * 1_000_000_000
        if options.duration_seconds > 0
        else None
    )
    run_expiration_time_us = (
        time.time_ns() // 1_000 + options.duration_seconds * 1_000_000
        if options.duration_seconds > 0
        else None
    )
    async with anyio.create_task_group() as task_group:
        for info in market_info:
            mapping = info.mapping
            account = AccountRef(
                address=options.account_address,
                account_index=options.account_index,
                market_id=mapping.market_id,
                market=mapping.market,
            )
            runtime = MakerRuntime(
                account=account,
                quote_config=options.quote_config,
                submit=options.submit,
                signing_key=options.signing_key,
                run_id=uuid.uuid4().hex[:12],
                maximum_basis_bps=options.maximum_basis_bps,
                basis_window_seconds=options.basis_window_seconds,
                basis_minimum_samples=options.basis_samples,
                maximum_feed_age_ms=options.maximum_feed_age_ms,
                maximum_book_age_ms=options.maximum_book_age_ms,
                maximum_pair_skew_ms=options.maximum_pair_skew_ms,
                requote_interval_ms=options.requote_interval_ms,
                minimum_order_rest_ms=options.minimum_order_rest_ms,
                duration_seconds=options.duration_seconds,
                run_deadline_ns=run_deadline_ns,
                run_expiration_time_us=run_expiration_time_us,
                max_traded_notional_usd=options.max_traded_notional_usd,
                max_loss_usd=options.max_loss_usd,
                mainnet=options.mainnet,
                candidate_mode=options.candidate_mode,
                max_alpha_bps=options.max_alpha_bps,
                alpha_report_path=options.alpha_report_path,
                alpha_report_data=options.alpha_report_data,
            )
            logger.info(
                "%s Arcus %s market=%s id=%s Binance=%s",
                "Submitting to" if options.submit else "Previewing",
                "mainnet" if options.mainnet else "testnet",
                mapping.market,
                mapping.market_id,
                mapping.binance_symbol,
            )
            _ = task_group.start_soon(run_market, runtime, info)


def main() -> int:
    """Run the maker in dry-run mode or explicitly on Arcus."""
    try:
        options = parse_options()
        configure_logging(options.log_level)
        anyio.run(_run, options)
        return 0
    except KeyboardInterrupt:
        return 0
    except InputError as error:
        print(f"Input error: {error}", file=sys.stderr)
        return 2
    except (ProtocolError, TimeoutError, OSError, websockets.WebSocketException) as error:
        print(f"Arcus maker stopped: {error}", file=sys.stderr)
        return 1
    except BaseExceptionGroup as errors:
        for error in errors.exceptions:
            logger.exception("Arcus maker task failed", exc_info=error)
        print(f"Arcus maker stopped: {errors!r}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
