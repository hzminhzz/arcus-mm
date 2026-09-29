"""Run the continuous Arcus market maker."""

from __future__ import annotations

import logging
import sys
import time
import uuid

import anyio
import websockets

from arcus_bot.bots.market_maker.index import run_market
from arcus_bot.bots.market_maker.market import MAINNET_WS, TESTNET_WS, MarketInfo, fetch_market_info
from arcus_bot.bots.market_maker.order_manager import MakerOrderManager
from arcus_bot.bots.market_maker.runtime import MakerRuntime
from arcus_bot.cli.maker_config import MakerOptions, parse_options
from arcus_bot.pricing.binance import BinanceBookTickerFeed
from arcus_bot.pricing.hyperliquid import HyperliquidBookTicker, HyperliquidBookTickerFeed
from arcus_bot.sdk.client import ArcusClient
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
            feed_type, feed_symbol = options.market_feeds.get(
                mapping.market,
                mapping.resolve_feed(
                    options.reference_feed if options.reference_feed != "auto" else None
                ),
            )
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
                reference_feed=feed_type,
                reference_symbol=feed_symbol,
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
                emergency_flatten_ratio=options.emergency_flatten_ratio,
                emergency_flatten_buffer_bps=options.emergency_flatten_buffer_bps,
            )
            logger.info(
                "%s Arcus %s market=%s id=%s feed=%s symbol=%s",
                "Submitting to" if options.submit else "Previewing",
                "mainnet" if options.mainnet else "testnet",
                mapping.market,
                mapping.market_id,
                feed_type,
                feed_symbol,
            )
            _ = task_group.start_soon(run_market, runtime, info)


async def _preview_feeds(options: MakerOptions) -> None:
    """Safe, non-trading live preview of reference feeds and Arcus orderbooks."""
    environment = "mainnet" if options.mainnet else "testnet"
    url = MAINNET_WS if options.mainnet else TESTNET_WS

    for mapping in options.markets:
        feed_type, feed_symbol = options.market_feeds.get(
            mapping.market,
            mapping.resolve_feed(
                options.reference_feed if options.reference_feed != "auto" else None
            ),
        )
        info = await fetch_market_info(mapping, options.mainnet)
        feed = (
            HyperliquidBookTickerFeed(feed_symbol)
            if feed_type == "hyperliquid"
            else BinanceBookTickerFeed(feed_symbol)
        )

        account = AccountRef(
            address=options.account_address,
            account_index=options.account_index,
            market_id=mapping.market_id,
            market=mapping.market,
        )
        runtime = MakerRuntime(
            account=account,
            quote_config=options.quote_config,
            submit=False,
            signing_key=None,
            run_id="preview",
            maximum_basis_bps=options.maximum_basis_bps,
            reference_feed=feed_type,
            reference_symbol=feed_symbol,
        )
        async with anyio.create_task_group() as tg:
            _ = tg.start_soon(feed.run)
            async with websockets.connect(url, open_timeout=10) as socket:
                client = ArcusClient(socket, account)
                order_manager = MakerOrderManager(
                    runtime=runtime,
                    market=info,
                    client=client,
                    orders=None,
                )
                await client.subscribe("l2Orderbook", mapping.market)

                with anyio.move_on_after(10) as scope:
                    while True:
                        with anyio.move_on_after(0.2):
                            _ = await client.next_message()
                        try:
                            best_bid, best_ask = client.orderbook.current()
                            if feed.latest is not None:
                                break
                        except ProtocolError:
                            continue

                tg.cancel_scope.cancel()

                if scope.cancelled_caught or feed.latest is None:
                    print(
                        f"FEED PREVIEW {mapping.market}: Timeout waiting for feed or book snapshot\n"
                    )
                    continue

                best_bid, best_ask = client.orderbook.current()
                arcus_mid = (best_bid + best_ask) / 2
                ref = feed.latest
                ref_anchor = ref.fair_anchor
                basis_bps = (abs(arcus_mid - ref_anchor) * 10_000) / ref_anchor
                basis_status = (
                    "PASS" if basis_bps <= options.maximum_basis_bps else "EXCEEDED"
                )

                quotes = order_manager.quotes(ref_anchor, best_bid, best_ask)
                quote_str = " ".join(
                    f"{q.side}{'[RO]' if q.reduce_only else ''} {q.quantity}@{q.price}" for q in quotes
                )

                oracle_desc = (
                    f", oracle={ref.oracle_px}"
                    if isinstance(ref, HyperliquidBookTicker) and ref.oracle_px is not None
                    else ""
                )
                bar = "=" * 72
                banner = (
                    f"\n{bar}\n"
                    + f"PREVIEW {mapping.market} (id={mapping.market_id}) on Arcus {environment}\n"
                    + f"Reference Feed:  {feed_type.upper()} ({feed_symbol})\n"
                    + f"Reference BBO:   bid={ref.bid} ask={ref.ask} (mid={ref.mid}{oracle_desc})\n"
                    + f"Arcus BBO:       bid={best_bid} ask={best_ask} (mid={arcus_mid})\n"
                    + f"Basis:           {basis_bps:.2f} bps (max: {options.maximum_basis_bps} bps) [{basis_status}]\n"
                    + f"Fair Anchor:     {ref_anchor}\n"
                    + f"Generated Quotes: {quote_str or 'None (risk filter or inventory)'}\n"
                    + f"{bar}\n"
                )
                print(banner)


def main() -> int:
    """Run the maker in dry-run mode or explicitly on Arcus."""
    try:
        options = parse_options()
        configure_logging(options.log_level)
        if getattr(options, "preview_feeds", False):
            anyio.run(_preview_feeds, options)
            return 0
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
