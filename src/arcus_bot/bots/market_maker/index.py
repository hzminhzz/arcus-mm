"""Continuous, freshness-gated Arcus maker session."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from time import monotonic_ns

import anyio
import websockets

from arcus_bot.bots.market_maker.market import MAINNET_WS, TESTNET_WS, MarketInfo
from arcus_bot.bots.market_maker.order_manager import MakerOrderManager
from arcus_bot.bots.market_maker.quoter import BasisEstimator, Quote
from arcus_bot.bots.market_maker.runtime import MakerClient, MakerOrderActions, MakerRuntime
from arcus_bot.pricing.binance import BinanceBookTickerFeed
from arcus_bot.pricing.binance import BinanceBookTicker
from arcus_bot.sdk.client import ArcusClient
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import OrderConfig, ProtocolError

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ContinuousMaker:
    """Manage one market's Arcus state, reference feed, and owned quotes."""

    runtime: MakerRuntime
    market: MarketInfo
    client: MakerClient
    feed: BinanceBookTickerFeed
    orders: MakerOrderActions | None
    order_manager: MakerOrderManager = field(init=False)
    basis: BasisEstimator = field(init=False)
    last_requote_at_ns: int = 0
    last_status: str = ""
    last_preview: tuple[Quote, ...] = ()

    def __post_init__(self) -> None:
        self.basis = BasisEstimator(
            window_ns=self.runtime.basis_window_seconds * 1_000_000_000,
            maximum_basis_bps=self.runtime.maximum_basis_bps,
            minimum_samples=self.runtime.basis_minimum_samples,
        )
        self.order_manager = MakerOrderManager(
            runtime=self.runtime,
            market=self.market,
            client=self.client,
            orders=self.orders,
        )

    async def run(self) -> None:
        """Subscribe, quote while healthy, then cancel and confirm on exit."""
        if self.runtime.submit:
            await self.client.subscribe_maker_channels()
            if not self.client.state.position_ready:
                raise ProtocolError("Arcus position snapshot was not received")
            if not self.client.state.orders_ready:
                raise ProtocolError("Arcus open-order snapshot was not received")
            if self.orders is None:
                raise ProtocolError("live maker session has no Arcus order adapter")
            if self.client.state.open_orders:
                logger.warning(
                    "Canceling %s existing %s orders before starting the dedicated maker",
                    len(self.client.state.open_orders),
                    self.market.mapping.market,
                )
                await self.order_manager.cancel_open_orders()
        else:
            await self.client.subscribe("l2Orderbook", self.runtime.account.market)

        try:
            async with anyio.create_task_group() as task_group:
                _ = task_group.start_soon(self.feed.run)
                try:
                    if self.runtime.dry_run_duration_seconds:
                        with anyio.move_on_after(
                            self.runtime.dry_run_duration_seconds
                        ) as duration_scope:
                            await self._quote_loop()
                        if duration_scope.cancelled_caught:
                            logger.info(
                                "DRY-RUN duration completed for %s",
                                self.market.mapping.market,
                            )
                    else:
                        await self._quote_loop()
                finally:
                    task_group.cancel_scope.cancel()
        finally:
            if self.runtime.submit:
                with anyio.CancelScope(shield=True):
                    await self.order_manager.cancel_open_orders()

    async def _quote_loop(self) -> None:
        """Recompute quotes from each Arcus frame and timed freshness check."""
        while True:
            with anyio.move_on_after(0.2):
                _ = await self.client.next_message()
            now_ns = monotonic_ns()
            reference = self.feed.latest
            reason = freshness_reason(
                now_ns,
                reference,
                self.client.orderbook.received_at_ns,
                self.runtime,
            )
            if reason is not None:
                await self.pause(reason)
                continue
            if reference is None:
                raise ProtocolError("freshness check accepted a missing Binance reference")

            try:
                best_bid, best_ask = self.client.orderbook.current()
            except ProtocolError:
                await self.pause("Arcus orderbook is invalid")
                continue
            local_mid = (best_bid + best_ask) / 2
            pair_skew_ns = abs(
                self.client.orderbook.received_at_ns - reference.received_at_ns
            )
            if pair_skew_ns > self.runtime.maximum_pair_skew_ms * 1_000_000:
                await self.pause("Binance and Arcus prices are not synchronized")
                continue
            instantaneous_basis_bps = (
                abs(local_mid - reference.mid) * Decimal(10_000) / reference.mid
            )
            if instantaneous_basis_bps > self.runtime.maximum_basis_bps:
                await self.pause("Arcus/Binance basis exceeded its configured limit")
                continue

            sample_at_ns = max(
                self.client.orderbook.received_at_ns,
                reference.received_at_ns,
            )
            self.basis.add_sample(local_mid, reference.mid, sample_at_ns)
            fair_price = self.basis.fair_price(reference.mid, now_ns)
            if fair_price is None:
                await self.pause("Waiting for a qualified Arcus/Binance basis")
                continue

            quotes = self.order_manager.quotes(fair_price, best_bid, best_ask)
            if not self.runtime.submit:
                self._log_preview(fair_price, quotes)
                continue
            if now_ns - self.last_requote_at_ns < self.runtime.requote_interval_ms * 1_000_000:
                continue
            await self.order_manager.reconcile(quotes, fair_price, best_bid, best_ask)
            self.last_requote_at_ns = now_ns

    async def pause(self, reason: str) -> None:
        """Cancel all owned orders when pricing or market state is unusable."""
        if reason != self.last_status:
            logger.warning("Maker paused for %s: %s", self.market.mapping.market, reason)
            self.last_status = reason
        if self.runtime.submit and (
            self.client.state.open_orders or self.order_manager.tracked_orders
        ):
            await self.order_manager.cancel_open_orders()

    def _log_preview(self, fair_price: Decimal, quotes: tuple[Quote, ...]) -> None:
        """Emit one non-trading quote preview when the calculated quote changes."""
        if not quotes or quotes == self.last_preview:
            return
        self.last_preview = quotes
        formatted = " ".join(
            f"{quote.side} {quote.quantity}@{quote.price}" for quote in quotes
        )
        logger.info(
            "DRY-RUN %s fair=%s basisSamples=%s %s",
            self.market.mapping.market,
            fair_price,
            len(self.basis.samples),
            formatted,
        )


async def run_market(runtime: MakerRuntime, market: MarketInfo) -> None:
    """Run one market in the selected Arcus environment."""
    reconnect_seconds = 1
    environment = "mainnet" if runtime.mainnet else "testnet"
    while True:
        try:
            url = MAINNET_WS if runtime.mainnet else TESTNET_WS
            async with websockets.connect(
                url,
                open_timeout=15,
                ping_interval=20,
                ping_timeout=20,
            ) as socket:
                client = ArcusClient(socket, runtime.account)
                order_config = OrderConfig(
                    account=runtime.account,
                    tick_size=market.mapping.tick_size,
                    step_size=market.mapping.step_size,
                )
                orders: ArcusOrders | None = (
                    ArcusOrders(client, order_config, runtime.signing_key)
                    if runtime.submit and runtime.signing_key is not None
                    else None
                )
                maker = ContinuousMaker(
                    runtime=runtime,
                    market=market,
                    client=client,
                    feed=BinanceBookTickerFeed(market.mapping.binance_symbol),
                    orders=orders,
                )
                await maker.run()
                return
        except (OSError, TimeoutError, websockets.WebSocketException) as error:
            logger.warning(
                "Arcus %s disconnected for %s; reconnecting in %ss: %s",
                environment,
                market.mapping.market,
                reconnect_seconds,
                error,
            )
        await anyio.sleep(reconnect_seconds)
        reconnect_seconds = min(reconnect_seconds * 2, 30)


def freshness_reason(
    now_ns: int,
    reference: BinanceBookTicker | None,
    book_received_at_ns: int,
    runtime: MakerRuntime,
) -> str | None:
    """Return a fail-closed pause reason when either feed is missing or stale."""
    if reference is None:
        return "Binance reference is not available"
    if now_ns - reference.received_at_ns > runtime.maximum_feed_age_ms * 1_000_000:
        return "Binance reference is stale"
    if (
        book_received_at_ns == 0
        or now_ns - book_received_at_ns > runtime.maximum_book_age_ms * 1_000_000
    ):
        return "Arcus orderbook is stale"
    if abs(book_received_at_ns - reference.received_at_ns) > runtime.maximum_pair_skew_ms * 1_000_000:
        return "Binance and Arcus prices are not synchronized"
    return None
