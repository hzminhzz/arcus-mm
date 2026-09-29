"""Continuous, freshness-gated Arcus maker session."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from time import monotonic_ns
from typing import Generic, TypeVar, cast

import anyio
import websockets

from arcus_bot.alpha.replay import compute_microprice
from arcus_bot.bots.market_maker.market import MAINNET_WS, TESTNET_WS, MarketInfo
from arcus_bot.bots.market_maker.order_manager import MakerOrderManager
from arcus_bot.bots.market_maker.quoter import BasisEstimator, Quote
from arcus_bot.bots.market_maker.runtime import (
    MAXIMUM_ACCOUNT_AGE_MS,
    MakerClient,
    MakerOrderActions,
    MakerRuntime,
)
from arcus_bot.cli.maker_config import verify_authentic_go_report
from arcus_bot.pricing.binance import BinanceBookTicker, BinanceBookTickerFeed
from arcus_bot.pricing.hyperliquid import HyperliquidBookTicker, HyperliquidBookTickerFeed
from arcus_bot.sdk.client import ArcusClient
from arcus_bot.sdk.orders import ArcusOrders
from arcus_bot.types import JsonObject, OrderConfig, ProtocolError

logger = logging.getLogger(__name__)

type ReferenceTicker = BinanceBookTicker | HyperliquidBookTicker
FeedT = TypeVar("FeedT", BinanceBookTickerFeed, HyperliquidBookTickerFeed)


def check_candidate_health(
    now_ns: int,
    candidate_ticker: ReferenceTicker | None,
    maximum_candidate_age_ms: int,
) -> tuple[bool, str]:
    """Check candidate BBO and quantity health and freshness."""
    if candidate_ticker is None:
        return False, "candidate ticker is not available"
    if candidate_ticker.bid_qty is None or candidate_ticker.ask_qty is None:
        return False, "candidate missing top-of-book quantities"
    if now_ns - candidate_ticker.received_at_ns > maximum_candidate_age_ms * 1_000_000:
        return False, "candidate feed is stale"
    if candidate_ticker.bid_qty <= 0 or candidate_ticker.ask_qty <= 0:
        return False, "candidate top-of-book quantities must be positive"
    if (
        candidate_ticker.bid <= 0
        or candidate_ticker.ask <= candidate_ticker.bid
        or not candidate_ticker.bid.is_finite()
        or not candidate_ticker.ask.is_finite()
    ):
        return False, "candidate prices must be finite, positive, and uncrossed"
    return True, "healthy"


@dataclass(slots=True)
class ContinuousMaker(Generic[FeedT]):
    """Manage one market's Arcus state, reference feed, and owned quotes."""

    runtime: MakerRuntime
    market: MarketInfo
    client: MakerClient
    feed: FeedT
    orders: MakerOrderActions | None
    candidate_feed: FeedT | None = None
    candidate_mode: str = "off"
    max_alpha_bps: Decimal = Decimal("0")
    alpha_report_path: str | Path | None = None
    alpha_report_data: JsonObject | None = None
    maximum_candidate_age_ms: int = 1_000
    recover_existing_orders: bool = False
    order_manager: MakerOrderManager = field(init=False)
    basis: BasisEstimator = field(init=False)
    last_requote_at_ns: int = 0
    last_status: str = ""
    last_preview: tuple[Quote, ...] = ()
    last_shadow_log: str = ""
    last_candidate_offset_bps: Decimal | None = None
    last_candidate_quotes: tuple[Quote, ...] = ()
    last_non_go_reason: str = ""
    starting_equity: Decimal | None = None
    session_started: bool = False
    stale_since_ns: int = 0
    stale_reason: str = ""

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
        self.candidate_mode = self.runtime.candidate_mode
        self.max_alpha_bps = self.runtime.max_alpha_bps
        self.alpha_report_path = self.runtime.alpha_report_path
        self.alpha_report_data = self.runtime.alpha_report_data

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
            if self.client.state.open_orders and not self.recover_existing_orders:
                raise ProtocolError(
                    f"Arcus reported existing {self.market.mapping.market} orders; "
                    + "refusing to take ownership"
                )
            if self.client.state.account_equity is None:
                raise ProtocolError("Arcus account-equity snapshot was not received")
            self.starting_equity = self.client.state.account_equity
            if self.recover_existing_orders:
                await self.order_manager.cancel_open_orders()
        else:
            await self.client.subscribe("l2Orderbook", self.runtime.account.market)

        self.session_started = True
        try:
            async with anyio.create_task_group() as task_group:
                _ = task_group.start_soon(self.feed.run)
                try:
                    if self.runtime.duration_seconds:
                        remaining_seconds = self.runtime.duration_seconds
                        if self.runtime.run_deadline_ns is not None:
                            remaining_seconds = max(
                                0,
                                (self.runtime.run_deadline_ns - monotonic_ns()) / 1_000_000_000,
                            )
                        with anyio.move_on_after(
                            remaining_seconds
                        ) as duration_scope:
                            await self._quote_loop()
                        if duration_scope.cancelled_caught:
                            logger.info(
                                "Maker run duration completed for %s",
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
            _ = await self.step()

    async def step(self, now_ns: int | None = None) -> tuple[Quote, ...] | None:
        """Execute one evaluation and quoting step."""
        return await self._step(now_ns)

    async def _step(self, now_ns: int | None = None) -> tuple[Quote, ...] | None:
        """Execute one evaluation and quoting step."""
        current_time_ns = monotonic_ns() if now_ns is None else now_ns
        await self._check_live_limits(current_time_ns)
        reference: ReferenceTicker | None = self.feed.latest
        feed_label = getattr(self.feed, "feed_name", "Binance")
        reason = freshness_reason(
            current_time_ns,
            reference,
            self.client.orderbook.received_at_ns,
            self.runtime,
            feed_name=feed_label,
        )
        if reason is not None:
            await self.pause(reason)
            if self._stale_duration_exceeded(reason, current_time_ns):
                raise OSError(f"market data remained stale: {reason}")
            return None
        self.stale_since_ns = 0
        self.stale_reason = ""
        if reference is None:
            raise ProtocolError(f"freshness check accepted a missing {feed_label} reference")

        try:
            best_bid, best_ask = self.client.orderbook.current()
        except ProtocolError:
            await self.pause("Arcus orderbook is invalid")
            return None
        local_mid = (best_bid + best_ask) / 2
        fair_anchor = reference.fair_anchor
        instantaneous_basis_bps = (
            abs(local_mid - fair_anchor) * Decimal(10_000) / fair_anchor
        )
        if instantaneous_basis_bps > self.runtime.maximum_basis_bps:
            await self.pause(f"Arcus/{feed_label} basis exceeded its configured limit")
            return None

        sample_at_ns = max(
            self.client.orderbook.received_at_ns,
            reference.received_at_ns,
        )
        self.basis.add_sample(local_mid, fair_anchor, sample_at_ns)
        baseline_fair_price = self.basis.fair_price(fair_anchor, current_time_ns)
        if baseline_fair_price is None:
            await self.pause(f"Waiting for a qualified Arcus/{feed_label} basis")
            return None

        baseline_quotes = self.order_manager.quotes(baseline_fair_price, best_bid, best_ask)

        if self.candidate_mode == "off":
            effective_fair_price = baseline_fair_price
            quotes = baseline_quotes
        elif self.candidate_mode == "shadow":
            cand_ticker = (
                self.candidate_feed.latest if self.candidate_feed is not None else reference
            )
            is_healthy, health_reason = check_candidate_health(
                current_time_ns, cand_ticker, self.maximum_candidate_age_ms
            )
            if (
                is_healthy
                and cand_ticker is not None
                and cand_ticker.bid_qty is not None
                and cand_ticker.ask_qty is not None
            ):
                try:
                    cand_m = compute_microprice(
                        cand_ticker.bid, cand_ticker.ask, cand_ticker.bid_qty, cand_ticker.ask_qty
                    )
                    cand_mid = (cand_ticker.bid + cand_ticker.ask) / Decimal("2")
                    cand_offset = cand_m - cand_mid
                    cand_offset_bps = (cand_offset / cand_mid) * Decimal("10000")
                    self.last_candidate_offset_bps = cand_offset_bps
                    cand_fair_price = baseline_fair_price + cand_offset
                    cand_quotes = self.order_manager.quotes(cand_fair_price, best_bid, best_ask)
                    self.last_candidate_quotes = cand_quotes
                    offset_desc = f"{cand_offset:+.4f} ({cand_offset_bps:+.2f} bps)"
                    health_desc = "healthy"
                except Exception as exc:
                    cand_quotes = ()
                    self.last_candidate_quotes = ()
                    offset_desc = "error"
                    health_desc = f"unhealthy ({exc})"
            else:
                cand_quotes = ()
                self.last_candidate_quotes = ()
                offset_desc = "N/A"
                health_desc = f"unhealthy ({health_reason})"

            shadow_log = (
                f"SHADOW {self.market.mapping.market} | baseline={baseline_quotes} | "
                f"candidate={cand_quotes} | offset={offset_desc} | health={health_desc}"
            )
            logger.info(shadow_log)
            self.last_shadow_log = shadow_log
            effective_fair_price = baseline_fair_price
            quotes = baseline_quotes
        elif self.candidate_mode == "bounded":
            is_go, go_reason, _ = verify_authentic_go_report(
                report_path=self.alpha_report_path,
                report_data=self.alpha_report_data,
            )
            cand_ticker = (
                self.candidate_feed.latest if self.candidate_feed is not None else reference
            )
            is_healthy, health_reason = check_candidate_health(
                current_time_ns, cand_ticker, self.maximum_candidate_age_ms
            )
            if not is_go:
                self.last_non_go_reason = go_reason
                logger.warning(
                    "Candidate non-GO fallback for %s: %s; using baseline fair price %s",
                    self.market.mapping.market,
                    go_reason,
                    baseline_fair_price,
                )
                effective_fair_price = baseline_fair_price
                quotes = baseline_quotes
            elif not is_healthy:
                self.last_non_go_reason = health_reason
                logger.warning(
                    "Candidate unhealthy for %s: %s; using baseline fair price %s",
                    self.market.mapping.market,
                    health_reason,
                    baseline_fair_price,
                )
                effective_fair_price = baseline_fair_price
                quotes = baseline_quotes
            else:
                assert (
                    cand_ticker is not None
                    and cand_ticker.bid_qty is not None
                    and cand_ticker.ask_qty is not None
                )
                try:
                    cand_m = compute_microprice(
                        cand_ticker.bid, cand_ticker.ask, cand_ticker.bid_qty, cand_ticker.ask_qty
                    )
                    cand_mid = (cand_ticker.bid + cand_ticker.ask) / Decimal("2")
                    raw_offset = cand_m - cand_mid
                    raw_offset_bps = (raw_offset / cand_mid) * Decimal("10000")
                    clamped_bps = max(-self.max_alpha_bps, min(self.max_alpha_bps, raw_offset_bps))
                    bounded_offset = baseline_fair_price * clamped_bps / Decimal("10000")
                    effective_fair_price = baseline_fair_price + bounded_offset
                    quotes = self.order_manager.quotes(effective_fair_price, best_bid, best_ask)
                    logger.info(
                        "BOUNDED CANDIDATE %s: raw_bps=%.2f clamped_bps=%.2f offset=%s fair=%s",
                        self.market.mapping.market,
                        raw_offset_bps,
                        clamped_bps,
                        bounded_offset,
                        effective_fair_price,
                    )
                except Exception as exc:
                    self.last_non_go_reason = str(exc)
                    logger.warning(
                        "Candidate evaluation failed for %s: %s; using baseline fair price %s",
                        self.market.mapping.market,
                        exc,
                        baseline_fair_price,
                    )
                    effective_fair_price = baseline_fair_price
                    quotes = baseline_quotes
        else:
            raise ProtocolError(f"unknown candidate mode: {self.candidate_mode!r}")

        if not self.runtime.submit:
            self._log_preview(effective_fair_price, quotes)
            return quotes
        if current_time_ns - self.last_requote_at_ns < self.runtime.requote_interval_ms * 1_000_000:
            return quotes
        await self.order_manager.reconcile(quotes, effective_fair_price, best_bid, best_ask)
        self.last_requote_at_ns = current_time_ns
        return quotes

    def _stale_duration_exceeded(self, reason: str, now_ns: int) -> bool:
        """Reconnect after a full freshness window without market data."""
        if reason != self.stale_reason:
            self.stale_reason = reason
            self.stale_since_ns = now_ns
            return False
        if self.stale_since_ns == 0:
            self.stale_since_ns = now_ns
            return False
        grace_ms = max(
            self.runtime.maximum_feed_age_ms,
            self.runtime.maximum_book_age_ms,
            self.runtime.maximum_pair_skew_ms,
        )
        return now_ns - self.stale_since_ns >= grace_ms * 1_000_000

    async def _check_live_limits(self, now_ns: int) -> None:
        """Fail closed when live turnover or account-equity limits are reached."""
        if not self.runtime.submit:
            return
        maximum_turnover = self.runtime.max_traded_notional_usd
        maximum_loss = self.runtime.max_loss_usd
        equity = self.client.state.account_equity
        if maximum_loss is not None:
            if equity is None or self.client.state.account_updated_at_ns == 0:
                raise ProtocolError("Arcus account-equity update is unavailable")
            account_age_ns = now_ns - self.client.state.account_updated_at_ns
            if account_age_ns < 0 or account_age_ns > MAXIMUM_ACCOUNT_AGE_MS * 1_000_000:
                await self.pause("Arcus account-equity update is stale")
                raise ProtocolError("Arcus account-equity update is stale")
            if self.starting_equity is None:
                self.starting_equity = equity
            if self.starting_equity - equity >= maximum_loss:
                await self.pause("maximum account-equity drawdown reached")
                raise ProtocolError("maximum account-equity drawdown reached")
        if maximum_turnover is not None:
            if self.client.state.cumulative_traded_notional_usd >= maximum_turnover:
                await self.pause("maximum cumulative traded notional reached")
                raise ProtocolError("maximum cumulative traded notional reached")

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
    recover_existing_orders = False
    environment = "mainnet" if runtime.mainnet else "testnet"
    while True:
        maker: ContinuousMaker[BinanceBookTickerFeed] | ContinuousMaker[HyperliquidBookTickerFeed] | None = None
        remaining_seconds: float | None = None
        if runtime.run_deadline_ns is not None:
            remaining_seconds = (
                runtime.run_deadline_ns - monotonic_ns()
            ) / 1_000_000_000
            if remaining_seconds <= 0:
                return
        try:
            url = MAINNET_WS if runtime.mainnet else TESTNET_WS
            async with websockets.connect(
                url,
                open_timeout=(
                    15 if remaining_seconds is None else min(15, remaining_seconds)
                ),
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
                if runtime.reference_feed == "hyperliquid":
                    hl_sym = (
                        runtime.reference_symbol
                        or market.mapping.hyperliquid_symbol
                        or ""
                    )
                    maker = ContinuousMaker(
                        runtime=runtime,
                        market=market,
                        client=client,
                        feed=HyperliquidBookTickerFeed(hl_sym),
                        orders=orders,
                        recover_existing_orders=recover_existing_orders,
                    )
                else:
                    bn_sym = (
                        runtime.reference_symbol
                        or market.mapping.binance_symbol
                        or ""
                    )
                    maker = ContinuousMaker(
                        runtime=runtime,
                        market=market,
                        client=client,
                        feed=BinanceBookTickerFeed(bn_sym),
                        orders=orders,
                        recover_existing_orders=recover_existing_orders,
                    )
                if runtime.run_deadline_ns is None:
                    await maker.run()
                else:
                    remaining_seconds = max(
                        0,
                        (runtime.run_deadline_ns - monotonic_ns()) / 1_000_000_000,
                    )
                    with anyio.move_on_after(remaining_seconds) as duration_scope:
                        await maker.run()
                    if duration_scope.cancelled_caught:
                        return
                return
        except (OSError, TimeoutError, websockets.WebSocketException) as error:
            if maker is not None and maker.session_started:
                recover_existing_orders = True
            logger.warning(
                "Arcus %s disconnected for %s; reconnecting in %ss: %s",
                environment,
                market.mapping.market,
                reconnect_seconds,
                error,
            )
        except BaseExceptionGroup as errors:
            if not is_recoverable_session_error(errors):
                raise
            if maker is not None and maker.session_started:
                recover_existing_orders = True
            logger.warning(
                "Arcus %s session failed for %s; reconnecting in %ss: %s",
                environment,
                market.mapping.market,
                reconnect_seconds,
                errors,
            )
        delay_seconds = reconnect_seconds
        if runtime.run_deadline_ns is not None:
            remaining_seconds = (
                runtime.run_deadline_ns - monotonic_ns()
            ) / 1_000_000_000
            if remaining_seconds <= 0:
                return
            delay_seconds = min(delay_seconds, remaining_seconds)
        await anyio.sleep(delay_seconds)
        reconnect_seconds = min(reconnect_seconds * 2, 30)


def is_recoverable_session_error(error: BaseExceptionGroup[BaseException]) -> bool:
    """Identify transport/RPC failures that can safely restart a session."""
    for item in error.exceptions:
        if isinstance(item, BaseExceptionGroup):
            if not is_recoverable_session_error(cast(BaseExceptionGroup[BaseException], item)):
                return False
        elif not isinstance(item, (OSError, TimeoutError, websockets.WebSocketException)):
            return False
    return True


def freshness_reason(
    now_ns: int,
    reference: ReferenceTicker | None,
    book_received_at_ns: int,
    runtime: MakerRuntime,
    feed_name: str = "Binance",
) -> str | None:
    """Return a fail-closed pause reason when either feed is missing, stale, or skewed."""
    if reference is None:
        return f"{feed_name} reference is not available"
    if now_ns - reference.received_at_ns > runtime.maximum_feed_age_ms * 1_000_000:
        return f"{feed_name} reference is stale"
    if (
        book_received_at_ns == 0
        or now_ns - book_received_at_ns > runtime.maximum_book_age_ms * 1_000_000
    ):
        return "Arcus orderbook is stale"
    if abs(reference.received_at_ns - book_received_at_ns) > runtime.maximum_pair_skew_ms * 1_000_000:
        return "reference and book pair skew exceeds limit"
    return None
