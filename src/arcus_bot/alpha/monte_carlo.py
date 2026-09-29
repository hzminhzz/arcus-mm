from __future__ import annotations

import math
import random
import statistics
import urllib.request
from dataclasses import dataclass
from typing import Final, cast
import http.client

from arcus_bot.types import JSON_ADAPTER

_BPS: Final = 10_000


@dataclass(frozen=True, slots=True)
class MarketParams:
    symbol: str
    venue: str
    current_price: float
    volatility_per_min: float
    intensity_A: float = 8.0
    intensity_kappa: float = 0.5


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    order_size_usd: float
    max_position_usd: float
    maker_fee_bps: float
    minimum_edge_bps: float
    latency_buffer_bps: float
    inventory_skew_bps: float
    emergency_flatten_ratio: float = 1.20
    emergency_flatten_buffer_bps: float = 10.0


@dataclass(slots=True)
class PathResult:
    total_pnl: float
    max_drawdown: float
    num_fills: int
    over_cap_time_pct: float
    emergency_flatten_count: int
    final_inventory: float


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    config: SimulationConfig
    mean_pnl: float
    pnl_std: float
    sharpe_ratio: float
    mean_drawdown: float
    max_drawdown: float
    fill_rate_per_hour: float
    over_cap_risk_pct: float
    emergency_flatten_rate: float
    score: float


def fetch_binance_market_params(symbol: str = "ZECUSDT") -> MarketParams:
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=120"
    req = urllib.request.Request(url, headers={"User-Agent": "arcus-monte-carlo"})
    resp = cast(http.client.HTTPResponse, urllib.request.urlopen(req, timeout=10))
    with resp:
        raw_bytes = resp.read()
        parsed = JSON_ADAPTER.validate_json(raw_bytes)

    if not isinstance(parsed, list):
        return MarketParams(symbol=symbol, venue="Binance", current_price=1370.0, volatility_per_min=0.002)

    closes: list[float] = []
    for item in parsed:
        if isinstance(item, list) and len(item) > 4:
            val = item[4]
            if isinstance(val, (str, int, float)):
                closes.append(float(val))

    if not closes:
        return MarketParams(symbol=symbol, venue="Binance", current_price=1370.0, volatility_per_min=0.002)

    current_price = closes[-1]
    log_returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    vol_1m = statistics.stdev(log_returns) if len(log_returns) > 1 else 0.002
    return MarketParams(
        symbol=symbol,
        venue="Binance",
        current_price=current_price,
        volatility_per_min=vol_1m,
        intensity_A=10.0,
        intensity_kappa=0.6,
    )


def fetch_hyperliquid_market_params(coin: str = "xyz:HOOD") -> MarketParams:
    url = "https://api.hyperliquid.xyz/info"
    body = b'{"type":"candleSnapshot","req":{"coin":"' + coin.encode() + b'","interval":"1m","startTime":0}}'
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "User-Agent": "arcus-monte-carlo"})
    resp = cast(http.client.HTTPResponse, urllib.request.urlopen(req, timeout=10))
    with resp:
        raw_bytes = resp.read()
        parsed = JSON_ADAPTER.validate_json(raw_bytes)

    if not isinstance(parsed, list) or not parsed:
        return MarketParams(symbol=coin, venue="Hyperliquid", current_price=115.0, volatility_per_min=0.0015)

    closes: list[float] = []
    for row in parsed[-120:]:
        if isinstance(row, dict):
            c_val = row.get("c")
            if isinstance(c_val, (str, int, float)):
                closes.append(float(c_val))

    if not closes:
        return MarketParams(symbol=coin, venue="Hyperliquid", current_price=115.0, volatility_per_min=0.0015)

    current_price = closes[-1]
    log_returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]
    vol_1m = statistics.stdev(log_returns) if len(log_returns) > 1 else 0.0015
    return MarketParams(
        symbol=coin,
        venue="Hyperliquid",
        current_price=current_price,
        volatility_per_min=vol_1m,
        intensity_A=6.0,
        intensity_kappa=0.4,
    )


def simulate_path(
    market: MarketParams,
    config: SimulationConfig,
    n_steps: int = 60,
    dt_min: float = 1.0,
    rng: random.Random | None = None,
) -> PathResult:
    if rng is None:
        rng = random.Random()

    price = market.current_price
    position = 0.0
    cash = 0.0
    peak_equity = 0.0
    max_dd = 0.0
    num_fills = 0
    over_cap_steps = 0
    emergency_flattens = 0

    order_qty_base = config.order_size_usd / price
    max_position_qty = config.max_position_usd / price
    emergency_threshold_usd = config.max_position_usd * config.emergency_flatten_ratio

    for _ in range(n_steps):
        z = rng.gauss(0, 1)
        jump = (rng.gauss(0, 3) * market.volatility_per_min) if rng.random() < 0.05 else 0.0
        pct_change = (market.volatility_per_min * math.sqrt(dt_min) * z) + jump
        price *= math.exp(pct_change)
        price = max(price, 0.01)

        position_notional = abs(position) * price
        if position_notional > config.max_position_usd:
            over_cap_steps += 1

        if position_notional >= emergency_threshold_usd:
            emergency_flattens += 1
            excess_qty = abs(position) - (config.max_position_usd / price)
            if excess_qty > 0:
                slippage = config.emergency_flatten_buffer_bps / _BPS
                if position > 0:
                    fill_px = price * (1.0 - slippage)
                    position -= excess_qty
                    cash += excess_qty * fill_px * (1.0 - config.maker_fee_bps / _BPS)
                else:
                    fill_px = price * (1.0 + slippage)
                    position += excess_qty
                    cash -= excess_qty * fill_px * (1.0 + config.maker_fee_bps / _BPS)
                num_fills += 1

        inv_ratio = max(-1.0, min(1.0, position / max_position_qty))
        center = price * (1.0 - inv_ratio * config.inventory_skew_bps / _BPS)
        half_spread_bps = max(config.maker_fee_bps + config.minimum_edge_bps + config.latency_buffer_bps, 0.0)
        buy_price = center * (1.0 - half_spread_bps / _BPS)
        sell_price = center * (1.0 + half_spread_bps / _BPS)

        if position < 0 and buy_price > price:
            buy_price = price
        elif position > 0 and sell_price < price:
            sell_price = price

        allow_buy = True
        allow_sell = True
        if position_notional > config.max_position_usd:
            if position > 0:
                allow_buy = False
            elif position < 0:
                allow_sell = False

        buy_delta_bps = max(0.0, (price - buy_price) / price * _BPS)
        sell_delta_bps = max(0.0, (sell_price - price) / price * _BPS)

        buy_intensity = market.intensity_A * math.exp(-market.intensity_kappa * buy_delta_bps)
        sell_intensity = market.intensity_A * math.exp(-market.intensity_kappa * sell_delta_bps)

        buy_fill_prob = 1.0 - math.exp(-buy_intensity * dt_min)
        sell_fill_prob = 1.0 - math.exp(-sell_intensity * dt_min)

        if allow_buy and rng.random() < buy_fill_prob:
            qty = min(order_qty_base, abs(position)) if position < 0 else order_qty_base
            position += qty
            cash -= qty * buy_price * (1.0 + config.maker_fee_bps / _BPS)
            num_fills += 1
            price -= 0.25 * market.volatility_per_min * price

        if allow_sell and rng.random() < sell_fill_prob:
            qty = min(order_qty_base, position) if position > 0 else order_qty_base
            position -= qty
            cash += qty * sell_price * (1.0 - config.maker_fee_bps / _BPS)
            num_fills += 1
            price += 0.25 * market.volatility_per_min * price

        equity = cash + (position * price)
        if equity > peak_equity:
            peak_equity = equity
        dd = peak_equity - equity
        if dd > max_dd:
            max_dd = dd

    total_pnl = cash + (position * price)
    return PathResult(
        total_pnl=total_pnl,
        max_drawdown=max_dd,
        num_fills=num_fills,
        over_cap_time_pct=over_cap_steps / n_steps,
        emergency_flatten_count=emergency_flattens,
        final_inventory=position,
    )


def run_monte_carlo(
    market: MarketParams,
    config: SimulationConfig,
    n_paths: int = 500,
    n_steps: int = 60,
    seed: int = 42,
) -> OptimizationResult:
    rng = random.Random(seed)
    results = [
        simulate_path(market, config, n_steps=n_steps, rng=rng)
        for _ in range(n_paths)
    ]

    pnls = [r.total_pnl for r in results]
    drawdowns = [r.max_drawdown for r in results]
    fills = [r.num_fills for r in results]
    over_caps = [r.over_cap_time_pct for r in results]
    flattens = [r.emergency_flatten_count for r in results]

    mean_pnl = statistics.mean(pnls)
    std_pnl = statistics.stdev(pnls) if len(pnls) > 1 and statistics.stdev(pnls) > 0 else 1.0
    sharpe = (mean_pnl / std_pnl) * math.sqrt(24 * 365)
    mean_dd = statistics.mean(drawdowns)
    max_dd = max(drawdowns)
    fill_rate = statistics.mean(fills)
    over_cap_pct = statistics.mean(over_caps) * 100
    flatten_rate = statistics.mean(flattens)

    score = sharpe - (mean_dd / config.max_position_usd * 2.0) - (flatten_rate * 5.0)

    return OptimizationResult(
        config=config,
        mean_pnl=mean_pnl,
        pnl_std=std_pnl,
        sharpe_ratio=sharpe,
        mean_drawdown=mean_dd,
        max_drawdown=max_dd,
        fill_rate_per_hour=fill_rate,
        over_cap_risk_pct=over_cap_pct,
        emergency_flatten_rate=flatten_rate,
        score=score,
    )


def optimize_market(market: MarketParams, n_paths: int = 300) -> list[OptimizationResult]:
    grid: list[OptimizationResult] = []
    edges = [1.5, 2.5, 4.0]
    skews = [10.0, 20.0, 30.0]
    caps = [300.0, 500.0, 1000.0]

    for edge in edges:
        for skew in skews:
            for cap in caps:
                for sz in [cap * 0.10, cap * 0.20]:
                    cfg = SimulationConfig(
                        order_size_usd=sz,
                        max_position_usd=cap,
                        maker_fee_bps=0.0,
                        minimum_edge_bps=edge,
                        latency_buffer_bps=1.0,
                        inventory_skew_bps=skew,
                        emergency_flatten_ratio=1.20,
                    )
                    res = run_monte_carlo(market, cfg, n_paths=n_paths)
                    grid.append(res)

    grid.sort(key=lambda r: r.score, reverse=True)
    return grid
