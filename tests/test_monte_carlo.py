from __future__ import annotations

from arcus_bot.alpha.monte_carlo import (
    MarketParams,
    SimulationConfig,
    optimize_market,
    run_monte_carlo,
    simulate_path,
)


def test_simulate_path_runs_without_errors() -> None:
    market = MarketParams(
        symbol="TEST",
        venue="Mock",
        current_price=100.0,
        volatility_per_min=0.001,
    )
    config = SimulationConfig(
        order_size_usd=10.0,
        max_position_usd=100.0,
        maker_fee_bps=0.0,
        minimum_edge_bps=2.0,
        latency_buffer_bps=1.0,
        inventory_skew_bps=15.0,
        emergency_flatten_ratio=1.20,
    )
    res = simulate_path(market, config, n_steps=10)
    assert res.num_fills >= 0
    assert res.max_drawdown >= 0.0


def test_run_monte_carlo_computes_sharpe_and_drawdown() -> None:
    market = MarketParams(
        symbol="TEST",
        venue="Mock",
        current_price=100.0,
        volatility_per_min=0.001,
    )
    config = SimulationConfig(
        order_size_usd=10.0,
        max_position_usd=100.0,
        maker_fee_bps=0.0,
        minimum_edge_bps=2.0,
        latency_buffer_bps=1.0,
        inventory_skew_bps=15.0,
    )
    opt = run_monte_carlo(market, config, n_paths=20, n_steps=10)
    assert opt.sharpe_ratio is not None
    assert opt.mean_drawdown >= 0.0
    assert opt.fill_rate_per_hour >= 0.0


def test_optimize_market_returns_sorted_results() -> None:
    market = MarketParams(
        symbol="TEST",
        venue="Mock",
        current_price=100.0,
        volatility_per_min=0.001,
    )
    grid = optimize_market(market, n_paths=5)
    assert len(grid) > 0
    assert grid[0].score >= grid[-1].score
