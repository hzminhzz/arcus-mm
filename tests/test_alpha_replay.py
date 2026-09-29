from __future__ import annotations

from decimal import Decimal
import hashlib
import json
import time
from pathlib import Path

import pytest

from arcus_bot.alpha.replay import (
    compute_microprice,
    compute_microprice_offset,
    compute_top_of_book_imbalance,
    evaluate_replay,
    calculate_after_fee_markout_bps,
)
from arcus_bot.cli.maker_config import verify_authentic_go_report
from arcus_bot.types import JSON_ADAPTER, JsonObject, JsonValue


FIXTURES_DIR = Path(__file__).parent / "fixtures"
PUBLIC_FIXTURE_PATH = FIXTURES_DIR / "alpha_public.jsonl"
FILLS_FIXTURE_PATH = FIXTURES_DIR / "alpha_fills.jsonl"


def load_jsonl(path: Path) -> list[JsonObject]:
    assert path.is_file(), f"Fixture file not found: {path}"
    records: list[JsonObject] = []
    with path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record: JsonValue = JSON_ADAPTER.validate_json(line)
            except ValueError as exc:
                raise AssertionError(f"Invalid JSON at {path}:{line_num}: {exc}") from exc
            assert isinstance(record, dict), f"Expected JSON object at {path}:{line_num}"
            records.append(record)
    return records


def certify_policy_go(
    policy_name: str,
    fills_records: list[JsonObject],
    required_min_fills: int = 1,
) -> str:
    policy_fills = [f for f in fills_records if f.get("policy") == policy_name]
    if not policy_fills:
        return "INCONCLUSIVE"

    has_synthetic = any(
        f.get("source") != "observed" or f.get("provenance") != "observed"
        for f in policy_fills
    )
    if has_synthetic:
        return "NO_GO"

    observed_fills = [
        f for f in policy_fills
        if f.get("source") == "observed" and f.get("provenance") == "observed"
    ]
    if len(observed_fills) < required_min_fills:
        return "INCONCLUSIVE"

    return "GO"


def test_point_in_time_fixture_excludes_future_prices():
    records = load_jsonl(PUBLIC_FIXTURE_PATH)
    assert len(records) > 0, "alpha_public.jsonl fixture is empty"

    last_recv_time_ns = -1
    last_event_time_ms = -1

    for idx, rec in enumerate(records):
        assert "recv_time_ns" in rec, f"Missing recv_time_ns at index {idx}"
        assert "event_time_ms" in rec, f"Missing event_time_ms at index {idx}"
        assert "b" in rec and "a" in rec, f"Missing BBO prices at index {idx}"
        assert "B" in rec and "A" in rec, f"Missing BBO quantities at index {idx}"

        recv_time_ns = rec["recv_time_ns"]
        event_time_ms = rec["event_time_ms"]
        assert isinstance(recv_time_ns, int)
        assert isinstance(event_time_ms, int)
        assert isinstance(rec["b"], str)
        assert isinstance(rec["a"], str)
        best_bid = float(rec["b"])
        best_ask = float(rec["a"])

        assert recv_time_ns >= last_recv_time_ns, (
            f"Receipt monotonic time decreased at index {idx}: {recv_time_ns} < {last_recv_time_ns}"
        )
        assert event_time_ms >= last_event_time_ms, (
            f"Event timestamp decreased at index {idx}: {event_time_ms} < {last_event_time_ms}"
        )
        assert best_bid < best_ask, f"Crossed or zero-spread book at index {idx}: bid={best_bid}, ask={best_ask}"

        last_recv_time_ns = recv_time_ns
        last_event_time_ms = event_time_ms


def test_synthetic_fills_cannot_certify_go():
    records = load_jsonl(FILLS_FIXTURE_PATH)
    assert len(records) > 0, "alpha_fills.jsonl fixture is empty"

    candidate_status = certify_policy_go("experimental_candidate_v2", records)
    assert candidate_status in ("INCONCLUSIVE", "NO_GO"), (
        f"Expected INCONCLUSIVE or NO_GO for synthetic fills, got {candidate_status}"
    )
    assert candidate_status == "NO_GO", f"Expected NO_GO due to synthetic provenance, got {candidate_status}"

    missing_status = certify_policy_go("unseen_future_policy", records)
    assert missing_status in ("INCONCLUSIVE", "NO_GO"), (
        f"Expected INCONCLUSIVE or NO_GO for missing fills, got {missing_status}"
    )
    assert missing_status == "INCONCLUSIVE", f"Expected INCONCLUSIVE for missing policy, got {missing_status}"

    baseline_status = certify_policy_go("maker_baseline_v1", records)
    assert baseline_status == "GO", f"Expected GO for strictly observed baseline, got {baseline_status}"

    tampered_records = [
        dict(r) if r.get("policy") != "maker_baseline_v1" else {**r, "provenance": "synthetic"}
        for r in records
    ]
    tampered_status = certify_policy_go("maker_baseline_v1", tampered_records)
    assert tampered_status in ("INCONCLUSIVE", "NO_GO")
    assert tampered_status == "NO_GO"


def test_candidate_microprice_math():
    bid_qty = Decimal("2.0")
    ask_qty = Decimal("1.0")
    imbalance = compute_top_of_book_imbalance(bid_qty, ask_qty)
    assert imbalance == Decimal("1.0") / Decimal("3.0")

    best_bid = Decimal("100.0")
    best_ask = Decimal("101.0")
    microprice = compute_microprice(best_bid, best_ask, bid_qty, ask_qty)
    expected_m = (Decimal("2.0") * Decimal("101.0") + Decimal("1.0") * Decimal("100.0")) / Decimal("3.0")
    assert microprice == expected_m

    offset = compute_microprice_offset(best_bid, best_ask, bid_qty, ask_qty)
    expected_mid = Decimal("100.5")
    assert offset == expected_m - expected_mid

    with pytest.raises(ValueError):
        _ = compute_top_of_book_imbalance(Decimal("-1"), Decimal("1"))
    with pytest.raises(ValueError):
        _ = compute_top_of_book_imbalance(Decimal("0"), Decimal("1"))
    with pytest.raises(ValueError):
        _ = compute_microprice(Decimal("101"), Decimal("100"), Decimal("1"), Decimal("1"))


def test_future_leak_rejected():
    public_recs: list[JsonObject] = [
        {"recv_time_ns": 200, "event_time_ms": 20, "symbol": "BTCUSDT", "b": "100", "a": "101", "B": "1", "A": "1"},
        {"recv_time_ns": 100, "event_time_ms": 10, "symbol": "BTCUSDT", "b": "100", "a": "101", "B": "1", "A": "1"},
    ]
    fills: list[JsonObject] = [
        {"recv_time_ns": 150, "event_time_ms": 15, "symbol": "BTCUSDT", "policy": "baseline", "side": "BUY", "price": "100", "quantity": "1", "fee": "0.01", "source": "observed", "provenance": "observed"}
    ]
    with pytest.raises(ValueError, match="Receipt monotonic time decreased"):
        _ = evaluate_replay(public_recs, fills)


def test_insufficient_fills_returns_inconclusive():
    public_recs: list[JsonObject] = [
        {
            "recv_time_ns": 1000 + i * 10,
            "event_time_ms": 100 + i,
            "symbol": "BTCUSDT",
            "b": "100.0",
            "a": "101.0",
            "B": "1.0",
            "A": "1.0",
        }
        for i in range(120)
    ]
    fills: list[JsonObject] = [
        {
            "recv_time_ns": 1000 + i * 10,
            "event_time_ms": 100 + i,
            "symbol": "BTCUSDT",
            "policy": "maker_baseline_v1",
            "side": "BUY",
            "price": "100.0",
            "quantity": "0.1",
            "fee": "0.001",
            "source": "observed",
            "provenance": "observed",
        }
        for i in range(10)
    ]
    res = evaluate_replay(public_recs, fills)
    assert res["decision"] == "INCONCLUSIVE"
    assert isinstance(res["reason"], str)
    assert "Insufficient observed fills" in res["reason"]


def test_adverse_costs_stress():
    public_recs: list[JsonObject] = [
        {
            "recv_time_ns": 1000 + i * 100,
            "event_time_ms": 1000 + i * 10,
            "symbol": "BTCUSDT",
            "b": "100.0",
            "a": "101.0",
            "B": "1.0",
            "A": "1.0",
        }
        for i in range(200)
    ]
    baseline_fills: list[JsonObject] = [
        {
            "recv_time_ns": 1000 + i * 10,
            "event_time_ms": 1000 + i,
            "symbol": "BTCUSDT",
            "policy": "baseline",
            "side": "BUY",
            "price": "100.5",
            "quantity": "1.0",
            "fee": "0.08",
            "source": "observed",
            "provenance": "observed",
        }
        for i in range(35)
    ]
    candidate_fills: list[JsonObject] = [
        {
            "recv_time_ns": 1000 + i * 10,
            "event_time_ms": 1000 + i,
            "symbol": "BTCUSDT",
            "policy": "candidate",
            "side": "BUY",
            "price": "100.5",
            "quantity": "1.0",
            "fee": "0.08",
            "source": "observed",
            "provenance": "observed",
        }
        for i in range(35)
    ]
    res = evaluate_replay(public_recs, baseline_fills + candidate_fills)
    assert res["decision"] == "NO_GO"


def test_queue_churn_penalty():
    public_recs: list[JsonObject] = [
        {
            "recv_time_ns": 1000 + i * 10,
            "event_time_ms": 100 + i,
            "symbol": "BTCUSDT",
            "b": "100.0",
            "a": "101.0",
            "B": "1.0",
            "A": "1.0",
        }
        for i in range(120)
    ]
    fills: list[JsonObject] = []
    res = evaluate_replay(
        public_recs,
        fills,
        churn_penalty_threshold=0.5,
        observed_churn_ratio=0.8,
    )
    assert res["decision"] == "NO_GO"
    assert isinstance(res["reason"], str)
    assert "Excessive quote churn ratio" in res["reason"]


def test_markout_never_uses_wrong_or_pre_horizon_book() -> None:
    fill: JsonObject = {"event_time_ms": 1000, "symbol": "BTCUSDT", "venue": "binance", "policy": "candidate", "price": "100", "quantity": "1", "fee": "0", "side": "BUY"}
    def book(t: int, symbol: str = "BTCUSDT", venue: str = "binance", policy: str = "candidate") -> JsonObject:
        return {"event_time_ms": t, "symbol": symbol, "venue": venue, "policy": policy, "b": "200", "a": "202"}
    for observations in ([book(500)], [book(1100, "ETHUSDT")], [book(1100, venue="other")], [book(1100, policy="baseline")]):
        assert calculate_after_fee_markout_bps(fill, observations) is None
    assert calculate_after_fee_markout_bps(fill, [book(500), book(1100)]) == Decimal("10100")


def _probe_data(*, age_ms: int = 0, matching: bool = True, latency: bool = True) -> tuple[list[JsonObject], list[JsonObject]]:
    now = int(time.time() * 1000) - age_ms
    books: list[JsonObject] = [{"recv_time_ns": i + 1, "event_time_ms": now - 12000 + i * 100,
        "symbol": "BTCUSDT" if matching else "ETHUSDT", "venue": "binance", "b": "100", "a": "102", "B": "1", "A": "1"} for i in range(120)]
    fills: list[JsonObject] = [{"event_time_ms": now - 12000 + i * 100, "symbol": "BTCUSDT", "venue": "binance",
        "policy": policy, "side": "BUY", "price": price, "quantity": "1", "fee": "0",
        "source": "observed", "provenance": "observed", "recv_time_ns": i + 1,
        **({"action_latency_source": "measured", "action_latency_ms": 10, "action_sent_time_ms": now - 12010 + i * 100} if latency else {})}
        for policy, price in (("baseline", "100"), ("candidate", "99")) for i in range(30)]
    return books, fills


def test_false_go_probe_guards() -> None:
    books, fills = _probe_data()
    assert evaluate_replay(books, fills)["decision"] == "GO"
    for bad_books, bad_fills in (
        _probe_data(age_ms=10 * 86400_000),
        _probe_data(age_ms=-86400_000),
        _probe_data(matching=False),
        _probe_data(latency=False),
    ):
        assert evaluate_replay(bad_books, bad_fills)["decision"] != "GO"
    # A total of 60 must not hide a policy with fewer than 30 observed fills.
    unbalanced = fills[:29] + fills[30:61]
    assert evaluate_replay(books, unbalanced)["decision"] == "INCONCLUSIVE"
    assert evaluate_replay(books, unbalanced)["baseline_observed_fills_count"] == 29


def test_forged_report_and_source_binding(tmp_path: Path) -> None:
    forged: JsonObject = {"decision": "GO", "reason": "ok", "independent_decisions_count": 100,
        "observed_fills_count": 30, "halves_sign_stable": True,
        "cost_stress_results": {key: {"positive": True} for key in ("1x", "2x", "3x")},
        "bootstrap_ci_95": [1, 2]}
    report_path = tmp_path / "report.json"
    _ = report_path.write_text(json.dumps(forged))
    assert not verify_authentic_go_report(report_path)[0]
    books, fills = _probe_data()
    public_path, fills_path = tmp_path / "public.jsonl", tmp_path / "fills.jsonl"
    for path, rows in ((public_path, books), (fills_path, fills)):
        _ = path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    report = evaluate_replay(books, fills)
    report["generated_at_ms"] = int(time.time() * 1000)
    report["data_provenance"] = {f"{name}_{field}": value for name, path in (("public", public_path), ("fills", fills_path))
        for field, value in (("path", str(path.resolve())), ("sha256", hashlib.sha256(path.read_bytes()).hexdigest()))}
    _ = report_path.write_text(json.dumps(report))
    assert verify_authentic_go_report(report_path)[0]
    report["candidate_observed_fills_count"] = 100
    _ = report_path.write_text(json.dumps(report))
    assert not verify_authentic_go_report(report_path)[0]
    report["candidate_observed_fills_count"] = 30
    report["generated_at_ms"] += 86400_000
    _ = report_path.write_text(json.dumps(report))
    assert not verify_authentic_go_report(report_path)[0]
    report["generated_at_ms"] -= 86400_000
    _ = report_path.write_text(json.dumps(report))
    _ = fills_path.write_text(fills_path.read_text() + "\n")
    assert not verify_authentic_go_report(report_path)[0]


def _assert_unavailable_economics(report: JsonObject) -> None:
    statuses = report["economics_status"]
    assert isinstance(statuses, dict)
    for field in ("baseline_markout_bps", "candidate_markout_bps", "net_difference_bps",
                  "bootstrap_ci_95", "halves_sign_stable"):
        assert report[field] is None
        assert statuses[field] == "unavailable"
    stress = report["cost_stress_results"]
    assert isinstance(stress, dict)
    for multiplier in ("1x", "2x", "3x"):
        row = stress[multiplier]
        assert isinstance(row, dict)
        assert row["status"] == "unavailable"
        for field in ("baseline_markout_bps", "candidate_markout_bps", "net_difference_bps", "positive"):
            assert row[field] is None


def test_absent_fills_do_not_report_zero_economics() -> None:
    books, _ = _probe_data()
    report = evaluate_replay(books, [])
    assert report["decision"] == "INCONCLUSIVE"
    assert report["observed_fills_count"] == 0
    _assert_unavailable_economics(report)


def test_missing_post_horizon_books_leave_economics_unavailable() -> None:
    books, fills = _probe_data()
    report = evaluate_replay(books, fills, action_horizon_ms=20_000)
    assert report["decision"] == "INCONCLUSIVE"
    assert report["reason"] == "Missing matching post-horizon book observation"
    assert report["action_latency_provenance"] == "measured"
    _assert_unavailable_economics(report)


def test_computed_economics_are_labeled_estimated() -> None:
    books, fills = _probe_data()
    report = evaluate_replay(books, fills)
    assert report["decision"] == "GO"
    statuses = report["economics_status"]
    assert isinstance(statuses, dict)
    for field in ("baseline_markout_bps", "candidate_markout_bps", "net_difference_bps",
                  "bootstrap_ci_95", "halves_sign_stable"):
        assert report[field] is not None
        assert statuses[field] == "estimated"
    stress = report["cost_stress_results"]
    assert isinstance(stress, dict)
    for multiplier in ("1x", "2x", "3x"):
        row = stress[multiplier]
        assert isinstance(row, dict)
        assert row["status"] == "estimated"
        for field in ("baseline_markout_bps", "candidate_markout_bps", "net_difference_bps", "positive"):
            assert row[field] is not None
