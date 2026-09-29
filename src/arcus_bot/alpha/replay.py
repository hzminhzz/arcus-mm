from __future__ import annotations

import argparse
import hashlib
import time
import json
import math
import random
from decimal import Decimal, InvalidOperation
from pathlib import Path

from arcus_bot.types import JSON_ADAPTER, JsonObject, JsonValue


def compute_top_of_book_imbalance(
    bid_qty: Decimal | str | float | int,
    ask_qty: Decimal | str | float | int,
) -> Decimal:
    try:
        qb = Decimal(str(bid_qty))
        qa = Decimal(str(ask_qty))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid quantity: bid_qty={bid_qty}, ask_qty={ask_qty}") from exc

    if not (qb.is_finite() and qa.is_finite()):
        raise ValueError(f"Quantities must be finite: qb={qb}, qa={qa}")
    if qb <= 0 or qa <= 0:
        raise ValueError(f"Quantities must be positive: qb={qb}, qa={qa}")

    denom = qb + qa
    if denom == 0:
        raise ValueError("Sum of quantities cannot be zero")

    return (qb - qa) / denom


def compute_microprice(
    best_bid: Decimal | str | float | int,
    best_ask: Decimal | str | float | int,
    bid_qty: Decimal | str | float | int,
    ask_qty: Decimal | str | float | int,
) -> Decimal:
    try:
        bid = Decimal(str(best_bid))
        ask = Decimal(str(best_ask))
        qb = Decimal(str(bid_qty))
        qa = Decimal(str(ask_qty))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("Invalid numeric input to compute_microprice") from exc

    if not (bid.is_finite() and ask.is_finite() and qb.is_finite() and qa.is_finite()):
        raise ValueError("Prices and quantities must be finite")
    if bid <= 0 or ask <= 0 or qb <= 0 or qa <= 0:
        raise ValueError("Prices and quantities must be positive")
    if bid >= ask:
        raise ValueError(f"Bid price ({bid}) must be strictly less than ask price ({ask})")

    denom = qb + qa
    if denom == 0:
        raise ValueError("Sum of quantities cannot be zero")

    return (qb * ask + qa * bid) / denom


def compute_microprice_offset(
    best_bid: Decimal | str | float | int,
    best_ask: Decimal | str | float | int,
    bid_qty: Decimal | str | float | int,
    ask_qty: Decimal | str | float | int,
) -> Decimal:
    bid = Decimal(str(best_bid))
    ask = Decimal(str(best_ask))
    m = compute_microprice(best_bid, best_ask, bid_qty, ask_qty)
    mid = (bid + ask) / Decimal("2")
    return m - mid


def load_jsonl(path: Path | str) -> list[JsonObject]:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    records: list[JsonObject] = []
    with p.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rec_val: JsonValue = JSON_ADAPTER.validate_json(line)
            except Exception as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_num}: {exc}") from exc
            if not isinstance(rec_val, dict):
                raise ValueError(f"Expected JSON object at {path}:{line_num}")
            records.append(rec_val)
    return records


def validate_point_in_time_order(public_records: list[JsonObject]) -> None:
    last_recv_time_ns = -1
    last_event_time_ms = -1

    for idx, rec in enumerate(public_records):
        for field in ("recv_time_ns", "event_time_ms", "b", "a", "B", "A"):
            if field not in rec:
                raise ValueError(f"Missing required field '{field}' at index {idx}")

        recv_time_ns = int(str(rec["recv_time_ns"]))
        event_time_ms = int(str(rec["event_time_ms"]))

        if recv_time_ns < last_recv_time_ns:
            raise ValueError(
                f"Receipt monotonic time decreased at index {idx}: {recv_time_ns} < {last_recv_time_ns}"
            )
        if event_time_ms < last_event_time_ms:
            raise ValueError(
                f"Event timestamp decreased at index {idx}: {event_time_ms} < {last_event_time_ms}"
            )

        bid = Decimal(str(rec["b"]))
        ask = Decimal(str(rec["a"]))
        if bid >= ask:
            raise ValueError(
                f"Crossed or zero-spread book at index {idx}: bid={bid}, ask={ask}"
            )

        last_recv_time_ns = recv_time_ns
        last_event_time_ms = event_time_ms


def calculate_after_fee_markout_bps(
    fill: JsonObject,
    public_records: list[JsonObject],
    horizon_ms: int = 100,
    cost_multiplier: float = 1.0,
) -> Decimal | None:
    fill_time_val = fill.get("event_time_ms")
    if fill_time_val is None:
        raise ValueError("Fill missing event_time_ms")
    fill_time_ms = int(str(fill_time_val))
    fill_price = Decimal(str(fill["price"]))
    fill_qty = Decimal(str(fill["quantity"]))
    fill_fee = Decimal(str(fill.get("fee", "0")))
    side = str(fill["side"]).upper()

    target_time_ms = fill_time_ms + horizon_ms

    future_rec: JsonObject | None = None
    for rec in public_records:
        rec_event_time = rec.get("event_time_ms")
        if (rec.get("symbol") == fill.get("symbol")
            and rec.get("venue") == fill.get("venue")
            and ("policy" not in rec or rec["policy"] == fill.get("policy"))
            and rec_event_time is not None
            and int(str(rec_event_time)) >= target_time_ms):
            if future_rec is None or int(str(rec_event_time)) < int(str(future_rec["event_time_ms"])):
                future_rec = rec

    if future_rec is None:
        return None

    future_bid = Decimal(str(future_rec["b"]))
    future_ask = Decimal(str(future_rec["a"]))
    future_mid = (future_bid + future_ask) / Decimal("2")

    notional = fill_price * fill_qty
    fee_bps = (fill_fee / notional * Decimal("10000")) if notional > 0 else Decimal("0")

    if side == "BUY":
        raw_bps = (future_mid - fill_price) / fill_price * Decimal("10000")
    elif side == "SELL":
        raw_bps = (fill_price - future_mid) / fill_price * Decimal("10000")
    else:
        raise ValueError(f"Unknown side: {side}")

    markout_bps = raw_bps - (fee_bps * Decimal(str(cost_multiplier)))
    return markout_bps


def block_bootstrap_difference_ci(
    baseline_diffs: list[float],
    block_size: int = 5,
    n_bootstraps: int = 1000,
    seed: int = 42,
) -> tuple[float, float]:
    if not baseline_diffs:
        return (0.0, 0.0)

    n = len(baseline_diffs)
    if n < block_size:
        block_size = max(1, n)

    rng = random.Random(seed)
    n_blocks = math.ceil(n / block_size)

    blocks = [baseline_diffs[i : i + block_size] for i in range(n - block_size + 1)]
    if not blocks:
        blocks = [baseline_diffs]

    boot_means: list[float] = []
    for _ in range(n_bootstraps):
        sample: list[float] = []
        for _ in range(n_blocks):
            blk = rng.choice(blocks)
            sample.extend(blk)
        sample = sample[:n]
        boot_means.append(sum(sample) / len(sample))

    boot_means.sort()
    low_idx = int(0.025 * n_bootstraps)
    high_idx = int(0.975 * n_bootstraps)
    return (boot_means[low_idx], boot_means[high_idx])


def evaluate_replay(
    public_records: list[JsonObject],
    fills_records: list[JsonObject],
    action_horizon_ms: int = 100,
    churn_penalty_threshold: float | None = None,
    observed_churn_ratio: float = 0.0,
) -> JsonObject:
    validate_point_in_time_order(public_records)

    symbols: set[str] = set()
    for r in public_records:
        sym = r.get("symbol")
        if isinstance(sym, str):
            symbols.add(sym)
    for f in fills_records:
        sym = f.get("symbol")
        if isinstance(sym, str):
            symbols.add(sym)

    symbol: str = next(iter(symbols)) if len(symbols) == 1 else ("ALL" if symbols else "UNKNOWN")

    independent_decisions_count = len(public_records)

    has_synthetic_fills = any(
        f.get("source") != "observed" or f.get("provenance") != "observed"
        for f in fills_records
    )

    baseline_observed_fills = [
        f for f in fills_records
        if "baseline" in str(f.get("policy", "")).lower()
        and f.get("source") == "observed"
        and f.get("provenance") == "observed"
    ]

    candidate_observed_fills = [
        f for f in fills_records
        if "candidate" in str(f.get("policy", "")).lower()
        and f.get("source") == "observed"
        and f.get("provenance") == "observed"
    ]

    total_observed_fills = len(baseline_observed_fills) + len(candidate_observed_fills)
    now_ms = int(time.time() * 1000)

    cost_stress_results: dict[str, JsonValue] = {
        "1x": {"baseline_markout_bps": None, "candidate_markout_bps": None, "net_difference_bps": None, "positive": None, "status": "unavailable"},
        "2x": {"baseline_markout_bps": None, "candidate_markout_bps": None, "net_difference_bps": None, "positive": None, "status": "unavailable"},
        "3x": {"baseline_markout_bps": None, "candidate_markout_bps": None, "net_difference_bps": None, "positive": None, "status": "unavailable"},
    }

    base_res: JsonObject = {
        "decision": "INCONCLUSIVE",
        "reason": "",
        "symbol": symbol,
        "independent_decisions_count": independent_decisions_count,
        "observed_fills_count": total_observed_fills,
        "baseline_observed_fills_count": len(baseline_observed_fills),
        "candidate_observed_fills_count": len(candidate_observed_fills),
        "action_latency_provenance": "unavailable",
        "action_horizon_ms": action_horizon_ms,
        "baseline_markout_bps": None,
        "candidate_markout_bps": None,
        "net_difference_bps": None,
        "bootstrap_ci_95": None,
        "halves_sign_stable": None,
        "economics_status": {
            field: "unavailable" for field in (
                "baseline_markout_bps", "candidate_markout_bps", "net_difference_bps",
                "bootstrap_ci_95", "halves_sign_stable",
            )
        },
        "cost_stress_results": cost_stress_results,
    }

    if churn_penalty_threshold is not None and observed_churn_ratio > churn_penalty_threshold:
        base_res["decision"] = "NO_GO"
        base_res["reason"] = (
            f"Excessive quote churn ratio {observed_churn_ratio:.2f} exceeded threshold {churn_penalty_threshold:.2f}"
        )
        return base_res

    if has_synthetic_fills:
        base_res["decision"] = "NO_GO"
        base_res["reason"] = "Dataset contains synthetic or counterfactual fills; cannot certify GO"
        return base_res

    if len(baseline_observed_fills) < 30 or len(candidate_observed_fills) < 30:
        base_res["decision"] = "INCONCLUSIVE"
        base_res["reason"] = (
            f"Insufficient observed fills: baseline has {len(baseline_observed_fills)} (need >=30), "
            f"candidate has {len(candidate_observed_fills)} (need >=30)"
        )
        return base_res

    if independent_decisions_count < 100:
        base_res["decision"] = "INCONCLUSIVE"
        base_res["reason"] = (
            f"Insufficient independent decisions: {independent_decisions_count} (need >=100)"
        )
        return base_res

    # An offline replay may describe old data, but it cannot certify current GO.
    timestamps = [int(str(r["event_time_ms"])) for r in public_records]
    timestamps.extend(int(str(f["event_time_ms"])) for f in fills_records)
    if not timestamps or min(timestamps) < now_ms - 86400_000 or max(timestamps) > now_ms:
        base_res["decision"] = "NO_GO"
        base_res["reason"] = "Missing, stale or future replay observations"
        return base_res

    def has_measured_latency(fill: JsonObject) -> bool:
        latency = fill.get("action_latency_ms")
        sent = fill.get("action_sent_time_ms")
        fill_time = fill.get("event_time_ms")
        receipt = fill.get("recv_time_ns")
        return (fill.get("action_latency_source") == "measured"
                and isinstance(latency, (int, float)) and not isinstance(latency, bool)
                and math.isfinite(latency) and latency >= 0
                and isinstance(sent, int) and not isinstance(sent, bool)
                and isinstance(fill_time, int) and not isinstance(fill_time, bool)
                and fill_time - sent == latency
                and isinstance(receipt, int) and receipt > 0
                and isinstance(fill.get("venue"), str) and bool(fill["venue"]))

    if not all(has_measured_latency(f) for f in baseline_observed_fills + candidate_observed_fills):
        base_res["reason"] = "Missing measured action latency or fill venue provenance"
        return base_res
    base_res["action_latency_provenance"] = "measured"

    paired_n = min(len(baseline_observed_fills), len(candidate_observed_fills))
    paired_baseline = baseline_observed_fills[:paired_n]
    paired_candidate = candidate_observed_fills[:paired_n]

    if any(calculate_after_fee_markout_bps(f, public_records, action_horizon_ms) is None
           for f in paired_baseline + paired_candidate):
        base_res["reason"] = "Missing matching post-horizon book observation"
        return base_res

    diffs_1x: list[float] = []
    base_markouts_1x: list[float] = []
    cand_markouts_1x: list[float] = []

    for mult in (1.0, 2.0, 3.0):
        b_marks = [
            float(calculate_after_fee_markout_bps(f, public_records, action_horizon_ms, mult) or Decimal("0"))
            for f in paired_baseline
        ]
        c_marks = [
            float(calculate_after_fee_markout_bps(f, public_records, action_horizon_ms, mult) or Decimal("0"))
            for f in paired_candidate
        ]
        diffs = [c - b for c, b in zip(c_marks, b_marks)]
        mean_b = sum(b_marks) / len(b_marks) if b_marks else 0.0
        mean_c = sum(c_marks) / len(c_marks) if c_marks else 0.0
        mean_diff = sum(diffs) / len(diffs) if diffs else 0.0

        key = f"{int(mult)}x"
        cost_stress_results[key] = {
            "baseline_markout_bps": round(mean_b, 4),
            "candidate_markout_bps": round(mean_c, 4),
            "net_difference_bps": round(mean_diff, 4),
            "positive": mean_diff > 0,
            "status": "estimated",
        }

        if mult == 1.0:
            diffs_1x = diffs
            base_markouts_1x = b_marks
            cand_markouts_1x = c_marks

    mean_base_1x = sum(base_markouts_1x) / len(base_markouts_1x)
    mean_cand_1x = sum(cand_markouts_1x) / len(cand_markouts_1x)
    mean_diff_1x = sum(diffs_1x) / len(diffs_1x)

    ci_low, ci_high = block_bootstrap_difference_ci(diffs_1x, block_size=5, n_bootstraps=1000, seed=42)

    half_size = paired_n // 2
    first_half_diff = sum(diffs_1x[:half_size]) / half_size if half_size > 0 else 0.0
    second_half_diff = sum(diffs_1x[half_size:]) / (paired_n - half_size) if (paired_n - half_size) > 0 else 0.0
    halves_sign_stable = (first_half_diff > 0 and second_half_diff > 0)

    base_res["baseline_markout_bps"] = round(mean_base_1x, 4)
    base_res["candidate_markout_bps"] = round(mean_cand_1x, 4)
    base_res["net_difference_bps"] = round(mean_diff_1x, 4)
    base_res["bootstrap_ci_95"] = [round(ci_low, 4), round(ci_high, 4)]
    base_res["halves_sign_stable"] = halves_sign_stable
    base_res["economics_status"] = {
        field: "estimated" for field in (
            "baseline_markout_bps", "candidate_markout_bps", "net_difference_bps",
            "bootstrap_ci_95", "halves_sign_stable",
        )
    }

    cost_stress_1x = cost_stress_results["1x"]
    cost_stress_2x = cost_stress_results["2x"]
    cost_stress_3x = cost_stress_results["3x"]

    positive_1x = isinstance(cost_stress_1x, dict) and bool(cost_stress_1x.get("positive"))
    positive_2x = isinstance(cost_stress_2x, dict) and bool(cost_stress_2x.get("positive"))
    positive_3x = isinstance(cost_stress_3x, dict) and bool(cost_stress_3x.get("positive"))

    cost_stress_passed = positive_1x and positive_2x and positive_3x

    if not cost_stress_passed:
        base_res["decision"] = "NO_GO"
        base_res["reason"] = (
            f"Adverse costs stress test failed: 1x={positive_1x}, 2x={positive_2x}, 3x={positive_3x}"
        )
        return base_res

    if ci_low <= 0:
        base_res["decision"] = "NO_GO"
        base_res["reason"] = f"Bootstrap 95% lower bound is non-positive: {ci_low:.4f} <= 0"
        return base_res

    if not halves_sign_stable:
        base_res["decision"] = "NO_GO"
        base_res["reason"] = (
            f"Chronological halves unstable: first_half={first_half_diff:.4f}, second_half={second_half_diff:.4f}"
        )
        return base_res

    base_res["decision"] = "GO"
    base_res["reason"] = "All certification conditions met with statistically and economically significant edge"
    return base_res


class ReplayArguments(argparse.Namespace):
    input: str = ""
    fills: str = ""
    output: str = ""
    horizon_ms: int = 100


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate candidate alpha policy against baseline and fills")
    _ = parser.add_argument("--input", required=True, help="Path to public jsonl fixture or feed")
    _ = parser.add_argument("--fills", required=True, help="Path to fills jsonl fixture")
    _ = parser.add_argument("--output", required=True, help="Path to output json summary")
    _ = parser.add_argument("--horizon-ms", type=int, default=100, help="Action markout horizon in ms")
    args = ReplayArguments()
    _ = parser.parse_args(namespace=args)

    public_records = load_jsonl(args.input)
    fills_records = load_jsonl(args.fills)

    eval_result = evaluate_replay(
        public_records=public_records,
        fills_records=fills_records,
        action_horizon_ms=args.horizon_ms,
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    eval_result["generated_at_ms"] = int(time.time() * 1000)
    eval_result["data_provenance"] = {
        "public_path": str(Path(args.input).resolve()),
        "public_sha256": hashlib.sha256(Path(args.input).read_bytes()).hexdigest(),
        "fills_path": str(Path(args.fills).resolve()),
        "fills_sha256": hashlib.sha256(Path(args.fills).read_bytes()).hexdigest(),
    }
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(eval_result, f, indent=2)

    print(json.dumps(eval_result, indent=2))


if __name__ == "__main__":
    main()
