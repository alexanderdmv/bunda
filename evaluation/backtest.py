"""evaluation/backtest.py

A practical, "good enough" backtest/diagnostics tool for the current project stage.

What it does:
1) Reads decisions CSV (prefers data/processed/decisions_v2.csv)
2) Shows dataset health: counts, WATCH rate, missingness
3) Compares WATCH vs SKIP distributions (median/quantiles) for key metrics
4) If price snapshots exist (multiple rows per token with non-null price_usd):
   computes forward returns over several horizons (e.g., 15/60/240 minutes)

This is NOT a full trading backtest (no slippage, fees, execution, fills).
It's designed to help you tune filter thresholds based on real collected stream data.

Run from project root (crypto_filter/):
  python -m evaluation.backtest

Or specify a CSV:
  python -m evaluation.backtest --csv data/processed/decisions_v2.csv

Optional forward-return horizons in minutes:
  python -m evaluation.backtest --horizons 5 15 60 240

If your CSV does not contain repeated snapshots/prices, the forward-return section
will gracefully skip.
"""

from __future__ import annotations

import argparse
import math
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd


DEFAULT_CSV_CANDIDATES = [
    os.path.join("data", "processed", "decisions_v2.csv"),
    os.path.join("data", "processed", "decisions.csv"),
]


NUMERIC_COLS = [
    "liquidity_usd",
    "price_usd",
    "volume_h24_usd",
    "price_change_h24",
    "market_cap_usd",
    "fdv_usd",
    "trade_h24",
    "unique_wallet_h24",
    "holders",
    "last_trade_unix",
    "last_trade_minutes",
    "age_minutes",
]


COMPARE_COLS = [
    # Most useful for tuning
    "liquidity_usd",
    "volume_h24_usd",
    "trade_h24",
    "unique_wallet_h24",
    "holders",
    "last_trade_minutes",
    "price_change_h24",
    "age_minutes",
]


@dataclass
class BacktestResult:
    horizon_min: int
    n_pairs: int
    mean_return: float
    median_return: float
    win_rate: float


def _pick_csv(path_arg: Optional[str]) -> str:
    if path_arg:
        return path_arg
    for p in DEFAULT_CSV_CANDIDATES:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(
        "No decisions CSV found. Looked for: " + ", ".join(DEFAULT_CSV_CANDIDATES)
    )


def _safe_to_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _make_key(df: pd.DataFrame) -> pd.Series:
    """Token key: prefer token_address; fallback to symbol; last resort name."""
    addr = df.get("token_address")
    sym = df.get("symbol")
    name = df.get("name")

    def _norm(x: object) -> str:
        if x is None:
            return ""
        s = str(x).strip()
        return s

    keys: List[str] = []
    for i in range(len(df)):
        a = _norm(addr.iat[i]) if addr is not None else ""
        s = _norm(sym.iat[i]) if sym is not None else ""
        n = _norm(name.iat[i]) if name is not None else ""

        if a:
            keys.append(a)
        elif s:
            keys.append(f"SYM:{s}")
        elif n:
            keys.append(f"NAME:{n}")
        else:
            # Worst case: row index (won't join across snapshots)
            keys.append(f"ROW:{i}")
    return pd.Series(keys, index=df.index, name="token_key")


def print_health(df: pd.DataFrame) -> None:
    print("\n=== DATASET HEALTH ===")
    print(f"Rows: {len(df):,}")

    if "decision" in df.columns:
        vc = df["decision"].fillna("(missing)").value_counts(dropna=False)
        print("\nDecision counts:")
        print(vc.to_string())
        if vc.sum() > 0 and "WATCH" in vc.index:
            watch_rate = float(vc["WATCH"]) / float(vc.sum())
            print(f"\nWATCH rate: {watch_rate:.2%}")

    # missingness
    cols = [c for c in ["token_address", "symbol", "liquidity_usd", "volume_h24_usd", "price_usd"] if c in df.columns]
    if cols:
        miss = df[cols].isna().mean().sort_values(ascending=False)
        print("\nMissingness (share of NaN) for key columns:")
        print((miss * 100).round(1).astype(str).add("%").to_string())


def _quantile_table(s: pd.Series) -> pd.Series:
    q = s.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
    q.index = ["p05", "p25", "p50", "p75", "p95"]
    return q


def print_compare(df: pd.DataFrame) -> None:
    if "decision" not in df.columns:
        print("\n(No 'decision' column found; skipping WATCH vs SKIP compare.)")
        return

    print("\n=== WATCH vs SKIP METRICS (quantiles) ===")

    decisions = [d for d in ["WATCH", "SKIP"] if d in set(df["decision"].dropna().unique())]
    if not decisions:
        print("No WATCH/SKIP rows found.")
        return

    for col in COMPARE_COLS:
        if col not in df.columns:
            continue

        print(f"\n-- {col} --")
        sub = df[["decision", col]].copy()
        sub[col] = pd.to_numeric(sub[col], errors="coerce")

        for d in decisions:
            s = sub.loc[sub["decision"] == d, col].dropna()
            if len(s) == 0:
                print(f"{d}: no data")
                continue
            q = _quantile_table(s)
            # compact, readable
            line = ", ".join([f"{idx}={qv:.4g}" for idx, qv in q.items()])
            print(f"{d}: n={len(s):,} | {line}")


def _forward_returns(
    df: pd.DataFrame,
    horizons_min: List[int],
    max_lookahead_multiplier: int = 3,
) -> List[BacktestResult]:
    """Compute forward returns using nearest snapshot after horizon.

    For each row (t0), find the first snapshot for the same token_key with
    timestamp >= t0 + horizon. Uses merge_asof within each token group.

    Needs:
      - timestamp_utc (ISO) parsable
      - price_usd numeric
      - multiple snapshots per token
    """

    required = {"timestamp_utc", "price_usd", "token_key"}
    if not required.issubset(df.columns):
        return []

    # keep only rows with timestamp & price
    base = df[["token_key", "timestamp_utc", "price_usd"]].copy()
    base["timestamp_utc"] = pd.to_datetime(base["timestamp_utc"], errors="coerce", utc=True)
    base["price_usd"] = pd.to_numeric(base["price_usd"], errors="coerce")
    base = base.dropna(subset=["timestamp_utc", "price_usd", "token_key"])
    if len(base) < 50:
        return []

    # Must have repeats
    counts = base["token_key"].value_counts()
    repeat_keys = set(counts[counts >= 2].index)
    base = base[base["token_key"].isin(repeat_keys)].copy()
    if len(base) < 50:
        return []

    base = base.sort_values(["token_key", "timestamp_utc"])

    results: List[BacktestResult] = []
    for h in horizons_min:
        # Create target time and asof-join within each token
        tmp = base.copy()
        tmp["t_target"] = tmp["timestamp_utc"] + pd.to_timedelta(h, unit="m")

        # We'll find the first observation with timestamp >= t_target.
        # merge_asof is <= by default; we invert by joining on sorted times with direction='forward'.
        merged = pd.merge_asof(
            tmp.sort_values(["token_key", "t_target"]),
            base.rename(columns={"timestamp_utc": "t_next", "price_usd": "price_next"})
                .sort_values(["token_key", "t_next"]),
            left_on="t_target",
            right_on="t_next",
            by="token_key",
            direction="forward",
            tolerance=pd.to_timedelta(h * max_lookahead_multiplier, unit="m"),
        )

        merged = merged.dropna(subset=["price_next"])
        if len(merged) == 0:
            continue

        r = (merged["price_next"] / merged["price_usd"]) - 1.0
        r = r.replace([math.inf, -math.inf], pd.NA).dropna()
        if len(r) == 0:
            continue

        res = BacktestResult(
            horizon_min=h,
            n_pairs=int(len(r)),
            mean_return=float(r.mean()),
            median_return=float(r.median()),
            win_rate=float((r > 0).mean()),
        )
        results.append(res)

    return results


def print_forward_backtest(df: pd.DataFrame, horizons: List[int]) -> None:
    print("\n=== FORWARD-RETURN BACKTEST (if possible) ===")

    out = _forward_returns(df, horizons)
    if not out:
        print(
            "Not enough price snapshots per token yet (or price_usd missing). "
            "Collect more stream data with prices, then rerun."
        )
        return

    rows = []
    for r in out:
        rows.append(
            {
                "horizon_min": r.horizon_min,
                "pairs": r.n_pairs,
                "mean_return": r.mean_return,
                "median_return": r.median_return,
                "win_rate": r.win_rate,
            }
        )

    tbl = pd.DataFrame(rows)
    # Friendly formatting
    tbl["mean_return"] = (tbl["mean_return"] * 100).round(2)
    tbl["median_return"] = (tbl["median_return"] * 100).round(2)
    tbl["win_rate"] = (tbl["win_rate"] * 100).round(2)

    print("All tokens (no decision split yet):")
    print(tbl.to_string(index=False))

    if "decision" not in df.columns:
        return

    # Split WATCH/SKIP
    for decision in ["WATCH", "SKIP"]:
        sub = df[df["decision"] == decision].copy()
        if len(sub) < 50:
            continue
        out_d = _forward_returns(sub, horizons)
        if not out_d:
            continue
        rows_d = []
        for r in out_d:
            rows_d.append(
                {
                    "horizon_min": r.horizon_min,
                    "pairs": r.n_pairs,
                    "mean_return": r.mean_return,
                    "median_return": r.median_return,
                    "win_rate": r.win_rate,
                }
            )
        tbl_d = pd.DataFrame(rows_d)
        tbl_d["mean_return"] = (tbl_d["mean_return"] * 100).round(2)
        tbl_d["median_return"] = (tbl_d["median_return"] * 100).round(2)
        tbl_d["win_rate"] = (tbl_d["win_rate"] * 100).round(2)
        print(f"\nDecision={decision}:")
        print(tbl_d.to_string(index=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest/diagnostics for crypto_filter decisions")
    parser.add_argument("--csv", type=str, default=None, help="Path to decisions CSV")
    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[5, 15, 60, 240],
        help="Forward-return horizons in minutes",
    )
    args = parser.parse_args()

    csv_path = _pick_csv(args.csv)
    print(f"Using CSV: {csv_path}")

    df = pd.read_csv(csv_path)
    df = _safe_to_numeric(df, NUMERIC_COLS)

    if "timestamp_utc" in df.columns:
        # Keep as string for now; parse later where needed
        pass

    df["token_key"] = _make_key(df)

    print_health(df)
    print_compare(df)
    print_forward_backtest(df, horizons=list(args.horizons))

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
