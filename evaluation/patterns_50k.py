from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from pipeline.control import load_control, deep_get
from evaluation.early_windows import compute_early_window_features

DB_PATH = Path("data/processed/tracker.db")
OUT_MD = Path("data/processed/patterns_50k_report.md")
OUT_CSV_HITS = Path("data/processed/hit_50k_tokens.csv")
OUT_CSV_STATS = Path("data/processed/hit_50k_stats.csv")

BASE_METRICS: List[str] = [
    # First-seen / baseline metrics
    "liquidity_usd",
    "volume_h24_usd",
    "trade_h24",
    "unique_wallet_h24",
    "holders",
    "fdv_usd",
    "market_cap_usd",
    "price_change_h24",
    "age_minutes",
]


def _pattern_settings(control: dict) -> Tuple[List[int], int]:
    """Return (windows_minutes, rest_sample_size)."""
    windows_raw = deep_get(control, "patterns.windows_minutes", [1, 5, 15])
    try:
        windows = sorted({int(x) for x in (windows_raw or [])})
    except Exception:
        windows = [1, 5, 15]
    if not windows:
        windows = [1, 5, 15]

    try:
        rest_n = int(deep_get(control, "patterns.rest_sample_size", 2000) or 2000)
    except Exception:
        rest_n = 2000

    return windows, max(rest_n, 0)


def _attach_early_features(
    con: sqlite3.Connection,
    df: pd.DataFrame,
    windows: List[int],
) -> pd.DataFrame:
    """Compute early-window features for each token in df (expects token_address, first_seen_utc)."""
    if df.empty:
        return df

    if "token_address" not in df.columns or "first_seen_utc" not in df.columns:
        return df

    features: List[Dict[str, float]] = []
    for _, row in df[["token_address", "first_seen_utc"]].iterrows():
        ta = str(row["token_address"])
        fs = row["first_seen_utc"]
        feats = compute_early_window_features(con, ta, fs, windows_minutes=windows)
        feats["token_address"] = ta
        features.append(feats)

    fdf = pd.DataFrame(features)
    if fdf.empty:
        return df

    return df.merge(fdf, on="token_address", how="left")


def _connect(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(
            f"tracker database not found: {db_path}. Run the stream first to generate snapshots."
        )
    return sqlite3.connect(db_path.as_posix())


def _get_threshold_and_milestone(con: sqlite3.Connection) -> Tuple[str, float]:
    """Return (milestone_name, threshold_usd).

    Priority:
      1) config/control.yaml -> milestones.market_cap_hit_usd (and VALID milestone)
      2) fallback to what's present in DB (legacy mcap_gte_*)
    """
    control = load_control()
    thr = deep_get(control, "milestones.market_cap_hit_usd", None)
    if thr is not None:
        try:
            thr_f = float(thr)
        except Exception:
            thr_f = 50_000.0
        return f"mcap_hit_valid_{int(thr_f)}", float(thr_f)

    rows = con.execute("SELECT DISTINCT milestone FROM milestones").fetchall()
    if not rows:
        return "mcap_hit_valid_50000", 50_000.0

    # prefer the largest threshold we've tracked (VALID first)
    parsed_valid: List[Tuple[float, str]] = []
    parsed_legacy: List[Tuple[float, str]] = []
    for (milestone,) in rows:
        m = str(milestone)
        if m.startswith("mcap_hit_valid_"):
            try:
                t = float(m.split("mcap_hit_valid_")[1])
                parsed_valid.append((t, m))
            except Exception:
                continue
        if m.startswith("mcap_gte_"):
            try:
                t = float(m.split("mcap_gte_")[1])
                parsed_legacy.append((t, m))
            except Exception:
                continue

    if parsed_valid:
        t, name = sorted(parsed_valid, key=lambda x: x[0])[-1]
        return name, float(t)

    if parsed_legacy:
        t, name = sorted(parsed_legacy, key=lambda x: x[0])[-1]
        return name, float(t)

    return str(rows[0][0]), 50_000.0


def _load_token_table(con: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM tokens", con)


def _load_hits(con: sqlite3.Connection, milestone_name: str) -> pd.DataFrame:
    q = """
    SELECT m.token_address, m.reached_at_utc, m.value AS market_cap_at_hit,
           t.symbol, t.name, t.chain, t.first_seen_utc,
           t.liquidity_usd, t.price_usd, t.volume_h24_usd, t.price_change_h24,
           t.market_cap_usd AS market_cap_first, t.fdv_usd, t.trade_h24, t.unique_wallet_h24,
           t.holders, t.age_minutes
    FROM milestones m
    LEFT JOIN tokens t ON t.token_address = m.token_address
    WHERE m.milestone = ?
    ORDER BY m.reached_at_utc DESC
    """
    return pd.read_sql_query(q, con, params=(milestone_name,))


def _summary(df: pd.DataFrame, group_name: str, metrics: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows = []
    for m in metrics:
        if m not in df.columns:
            continue
        s = pd.to_numeric(df[m], errors="coerce")
        rows.append(
            {
                "group": group_name,
                "metric": m,
                "count": int(s.notna().sum()),
                "mean": float(s.mean()) if s.notna().any() else float("nan"),
                "median": float(s.median()) if s.notna().any() else float("nan"),
                "p25": float(s.quantile(0.25)) if s.notna().any() else float("nan"),
                "p75": float(s.quantile(0.75)) if s.notna().any() else float("nan"),
                "p95": float(s.quantile(0.95)) if s.notna().any() else float("nan"),
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["metric", "group"]).reset_index(drop=True)
    return out


def _print_table(df: pd.DataFrame, title: str, width: int = 160) -> None:
    print("\n" + title)
    print("-" * min(len(title), width))
    if df.empty:
        print("(empty)")
        return
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", width,
        "display.colheader_justify", "left",
    ):
        print(df.to_string(index=False))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Find early-feature patterns for tokens that reached a market-cap milestone (default 50k)."
    )
    parser.add_argument("--db", default=str(DB_PATH), help="Path to tracker.db")
    parser.add_argument("--limit", type=int, default=50, help="How many HIT tokens to show")
    parser.add_argument("--width", type=int, default=160, help="Console table width")
    parser.add_argument("--save", action="store_true", help="Save CSV+MD reports to data/processed")
    args = parser.parse_args()

    db_path = Path(args.db)
    con = _connect(db_path)
    try:
        control = load_control()
        windows, rest_n = _pattern_settings(control)

        milestone_name, threshold = _get_threshold_and_milestone(con)
        tokens = _load_token_table(con)
        hits = _load_hits(con, milestone_name)

        # REST: tokens that have been seen but never hit the milestone
        if "token_address" in tokens.columns and "token_address" in hits.columns:
            rest = tokens[~tokens["token_address"].isin(hits["token_address"])].copy()
        else:
            rest = pd.DataFrame()

        # Optional: downsample REST for speed
        if rest_n and not rest.empty and len(rest) > rest_n:
            rest = rest.sample(n=rest_n, random_state=42)

        # Compute early-window activity features from snapshots
        # (works best when your stream collects multiple snapshots per token)
        hits = _attach_early_features(con, hits, windows=windows)
        rest = _attach_early_features(con, rest, windows=windows)

        window_metrics: List[str] = []
        for w in windows:
            window_metrics.extend(
                [
                    f"trades_{w}m",
                    f"wallets_{w}m",
                    f"volume_{w}m_usd",
                    f"liquidity_delta_{w}m_usd",
                    f"liquidity_max_{w}m_usd",
                ]
            )

        KEY_METRICS = BASE_METRICS + window_metrics

        title = f"PATTERNS: tokens that hit market cap >= {int(threshold):,} USD (milestone '{milestone_name}')"
        hit_cols = [
            "reached_at_utc",
            "symbol",
            "name",
            "token_address",
            "market_cap_at_hit",
            "liquidity_usd",
            "volume_h24_usd",
            "trade_h24",
            "unique_wallet_h24",
            "holders",
            "fdv_usd",
            "age_minutes",
            # Early-window activity (if available)
            "trades_1m",
            "wallets_1m",
            "volume_1m_usd",
            "trades_5m",
            "wallets_5m",
            "volume_5m_usd",
            "liquidity_delta_5m_usd",
            "liquidity_max_5m_usd",
            "trades_15m",
            "wallets_15m",
            "volume_15m_usd",
            "liquidity_delta_15m_usd",
            "liquidity_max_15m_usd",
        ]
        _print_table(
            hits[[c for c in hit_cols if c in hits.columns]].head(args.limit),
            title=f"HIT list (latest {min(args.limit, len(hits))}) | {title}",
            width=args.width,
        )

        s_hit = _summary(hits, group_name="HIT", metrics=KEY_METRICS)
        s_rest = _summary(rest, group_name="REST", metrics=KEY_METRICS)
        stats = pd.concat([s_hit, s_rest], ignore_index=True)

        _print_table(stats, title="Summary: early metrics HIT vs REST", width=args.width)

        # Simple “rule hints”: show metrics where HIT median > REST median by ratio
        hints = []
        for m in KEY_METRICS:
            try:
                mh = float(stats[(stats["group"] == "HIT") & (stats["metric"] == m)]["median"].iloc[0])
                mr = float(stats[(stats["group"] == "REST") & (stats["metric"] == m)]["median"].iloc[0])
                if mr and mr > 0:
                    ratio = mh / mr
                else:
                    ratio = float("nan")
                hints.append({"metric": m, "hit_median": mh, "rest_median": mr, "hit/rest": ratio})
            except Exception:
                continue

        hints_df = pd.DataFrame(hints)
        if not hints_df.empty:
            hints_df = hints_df.sort_values("hit/rest", ascending=False)
        _print_table(hints_df, title="Rule hints (median ratios)", width=args.width)

        if args.save:
            OUT_MD.parent.mkdir(parents=True, exist_ok=True)
            hits.to_csv(OUT_CSV_HITS, index=False)
            stats.to_csv(OUT_CSV_STATS, index=False)

            md: List[str] = []
            md.append(f"# Patterns report: HIT market cap >= {int(threshold):,} USD\n\n")
            md.append(f"- Milestone: `{milestone_name}`\n")
            md.append(f"- HIT tokens: **{len(hits)}**\n")
            md.append(f"- REST tokens: **{len(rest)}**\n\n")

            md.append("## HIT token list (latest)\n\n")
            md.append(hits.head(200).to_markdown(index=False) if not hits.empty else "_empty_")
            md.append("\n\n## Summary (early metrics)\n\n")
            md.append(stats.to_markdown(index=False) if not stats.empty else "_empty_")
            md.append("\n\n## Rule hints (median ratios)\n\n")
            md.append(hints_df.to_markdown(index=False) if not hints_df.empty else "_empty_")

            OUT_MD.write_text("".join(md), encoding="utf-8")

            print("\nSaved:")
            print(f"- {OUT_CSV_HITS}")
            print(f"- {OUT_CSV_STATS}")
            print(f"- {OUT_MD}")

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
