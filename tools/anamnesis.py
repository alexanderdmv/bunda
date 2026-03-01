#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print a table of filtering metrics per mint from snapshots.jsonl.

Usage:
  cd D:\Aladdin\sniper_v10
  python tools/print_trade_metrics.py --log logs/pipeline.log
  python tools/print_trade_metrics.py --mints 7U7a...pump 12BP...pump
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from tabulate import tabulate
except Exception:
    tabulate = None


MINT_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44}(?:pump)?)\b")
BUY_RE = re.compile(r"\bBUY (?:built|sent)\b.*?\bmint=([1-9A-HJ-NP-Za-km-z]{32,44}(?:pump)?)\b")
DECISION_RE = re.compile(r"\bCA=([1-9A-HJ-NP-Za-km-z]{32,44}(?:pump)?)\b")


def find_first(root: Path, pattern: str) -> Optional[Path]:
    for p in root.rglob("*"):
        if p.is_file() and re.search(pattern, p.name, re.IGNORECASE):
            return p
    return None


def load_jsonl_last_by_mint(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Returns last snapshot dict per mint.
    Accepts many possible snapshot schemas; we just keep the most recent occurrence.
    """
    last: Dict[str, Dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line[0] != "{":
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            mint = (
                obj.get("mint")
                or obj.get("token_address")
                or obj.get("ca")
                or obj.get("contract_address")
                or obj.get("address")
            )
            if not isinstance(mint, str):
                # Try nested schemas (dexscreener/raw)
                mint = (
                    obj.get("address")
                    or (obj.get("_raw") or {}).get("baseToken", {}).get("address")
                    or (obj.get("dex") or {}).get("baseToken", {}).get("address")
                    or (obj.get("dexscreener") or {}).get("baseToken", {}).get("address")
                )
            if not isinstance(mint, str):
                continue

            last[mint] = obj
    return last


def extract_mints_from_log(log_path: Path) -> List[str]:
    """
    Prefer mints that were actually bought; fallback to all CA=... occurrences.
    """
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    bought = BUY_RE.findall(text)
    if bought:
        # keep order, unique
        seen = set()
        out = []
        for m in bought:
            if m not in seen:
                seen.add(m)
                out.append(m)
        return out

    cas = DECISION_RE.findall(text)
    seen = set()
    out = []
    for m in cas:
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


def g(d: Dict[str, Any], *keys: str, default=None):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return x * 100.0


def normalize_snapshot(s: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map snapshot schema variants to a flat dict of metrics we care about.
    These correspond to runner_gate and related filters.
    """
    # Common nesting patterns we’ve seen:
    ds = g(s, "dex") or g(s, "dexscreener") or s

    # activity (m5)
    trade_m5 = to_float(g(ds, "trade_m5") or g(ds, "txns", "m5", "total") or g(ds, "txns", "m5"))
    buy_m5 = to_float(g(ds, "buy_m5") or g(ds, "txns", "m5", "buys"))
    sell_m5 = to_float(g(ds, "sell_m5") or g(ds, "txns", "m5", "sells"))

    vol_m5 = to_float(g(ds, "volume_m5_usd") or g(ds, "volume", "m5"))
    vol_h1 = to_float(g(ds, "volume_h1_usd") or g(ds, "volume", "h1"))
    vol_h6 = to_float(g(ds, "volume_h6_usd") or g(ds, "volume", "h6"))
    vol_h24 = to_float(g(ds, "volume_h24_usd") or g(ds, "volume", "h24"))

    # price change
    pc_m5 = to_float(g(ds, "price_change_m5") or g(ds, "priceChange", "m5"))
    pc_h1 = to_float(g(ds, "price_change_h1") or g(ds, "priceChange", "h1"))
    pc_h6 = to_float(g(ds, "price_change_h6") or g(ds, "priceChange", "h6"))
    pc_h24 = to_float(g(ds, "price_change_h24") or g(ds, "priceChange", "h24"))

    # liquidity & fdv
    liq_usd = to_float(g(ds, "liquidity_usd") or g(ds, "liquidity", "usd"))
    fdv = to_float(g(ds, "fdv") or g(ds, "fullyDilutedValuation"))
    mcap = to_float(g(ds, "market_cap") or g(ds, "marketCap"))

    # age
    age_min = to_float(g(ds, "age_min") or g(ds, "age_minutes") or g(ds, "age_mins"))
    # Some snapshots store pairCreatedAt (ms)
    pca = g(ds, "pairCreatedAt")
    if age_min is None and isinstance(pca, (int, float)):
        # cannot compute without wall-clock here; leave None
        pass

    # pump bonding curve (from executor enrichment)
    curve_sol = to_float(g(s, "pump", "curve_sol") or g(s, "pump_curve", "curve_sol") or g(s, "curve_sol"))
    curve_real_sol = to_float(g(s, "pump", "real_sol") or g(s, "pump_curve", "real_sol") or g(s, "real_sol"))
    curve_supply = to_float(g(s, "pump", "mint_supply") or g(s, "pump_curve", "mint_supply") or g(s, "mint_supply"))

    # derived
    sell_buy_ratio = None
    if buy_m5 and buy_m5 > 0 and sell_m5 is not None:
        sell_buy_ratio = float(sell_m5) / float(buy_m5)
    abs_pc_m5 = abs(pc_m5) if pc_m5 is not None else None

    return {
        "mint": s.get("mint") or s.get("ca") or s.get("contract_address") or s.get("address"),
        "age_min": age_min,
        "trade_m5": trade_m5,
        "buy_m5": buy_m5,
        "sell_m5": sell_m5,
        "sell/buy_m5": sell_buy_ratio,
        "vol_m5_usd": vol_m5,
        "vol_h1_usd": vol_h1,
        "liq_usd": liq_usd,
        "fdv": fdv,
        "mcap": mcap,
        "pc_m5": pc_m5,
        "abs_pc_m5": abs_pc_m5,
        "pc_h1": pc_h1,
        "pc_h6": pc_h6,
        "pc_h24": pc_h24,
        "curve_sol": curve_sol,
        "curve_real_sol": curve_real_sol,
        "curve_supply": curve_supply,
        # helpful debug flags if present
        "decision": s.get("decision") or g(s, "filter", "decision"),
        "reason": s.get("reason") or g(s, "filter", "reason"),
        "ml_score": s.get("ml_score") or g(s, "ml", "score"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Project root (default: .)")
    ap.add_argument("--log", default="logs/pipeline.log", help="Path to pipeline log (default: logs/pipeline.log)")
    ap.add_argument("--discovery", default="", help="Path to discovery_seen.jsonl (auto-detected if empty)")
    ap.add_argument("--snapshots", default="data/processed/snapshots.jsonl", help="Path to snapshots.jsonl (auto-detected if empty)")
    # NOTE: default must be None/[] (NOT a string), otherwise we'd iterate characters as mints.
    ap.add_argument("--mints", nargs="*", default=None, help="Specific mints to print (optional)")
    ap.add_argument("--limit", type=int, default=50, help="Max rows to print")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    log_path = (root / args.log).resolve()
    if not log_path.exists():
        raise SystemExit(f"Log not found: {log_path}")

    snapshots_path = Path(args.snapshots).resolve() if args.snapshots else None
    if snapshots_path is None or not snapshots_path.exists():
        snapshots_path = find_first(root, r"snapshots\.jsonl$")
    if snapshots_path is None or not snapshots_path.exists():
        raise SystemExit("Could not find snapshots.jsonl under project root. Use --snapshots to specify it.")

    last = load_jsonl_last_by_mint(snapshots_path)

    if args.mints:
        mints = args.mints
    else:
        mints = extract_mints_from_log(log_path)

    rows: List[Dict[str, Any]] = []
    missing: List[str] = []

    for m in mints:
        s = last.get(m)
        if not s:
            missing.append(m)
            continue
        rows.append(normalize_snapshot(s))

    # sort: show most "actionable" first (those with curve_sol + activity)
    def sort_key(r):
        return (
            0 if r.get("curve_sol") is not None else 1,
            -(r.get("trade_m5") or 0),
            -(r.get("vol_m5_usd") or 0),
        )

    rows.sort(key=sort_key)
    rows = rows[: args.limit]

    headers = [
        "disc_stage", "disc_reason", "disc_ds_status", "disc_sig", "disc_ts_unix",
        "mint",
        "age_min",
        "trade_m5",
        "buy_m5",
        "sell_m5",
        "sell/buy_m5",
        "vol_m5_usd",
        "liq_usd",
        "pc_m5",
        "curve_sol",
        "curve_real_sol",
        "ml_score",
        "decision",
        "reason",
    ]

    table = [[r.get(h) for h in headers] for r in rows]

    if tabulate:
        print(tabulate(table, headers=headers, tablefmt="github"))
    else:
        # fallback simple print
        print(headers)
        for row in table:
            print(row)

    if missing:
        print("\nMissing mints in snapshots (no snapshot found):")
        for m in missing[:50]:
            print(" -", m)
        if len(missing) > 50:
            print(f" ... and {len(missing)-50} more")


if __name__ == "__main__":
    main()