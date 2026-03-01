from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from pipeline.tracking.tracker import record_snapshot_and_maybe_milestone


# NOTE:
# A previous iteration of the project used a different CSV schema, which could
# silently corrupt the data when appending with mismatched headers.
#
# To prevent that, we *validate* header compatibility; if the existing file has a
# different header, we write to a new file name (decisions_v2.csv).

DEFAULT_PATH = Path("data/processed/decisions.csv")
# If the existing file has a different header, we write to a new versioned file.
FALLBACK_PATH = Path("data/processed/decisions_v4.csv")

# WATCH list: store the newest WATCH row per token
WATCHLIST_PATH = Path("data/processed/watchlist.csv")


FIELDS_V3: List[str] = [
    "timestamp_utc",
    "source",
    "chain",
    "token_address",
    "symbol",
    "name",
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
    "created_at",
    "age_minutes",
    "decision",
    "reason",
    "ml_score",
]

# Version 4 schema adds ml_score (optional).
FIELDS_V4: List[str] = list(FIELDS_V3)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_header(path: Path) -> List[str] | None:
    if not path.is_file():
        return None
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return None
            return [h.strip() for h in header]
    except Exception:
        return None


def _choose_output_path() -> Path:
    header = _read_header(DEFAULT_PATH)
    if header is None:
        return DEFAULT_PATH
    if header == FIELDS_V4:
        return DEFAULT_PATH
    # Header mismatch => write to versioned file to avoid corrupting older dataset
    return FALLBACK_PATH


def _append_watchlist_row(row: Dict[str, Any]) -> None:
    """Append WATCH token to watchlist.csv with dedupe by token_address."""
    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame([row])

    if not WATCHLIST_PATH.exists():
        new_df.to_csv(WATCHLIST_PATH, index=False)
        return

    old_df = pd.read_csv(WATCHLIST_PATH)
    df = pd.concat([new_df, old_df], ignore_index=True)

    if "timestamp_utc" in df.columns:
        df = df.sort_values("timestamp_utc", ascending=False)

    if "token_address" in df.columns:
        df = df.drop_duplicates(subset=["token_address"], keep="first")

    df.to_csv(WATCHLIST_PATH, index=False)


def save_decision(token: Dict[str, Any], decision: str, reason: str | None = None) -> Path:
    """Append a single decision row to decisions.csv/decisions_v2.csv.

    Side-effects:
      - updates tracker snapshots + milestone detection (e.g. hit 50k mcap)
      - appends WATCH rows to watchlist.csv (deduped)
    """
    out_path = _choose_output_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = out_path.is_file()

    row: Dict[str, Any] = {k: token.get(k) for k in FIELDS_V4}
    row["timestamp_utc"] = _now_utc_iso()
    row["decision"] = decision
    row["reason"] = reason

    with open(out_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS_V4)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    # Track snapshots + milestone detection (50k by default)
    try:
        record_snapshot_and_maybe_milestone(row)
    except Exception:
        # never crash live stream due to tracker
        pass

    # Keep a compact watchlist for quick review
    if str(decision).upper() == "WATCH":
        try:
            _append_watchlist_row(row)
        except Exception:
            pass

    return out_path
