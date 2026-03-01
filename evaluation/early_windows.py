from __future__ import annotations

import sqlite3
from datetime import timedelta
from typing import Any, Dict, Iterable, Optional

import pandas as pd


def _to_dt(x: Any) -> Optional[pd.Timestamp]:
    """Parse ISO timestamp (best effort) to pandas Timestamp (UTC-aware if possible)."""
    if x is None:
        return None
    try:
        # pandas handles offsets like "+00:00" correctly
        ts = pd.to_datetime(str(x), utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _to_num(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def load_snapshots_for_window(
    con: sqlite3.Connection,
    token_address: str,
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
) -> pd.DataFrame:
    """Load snapshots for token between [start_utc, end_utc]."""
    q = """
    SELECT
      timestamp_utc,
      liquidity_usd,
      volume_h24_usd,
      trade_h24,
      unique_wallet_h24,
      holders,
      market_cap_usd,
      fdv_usd,
      price_usd,
      price_change_h24,
      last_trade_minutes,
      age_minutes
    FROM snapshots
    WHERE token_address = ?
      AND timestamp_utc >= ?
      AND timestamp_utc <= ?
    ORDER BY timestamp_utc ASC
    """

    df = pd.read_sql_query(
        q,
        con,
        params=(
            token_address,
            start_utc.isoformat(),
            end_utc.isoformat(),
        ),
    )

    if df.empty:
        return df

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for c in [
        "liquidity_usd",
        "volume_h24_usd",
        "trade_h24",
        "unique_wallet_h24",
        "holders",
        "market_cap_usd",
        "fdv_usd",
        "price_usd",
        "price_change_h24",
        "last_trade_minutes",
        "age_minutes",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def compute_early_window_features(
    con: sqlite3.Connection,
    token_address: str,
    first_seen_utc: str,
    windows_minutes: Iterable[int] = (1, 5, 15),
) -> Dict[str, float]:
    """Compute early-window deltas from snapshots.

    IMPORTANT: metrics we store per snapshot are typically "rolling 24h" values.
    For very new tokens, these behave like cumulative counts/volume, so deltas
    over the first few minutes are a useful proxy for "activity in first X minutes".

    Returned keys (per window w):
      trades_{w}m, wallets_{w}m, volume_{w}m_usd,
      liquidity_delta_{w}m_usd, liquidity_max_{w}m_usd
    """
    start = _to_dt(first_seen_utc)
    if start is None:
        return {}

    out: Dict[str, float] = {}
    max_w = max(int(w) for w in windows_minutes)
    end_all = start + timedelta(minutes=max_w)

    # Pull everything up to max window once
    df_all = load_snapshots_for_window(con, token_address, start, end_all)
    if df_all.empty:
        return {}

    # Ensure sorted
    df_all = df_all.sort_values("timestamp_utc", ascending=True)

    # Helper: delta(last-first) within window
    def delta(col: str, dfw: pd.DataFrame) -> float:
        if col not in dfw.columns or dfw.empty:
            return float("nan")
        s = dfw[col].dropna()
        if s.empty:
            return float("nan")
        return float(s.iloc[-1] - s.iloc[0])

    for w in windows_minutes:
        w = int(w)
        end = start + timedelta(minutes=w)
        dfw = df_all[df_all["timestamp_utc"] <= end].copy()

        out[f"trades_{w}m"] = delta("trade_h24", dfw)
        out[f"wallets_{w}m"] = delta("unique_wallet_h24", dfw)
        out[f"volume_{w}m_usd"] = delta("volume_h24_usd", dfw)

        # Liquidity: delta and max
        if "liquidity_usd" in dfw.columns and not dfw.empty:
            liq = dfw["liquidity_usd"].dropna()
            if liq.empty:
                out[f"liquidity_delta_{w}m_usd"] = float("nan")
                out[f"liquidity_max_{w}m_usd"] = float("nan")
            else:
                out[f"liquidity_delta_{w}m_usd"] = float(liq.iloc[-1] - liq.iloc[0])
                out[f"liquidity_max_{w}m_usd"] = float(liq.max())
        else:
            out[f"liquidity_delta_{w}m_usd"] = float("nan")
            out[f"liquidity_max_{w}m_usd"] = float("nan")

    return out
