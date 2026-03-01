from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from pipeline.control import load_control, deep_get

# Default DB path (can be overridden by config/control.yaml: app.db_path)
DB_PATH = Path("data/processed/tracker.db")

# Defaults (can be overridden in config.yaml under `tracking:`)
DEFAULT_MCAP_MILESTONE_USD = 50_000.0
DEFAULT_CHAIN = "solana"


def _load_project_config() -> Dict[str, Any]:
    """Load config/config.yaml if present (safe: returns {} on any error)."""
    candidates = [Path("config/config.yaml"), Path("config.yaml")]
    for p in candidates:
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                return {}
    return {}


def _tracking_settings() -> Tuple[float, str]:
    """Return (mcap_milestone_usd, chain).

    Priority (highest to lowest):
      1) env TRACK_MCAP_MILESTONE_USD
      2) config/control.yaml -> milestones.market_cap_hit_usd
      3) config/config.yaml -> tracking.market_cap_milestone_usd
      4) defaults
    """
    env = os.getenv("TRACK_MCAP_MILESTONE_USD")
    if env:
        try:
            mcap = float(env)
        except Exception:
            mcap = DEFAULT_MCAP_MILESTONE_USD
    else:
        control = load_control()
        mcap = deep_get(control, "milestones.market_cap_hit_usd", None)
        if mcap is not None:
            try:
                mcap = float(mcap)
            except Exception:
                mcap = DEFAULT_MCAP_MILESTONE_USD
        else:
            cfg = _load_project_config()
            tcfg = cfg.get("tracking", {}) or {}
            mcap = float(tcfg.get("market_cap_milestone_usd", DEFAULT_MCAP_MILESTONE_USD) or DEFAULT_MCAP_MILESTONE_USD)

    # chain from control.yaml if present
    control = load_control()
    chain = deep_get(control, "app.chain", None)
    if chain:
        chain = str(chain).strip()
    else:
        cfg = _load_project_config()
        chain = str((cfg.get("birdeye", {}) or {}).get("chain", DEFAULT_CHAIN) or DEFAULT_CHAIN)

    return float(mcap), chain


def _db_path() -> Path:
    """Tracker DB path from control panel if present."""
    control = load_control()
    p = deep_get(control, "app.db_path", None)
    if p:
        try:
            return Path(str(p))
        except Exception:
            return DB_PATH
    return DB_PATH


def _connect() -> sqlite3.Connection:
    dbp = _db_path()
    dbp.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(dbp.as_posix())
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def _is_valid_hit(row: Dict[str, Any], threshold_usd: float) -> bool:
    """Validate a HIT to avoid phantom market cap artifacts.

    A token is considered a VALID HIT if:
      market_cap_usd >= threshold_usd
      and (if milestones.validate_hit=true) it passes minimal real-activity thresholds.
    """
    control = load_control()
    ms = deep_get(control, "milestones", {}) or {}

    validate = ms.get("validate_hit", True)
    if not validate:
        return True

    liq = _to_float(row.get("liquidity_usd"))
    vol = _to_float(row.get("volume_h24_usd"))
    trd = _to_float(row.get("trade_h24"))
    wal = _to_float(row.get("unique_wallet_h24"))
    hld = _to_float(row.get("holders"))
    age = _to_float(row.get("age_minutes"))
    pc = _to_float(row.get("price_change_h24"))

    min_liq = float(ms.get("min_liquidity_usd", 0) or 0)
    min_vol = float(ms.get("min_volume_h24_usd", 0) or 0)
    min_trd = float(ms.get("min_trade_h24", 0) or 0)
    min_wal = float(ms.get("min_unique_wallet_h24", 0) or 0)
    min_hld = float(ms.get("min_holders", 0) or 0)

    max_age = float(ms.get("max_age_minutes", 0) or 0)  # 0 disables
    max_abs_pc = ms.get("max_abs_price_change_h24", None)  # None disables

    if liq is None or liq < min_liq:
        return False
    if vol is None or vol < min_vol:
        return False
    if trd is None or trd < min_trd:
        return False
    if wal is None or wal < min_wal:
        return False
    if hld is None or hld < min_hld:
        return False

    if max_age and (age is None or age > max_age):
        return False

    if max_abs_pc is not None:
        try:
            max_abs_pc_f = float(max_abs_pc)
            if pc is not None and abs(pc) > max_abs_pc_f:
                return False
        except Exception:
            # ignore if user put something invalid
            pass

    return True


def _init_db(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tokens (
            token_address TEXT PRIMARY KEY,
            first_seen_utc TEXT,
            source TEXT,
            chain TEXT,
            symbol TEXT,
            name TEXT,

            -- Early / first-snapshot features (best effort)
            liquidity_usd REAL,
            price_usd REAL,
            volume_h24_usd REAL,
            price_change_h24 REAL,
            market_cap_usd REAL,
            fdv_usd REAL,
            trade_h24 REAL,
            unique_wallet_h24 REAL,
            holders REAL,
            created_at TEXT,
            age_minutes REAL
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,

            liquidity_usd REAL,
            price_usd REAL,
            volume_h24_usd REAL,
            price_change_h24 REAL,
            market_cap_usd REAL,
            fdv_usd REAL,
            trade_h24 REAL,
            unique_wallet_h24 REAL,
            holders REAL,
            last_trade_minutes REAL,
            age_minutes REAL,

            decision TEXT,
            FOREIGN KEY(token_address) REFERENCES tokens(token_address)
        );
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_snapshots_token_time
        ON snapshots(token_address, timestamp_utc);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS milestones (
            token_address TEXT NOT NULL,
            milestone TEXT NOT NULL,
            reached_at_utc TEXT NOT NULL,
            value REAL,
            PRIMARY KEY(token_address, milestone)
        );
        """
    )

    con.commit()


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return None


def _upsert_token_first_seen(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    token_address = str(row.get("token_address") or "").strip()
    if not token_address:
        return

    ts = str(row.get("timestamp_utc") or "").strip()
    source = str(row.get("source") or "").strip() or None

    mcap_milestone, chain = _tracking_settings()
    chain = str(row.get("chain") or chain).strip() or chain

    symbol = str(row.get("symbol") or "").strip() or None
    name = str(row.get("name") or "").strip() or None

    # store early features only on first insert
    con.execute(
        """
        INSERT OR IGNORE INTO tokens(
            token_address, first_seen_utc, source, chain, symbol, name,
            liquidity_usd, price_usd, volume_h24_usd, price_change_h24,
            market_cap_usd, fdv_usd, trade_h24, unique_wallet_h24, holders,
            created_at, age_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token_address,
            ts,
            source,
            chain,
            symbol,
            name,
            _to_float(row.get("liquidity_usd")),
            _to_float(row.get("price_usd")),
            _to_float(row.get("volume_h24_usd")),
            _to_float(row.get("price_change_h24")),
            _to_float(row.get("market_cap_usd")),
            _to_float(row.get("fdv_usd")),
            _to_float(row.get("trade_h24")),
            _to_float(row.get("unique_wallet_h24")),
            _to_float(row.get("holders")),
            str(row.get("created_at") or "").strip() or None,
            _to_float(row.get("age_minutes")),
        ),
    )


def _insert_snapshot(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    token_address = str(row.get("token_address") or "").strip()
    if not token_address:
        return

    con.execute(
        """
        INSERT INTO snapshots(
            token_address, timestamp_utc,
            liquidity_usd, price_usd, volume_h24_usd, price_change_h24,
            market_cap_usd, fdv_usd, trade_h24, unique_wallet_h24, holders,
            last_trade_minutes, age_minutes, decision
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token_address,
            str(row.get("timestamp_utc") or "").strip(),
            _to_float(row.get("liquidity_usd")),
            _to_float(row.get("price_usd")),
            _to_float(row.get("volume_h24_usd")),
            _to_float(row.get("price_change_h24")),
            _to_float(row.get("market_cap_usd")),
            _to_float(row.get("fdv_usd")),
            _to_float(row.get("trade_h24")),
            _to_float(row.get("unique_wallet_h24")),
            _to_float(row.get("holders")),
            _to_float(row.get("last_trade_minutes")),
            _to_float(row.get("age_minutes")),
            str(row.get("decision") or "").strip() or None,
        ),
    )


def _maybe_record_milestone(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    token_address = str(row.get("token_address") or "").strip()
    if not token_address:
        return

    ts = str(row.get("timestamp_utc") or "").strip()
    mcap = _to_float(row.get("market_cap_usd"))

    threshold, _chain = _tracking_settings()

    if mcap is None:
        return

    if mcap >= threshold:
        # Always keep the legacy milestone for backwards compatibility
        con.execute(
            """
            INSERT OR IGNORE INTO milestones(token_address, milestone, reached_at_utc, value)
            VALUES (?, ?, ?, ?)
            """,
            (token_address, f"mcap_gte_{int(threshold)}", ts, float(mcap)),
        )

        # New: VALID/INVALID hit milestones
        is_valid = _is_valid_hit(row, threshold)
        if is_valid:
            milestone = f"mcap_hit_valid_{int(threshold)}"
            con.execute(
                """
                INSERT OR IGNORE INTO milestones(token_address, milestone, reached_at_utc, value)
                VALUES (?, ?, ?, ?)
                """,
                (token_address, milestone, ts, float(mcap)),
            )
        else:
            control = load_control()
            if bool(deep_get(control, "milestones.record_invalid_hits", True)):
                milestone = f"mcap_hit_invalid_{int(threshold)}"
                con.execute(
                    """
                    INSERT OR IGNORE INTO milestones(token_address, milestone, reached_at_utc, value)
                    VALUES (?, ?, ?, ?)
                    """,
                    (token_address, milestone, ts, float(mcap)),
                )


def record_snapshot_and_maybe_milestone(row: Dict[str, Any]) -> None:
    """Persist a snapshot + milestone detection.

    This function is designed to be safe to call on every processed token.
    Any exceptions should be handled by the caller.
    """
    con = _connect()
    try:
        _init_db(con)
        _upsert_token_first_seen(con, row)
        _insert_snapshot(con, row)
        _maybe_record_milestone(con, row)
        con.commit()
    finally:
        con.close()
