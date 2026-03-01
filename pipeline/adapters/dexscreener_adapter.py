# pipeline/adapters/dexscreener_adapter.py
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import requests

from pipeline.state.dex_state import DexStateStore
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEX_BASE = "https://api.dexscreener.com"

# Reuse a single session (faster + fewer TLS handshakes)
_session = requests.Session()
_retry = Retry(
    total=2,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retry))
_session.headers.update({"accept": "application/json"})


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _get(url: str, timeout: float = 15.0) -> Any:
    r = _session.get(url, timeout=timeout)
    if r.status_code == 404:
        return None
    # Let callers detect rate-limit separately
    if r.status_code == 429:
        raise requests.HTTPError("rate_limited", response=r)
    r.raise_for_status()
    return r.json()


def _load_seen(path: str) -> set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
    except FileNotFoundError:
        return set()
    except Exception:
        return set()
    return set()


def _save_seen(path: str, seen: set[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted(seen), f, ensure_ascii=False)
    os.replace(tmp, path)


def _pick_main_pair(pairs: List[dict]) -> Optional[dict]:
    """Pick the most reliable pair.

    Prefer pairs with a known, positive liquidity.usd. If none have liquidity,
    fall back to the best volume.h24 pair.
    """

    pairs = list(pairs or [])

    def liq_usd(p: dict) -> float:
        return float((p.get("liquidity") or {}).get("usd") or 0.0)

    def vol24_usd(p: dict) -> float:
        return float((p.get("volume") or {}).get("h24") or 0.0)

    with_liq = [p for p in pairs if liq_usd(p) > 0]
    if with_liq:
        return max(with_liq, key=lambda p: (liq_usd(p), vol24_usd(p)))
    if pairs:
        return max(pairs, key=lambda p: vol24_usd(p))
    return None


def _normalize_pair(mint: str, pair: dict) -> Dict[str, Any]:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}

    # Determine which side corresponds to requested mint
    if str(base.get("address")) == str(mint):
        token_meta = base
    elif str(quote.get("address")) == str(mint):
        token_meta = quote
    else:
        token_meta = base

    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    # Additional windows (m5/h1/h6) for early-runner filtering
    m5 = txns.get("m5") or {}
    h1 = txns.get("h1") or {}
    h6 = txns.get("h6") or {}

    vol = pair.get("volume") or {}
    pc = pair.get("priceChange") or {}

    def _w_int(d, k):
        try:
            return int(d.get(k) or 0)
        except Exception:
            return 0

    buy_m5 = _w_int(m5, "buys"); sell_m5 = _w_int(m5, "sells")
    buy_h1 = _w_int(h1, "buys"); sell_h1 = _w_int(h1, "sells")
    buy_h6 = _w_int(h6, "buys"); sell_h6 = _w_int(h6, "sells")

    trade_m5 = buy_m5 + sell_m5
    trade_h1 = buy_h1 + sell_h1
    trade_h6 = buy_h6 + sell_h6

    volume_m5_usd = _to_float(vol.get("m5"))
    volume_h1_usd = _to_float(vol.get("h1"))
    volume_h6_usd = _to_float(vol.get("h6"))

    price_change_m5 = _to_float(pc.get("m5"))
    price_change_h1 = _to_float(pc.get("h1"))
    price_change_h6 = _to_float(pc.get("h6"))

    # Social / website hints if present in DS payload
    info = pair.get("info") or {}
    websites = info.get("websites") or []
    socials = info.get("socials") or []

    def _extract_urls(items):
        out = []
        for it in items or []:
            if isinstance(it, str) and it.strip():
                out.append(it.strip())
            elif isinstance(it, dict):
                u = it.get("url") or it.get("link")
                if isinstance(u, str) and u.strip():
                    out.append(u.strip())
        # unique, keep order
        seen = set()
        uniq = []
        for u in out:
            if u in seen:
                continue
            seen.add(u); uniq.append(u)
        return uniq

    website_urls = _extract_urls(websites)

    def _pick_social(kind: str) -> str | None:
        for it in socials or []:
            if isinstance(it, dict):
                t = str(it.get("type") or it.get("name") or "").lower()
                u = it.get("url") or it.get("link")
                if not isinstance(u, str) or not u.strip():
                    continue
                u = u.strip()
                if t == kind or (kind in t):
                    return u
        return None

    twitter_url = _pick_social("twitter") or _pick_social("x")
    telegram_url = _pick_social("telegram")
    discord_url = _pick_social("discord")

    # DexScreener's pairCreatedAt is typically a unix timestamp (sometimes ms).
    created_raw = pair.get("pairCreatedAt")
    created_unix: Optional[int] = None
    created_iso: Optional[str] = None
    age_minutes: Optional[float] = None
    try:
        if created_raw is not None:
            ts = int(float(created_raw))
            # Heuristic: treat very large values as milliseconds
            if ts > 1_000_000_000_000:
                ts = ts // 1000
            created_unix = ts
            created_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")
            age_minutes = max(0.0, (time.time() - float(ts)) / 60.0)
    except Exception:
        created_unix = None
        created_iso = None
        age_minutes = None

    return {
        "source": "dexscreener",
        "token_address": mint,
        "symbol": token_meta.get("symbol"),
        "name": token_meta.get("name"),

        "pair_address": pair.get("pairAddress"),
        "dex_id": pair.get("dexId"),
        "pair_created_at": created_raw,
        "dexscreener_url": pair.get("url"),

        "price_usd": _to_float(pair.get("priceUsd")),
        # DexScreener sometimes returns null liquidity even when other fields are present.
        # Normalize missing liquidity to 0.0 so rules behave deterministically.
        "liquidity_usd": _to_float((pair.get("liquidity") or {}).get("usd")) or 0.0,
        "volume_h24_usd": _to_float((pair.get("volume") or {}).get("h24")),
        "price_change_h24": _to_float((pair.get("priceChange") or {}).get("h24")),

        # Use txns as trade proxy (DexScreener doesn't provide unique wallets)
        "trade_h24": int((h24.get("buys") or 0) + (h24.get("sells") or 0)),
        "buy_h24": int(h24.get("buys") or 0),
        "sell_h24": int(h24.get("sells") or 0),

        # Early windows
        "trade_m5": trade_m5,
        "buy_m5": buy_m5,
        "sell_m5": sell_m5,
        "volume_m5_usd": volume_m5_usd,
        "price_change_m5": price_change_m5,

        "trade_h1": trade_h1,
        "buy_h1": buy_h1,
        "sell_h1": sell_h1,
        "volume_h1_usd": volume_h1_usd,
        "price_change_h1": price_change_h1,

        "trade_h6": trade_h6,
        "buy_h6": buy_h6,
        "sell_h6": sell_h6,
        "volume_h6_usd": volume_h6_usd,
        "price_change_h6": price_change_h6,

        # Social / website hints (best-effort)
        "website_urls": website_urls,
        "twitter_url": twitter_url,
        "telegram_url": telegram_url,
        "discord_url": discord_url,
        "has_website": bool(website_urls),
        "has_twitter": bool(twitter_url),
        "has_telegram": bool(telegram_url),
        "has_discord": bool(discord_url),

        # Not available from DexScreener directly
        "unique_wallet_h24": None,
        "holders": None,
        "last_trade_unix": None,
        "last_trade_minutes": None,
        "market_cap_usd": _to_float(pair.get("marketCap")),
        "fdv_usd": _to_float(pair.get("fdv")),

        # Tracker/history fields
        "created_at": created_iso,
        "age_minutes": age_minutes,

        "_raw": pair,
    }


def stream_tokens_dexscreener(
    poll_seconds: int = 20,
    profiles_limit: int = 500,
    chain: str = "solana",
    timeout_seconds: float = 15.0,
    seen_path: str = "data/processed/seen_dexscreener.json",
    max_new_tokens_per_poll: int = 60,
    per_pair_sleep_seconds: float = 0.25,
    limit: Optional[int] = None,
    debug: bool = False,
    state_store: Optional[DexStateStore] = None,
    max_due_pending_per_poll: int = 50,
) -> Iterator[Dict[str, Any]]:
    """Discovery-first stream via DexScreener with a *PENDING_RECHECK* mechanism.

    Why PENDING_RECHECK:
      - Many early runners fail initial thresholds (liq/vol/trades) for a couple minutes.
      - If we mark them as permanently 'seen' after first fetch, we will miss them.

    State:
      - `seen_path` stores both permanent seen + a pending queue (backward compatible).
      - Pending mints are re-hydrated on a timer until they either WATCH or expire.

    Endpoints and rate limits (DexScreener):
      - GET /token-profiles/latest/v1  (60 req/min)
      - GET /token-pairs/v1/{chainId}/{tokenAddress} (300 req/min)

    Strategy:
      1) Pull token profiles feed
      2) Add due pending mints (recheck queue)
      3) Hydrate each mint with token-pairs and yield a normalized token snapshot
      4) main.py updates the state store after a decision:
           WATCH => permanent seen
           SKIP temporary => pending
           SKIP permanent => permanent seen
    """

    yielded = 0

    if state_store is None:
        state_store = DexStateStore(seen_path)

    while True:
        # A local guard to avoid processing the same mint twice in one poll.
        processed_this_poll: set[str] = set()
        profiles_count = 0
        new_count = 0
        try:
            now = int(time.time())
            due_pending = state_store.due_pending_mints(now_unix=now, max_n=max_due_pending_per_poll)

            profiles = _get(f"{DEX_BASE}/token-profiles/latest/v1", timeout=timeout_seconds)
            if not isinstance(profiles, list):
                profiles = []

            profiles_count = len(profiles)

            if profiles_limit and profiles_limit > 0:
                profiles = profiles[: int(profiles_limit)]

            new_mints: List[str] = []
            for p in profiles:
                if not isinstance(p, dict):
                    continue
                if str(p.get("chainId")) != str(chain):
                    continue
                mint = p.get("tokenAddress") or p.get("token_address")
                if not mint:
                    continue
                mint = str(mint)

                # Skip anything already known (permanent seen OR pending).
                # Pending mints are re-checked only via the due_pending queue to avoid "new" spam.
                if state_store.is_known(mint, now_unix=now):
                    continue

                new_mints.append(mint)

            # Build processing list: due pending first, then new
            mints: List[str] = []
            for m in due_pending + new_mints:
                if m in processed_this_poll:
                    continue
                processed_this_poll.add(m)
                mints.append(m)

            if max_new_tokens_per_poll and max_new_tokens_per_poll > 0:
                # cap across both due+new to protect pair endpoint
                mints = mints[: int(max_new_tokens_per_poll)]

            new_count = len(mints)

            if debug:
                print(f"[dexscreener] profiles={profiles_count} new_profiles={len(new_mints)} due_pending={len(due_pending)} total={new_count} seen={len(state_store.seen)} pending={len(state_store.pending)}")

            for mint in mints:
                try:
                    pairs = _get(f"{DEX_BASE}/token-pairs/v1/{chain}/{mint}", timeout=timeout_seconds)
                except requests.HTTPError as e:
                    status = getattr(getattr(e, "response", None), "status_code", None)
                    if status == 429:
                        if debug:
                            print("[dexscreener] hit 429 rate limit; pausing")
                        state_store.save()
                        time.sleep(max(5, int(poll_seconds)))
                        break
                    if debug:
                        print(f"[dexscreener] http error for {mint}: {e!r}")
                    # do NOT mark as seen here; main will handle state based on decision
                    token = {"source": "dexscreener", "token_address": mint, "liquidity_usd": 0.0, "_ds_status": "http_error"}
                    yield token
                    yielded += 1
                    continue
                except requests.RequestException as e:
                    if debug:
                        print(f"[dexscreener] request error for {mint}: {e!r}")
                    token = {"source": "dexscreener", "token_address": mint, "liquidity_usd": 0.0, "_ds_status": "request_error"}
                    yield token
                    yielded += 1
                    continue

                if not isinstance(pairs, list) or not pairs:
                    # Pair isn't ready yet (common in the first ~1-3 minutes)
                    token = {"source": "dexscreener", "token_address": mint, "liquidity_usd": 0.0, "_ds_status": "no_pairs"}
                    yield token
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        state_store.save()
                        return
                    if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                        time.sleep(float(per_pair_sleep_seconds))
                    continue

                main_pair = _pick_main_pair(pairs)
                if not isinstance(main_pair, dict):
                    token = {"source": "dexscreener", "token_address": mint, "liquidity_usd": 0.0, "_ds_status": "no_pairs"}
                    yield token
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        state_store.save()
                        return
                    if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                        time.sleep(float(per_pair_sleep_seconds))
                    continue

                token = _normalize_pair(mint, main_pair)
                token["_ds_status"] = "ok"

                yield token
                yielded += 1

                if limit is not None and yielded >= limit:
                    state_store.save()
                    return

                if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                    time.sleep(float(per_pair_sleep_seconds))

            state_store.save()

        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if debug:
                print(f"[dexscreener] http error status={status}: {e!r}")
        except requests.RequestException as e:
            if debug:
                print(f"[dexscreener] request error: {e!r}")
        except Exception as e:
            if debug:
                print(f"[dexscreener] error: {e!r}")

        time.sleep(int(poll_seconds))
