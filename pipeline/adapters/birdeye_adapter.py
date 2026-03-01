# pipeline/adapters/birdeye_adapter.py
from __future__ import annotations

import time
from typing import Any, Dict, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BIRDEYE_BASE = "https://public-api.birdeye.so"

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


def _pick_first(d: dict[str, Any], keys: list[str]) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _birdeye_get(path: str, api_key: str, params: dict[str, Any], timeout: float) -> dict[str, Any]:
    url = f"{BIRDEYE_BASE}{path}"
    headers = {"X-API-KEY": api_key}
    r = _session.get(url, headers=headers, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _normalize_token_row(row: dict[str, Any]) -> Dict[str, Any]:
    address = row.get("address") or row.get("tokenAddress") or row.get("mint")
    symbol = row.get("symbol")
    name = row.get("name")

    liquidity_usd = _pick_first(row, ["liquidity", "liquidityUsd", "liquidity_usd"])

    return {
        "source": "birdeye",
        "token_address": address,
        "symbol": symbol,
        "name": name,
        "liquidity_usd": liquidity_usd,

        # "created" is rarely present in list endpoints; we compute age from overview if possible
        "created_at": _pick_first(row, ["createdAt", "created_at", "createdTime", "timeCreated"]),
        "age_minutes": None,

        # enriched metrics (overview)
        "price_usd": None,
        "volume_h24_usd": None,
        "price_change_h24": None,
        "market_cap_usd": None,
        "fdv_usd": None,

        # activity (overview)
        "trade_h24": None,
        "buy_h24": None,
        "sell_h24": None,
        "unique_wallet_h24": None,
        "holders": None,
        "last_trade_unix": None,
        "last_trade_minutes": None,

        # placeholders for later (holders distribution/dev share)
        "dev_wallet_share": None,
        "top10_holders": None,

        "_raw": row,
    }


def _enrich_with_overview(
    token: Dict[str, Any],
    api_key: str,
    chain: str,
    timeout: float,
) -> None:
    """
    Enrich token with Birdeye /defi/token_overview.
    Best-effort: never raise exceptions.
    """
    address = token.get("token_address")
    if not address:
        return

    try:
        data = _birdeye_get(
            "/defi/token_overview",
            api_key=api_key,
            params={"address": address, "chain": chain},
            timeout=timeout,
        )
    except requests.RequestException:
        return

    ov = data.get("data") if isinstance(data, dict) else None
    if not isinstance(ov, dict):
        return

    # Price
    token["price_usd"] = _to_float(_pick_first(ov, ["price", "priceUsd", "price_usd"])) or token.get("price_usd")

    # Liquidity (overview often has better value than list)
    token["liquidity_usd"] = _to_float(_pick_first(ov, ["liquidity", "liquidityUsd", "liquidity_usd"])) or token.get("liquidity_usd")

    # 24h volume — confirmed by your DEBUG OVERVIEW KEYS: v24hUSD exists
    token["volume_h24_usd"] = _to_float(_pick_first(ov, ["v24hUSD", "v24hUsd", "v24h_usd", "volume24hUSD", "volume24hUsd", "volume_24h_usd"]))

    # 24h price change — confirmed: priceChange24hPercent exists
    token["price_change_h24"] = _to_float(_pick_first(ov, ["priceChange24hPercent", "priceChange24h"]))

    # Caps — confirmed: marketCap, fdv exist
    token["market_cap_usd"] = _to_float(_pick_first(ov, ["marketCap", "marketCapUSD", "marketCapUsd", "market_cap_usd"]))
    token["fdv_usd"] = _to_float(_pick_first(ov, ["fdv", "fdvUSD", "fdvUsd", "fdv_usd"]))

    # Activity — confirmed: trade24h, buy24h, sell24h, uniqueWallet24h exist
    token["trade_h24"] = _to_float(_pick_first(ov, ["trade24h"]))
    token["buy_h24"] = _to_float(_pick_first(ov, ["buy24h"]))
    token["sell_h24"] = _to_float(_pick_first(ov, ["sell24h"]))
    token["unique_wallet_h24"] = _to_float(_pick_first(ov, ["uniqueWallet24h"]))

    # Holders count — confirmed: holder exists
    token["holders"] = _to_float(_pick_first(ov, ["holder"]))

    # "Age": Birdeye overview doesn't expose pair creation in your keys, but does expose lastTradeUnixTime.
    # We'll compute last_trade_minutes and, if created_at is missing, set created_at to lastTradeUnixTime as a proxy.
    last_trade = _pick_first(ov, ["lastTradeUnixTime"])
    if isinstance(last_trade, (int, float)) and last_trade > 0:
        token["last_trade_unix"] = int(last_trade)
        token["last_trade_minutes"] = max(0.0, (time.time() - float(last_trade)) / 60.0)
        if token.get("created_at") is None:
            token["created_at"] = int(last_trade)
            token["age_minutes"] = token["last_trade_minutes"]

    # Keep raw overview for debugging / future mapping
    token["_overview_raw"] = ov


def stream_tokens_birdeye(
    api_key: str,
    chain: str = "solana",
    poll_seconds: int = 3,
    limit: Optional[int] = None,
    debug: bool = False,
    # Enrichment controls (avoid rate limits + avoid blocking)
    enrich_overview: bool = True,
    enrich_min_liquidity_usd: float = 5.0,
    enrich_sleep_seconds: float = 0.1,
    enrich_timeout_seconds: float = 8.0,
) -> Iterator[Dict[str, Any]]:
    """
    Stream recently listed tokens via Birdeye Token List (V3).
    Optionally enrich "non-trash" tokens with /defi/token_overview, best-effort.
    """
    path = "/defi/v3/token/list"
    yielded = 0
    last_seen = int(time.time()) - 60

    while True:
        try:
            params = {
                "chain": chain,
                "sort_by": "recent_listing_time",
                "sort_type": "asc",
                "limit": 50,
                "recent_listing_time": last_seen,
            }

            data = _birdeye_get(path, api_key=api_key, params=params, timeout=10.0)
            payload = data.get("data") if isinstance(data, dict) else None

            rows = []
            if isinstance(payload, dict):
                rows = payload.get("tokens") or payload.get("items") or payload.get("list") or []
            elif isinstance(payload, list):
                rows = payload

            if debug:
                print(f"[birdeye] fetched rows={len(rows)} last_seen={last_seen}")

            max_listing_time = last_seen

            for row in rows:
                if not isinstance(row, dict):
                    continue

                t = _pick_first(row, ["recent_listing_time", "recentListingTime", "listedAt"])
                if isinstance(t, (int, float)):
                    if int(t) > max_listing_time:
                        max_listing_time = int(t)

                token = _normalize_token_row(row)
                if not token.get("token_address"):
                    continue

                if enrich_overview:
                    try:
                        liq_f = _to_float(token.get("liquidity_usd")) or 0.0
                        if liq_f >= float(enrich_min_liquidity_usd):
                            _enrich_with_overview(
                                token,
                                api_key=api_key,
                                chain=chain,
                                timeout=enrich_timeout_seconds,
                            )
                            time.sleep(float(enrich_sleep_seconds))
                    except Exception:
                        pass  # never fail the stream due to enrichment/parsing

                yield token
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

            last_seen = max_listing_time if max_listing_time > last_seen else last_seen

        except requests.RequestException as e:
            if debug:
                print(f"[birdeye] list error: {e!r}")
        except Exception as e:
            if debug:
                print(f"[birdeye] error: {e!r}")

        time.sleep(poll_seconds)
