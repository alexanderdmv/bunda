from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class OrdersInfo:
    has_paid_orders: bool
    orders_count: int
    last_fetch_unix: int


class DexScreenerOrdersEnricher:
    """Optional enrichment: check paid orders for a token.

    Endpoint: GET https://api.dexscreener.com/orders/v1/{chainId}/{tokenAddress}
    Rate-limit: 60 req/min (DexScreener docs).

    We cache results to avoid re-hitting the endpoint for the same mint.
    """

    def __init__(self, timeout_seconds: float = 15, cache_ttl_seconds: int = 300):
        self.timeout_seconds = float(timeout_seconds)
        self.cache_ttl_seconds = int(cache_ttl_seconds)
        self._cache: Dict[Tuple[str, str], OrdersInfo] = {}

        self.session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)

    def enrich(self, chain_id: str, token_address: str) -> Dict[str, Any]:
        now = int(time.time())
        key = (str(chain_id), str(token_address))
        cached = self._cache.get(key)
        if cached and (now - cached.last_fetch_unix) < self.cache_ttl_seconds:
            return {
                "has_paid_orders": bool(cached.has_paid_orders),
                "paid_orders_count": int(cached.orders_count),
            }

        url = f"https://api.dexscreener.com/orders/v1/{chain_id}/{token_address}"
        r = self.session.get(url, timeout=self.timeout_seconds)
        r.raise_for_status()
        data = r.json()

        # Docs specify Response object[]; treat any non-empty array as “has paid orders”.
        orders_count = 0
        if isinstance(data, list):
            orders_count = len(data)
        elif isinstance(data, dict):
            # Defensive fallback (some APIs wrap lists)
            arr = data.get("orders")
            if isinstance(arr, list):
                orders_count = len(arr)

        info = OrdersInfo(
            has_paid_orders=orders_count > 0,
            orders_count=int(orders_count),
            last_fetch_unix=now,
        )
        self._cache[key] = info

        return {
            "has_paid_orders": bool(info.has_paid_orders),
            "paid_orders_count": int(info.orders_count),
        }
