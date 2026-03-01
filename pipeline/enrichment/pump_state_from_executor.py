import time
import requests
from typing import Any, Dict, Optional, Tuple


class PumpStateFromExecutor:
    """Fetch Pump.fun bonding-curve state from the local TS executor.

    Why: Pump.fun bonding-curve account layouts have changed over time (account size extensions),
    so parsing raw account bytes in Python is brittle. The official Pump SDK can fetch/parse
    state reliably. We use it via the local executor and cache the result.

    The enricher fills these token fields (if available):
      - pump_curve_real_sol_reserves (float, SOL)
      - pump_curve_real_sol_reserves_lamports (int)
      - pump_curve_virtual_sol_reserves (float, SOL)
      - pump_curve_virtual_sol_reserves_lamports (int)
      - pump_curve_state_source = 'executor'
    """

    def __init__(
        self,
        executor_url: str,
        enabled: bool = True,
        timeout_seconds: float = 1.8,
        cache_ttl_seconds: float = 2.5,
        max_cache_size: int = 10_000,
    ) -> None:
        self.executor_url = str(executor_url).rstrip("/")
        self.enabled = bool(enabled)
        self.timeout_seconds = float(timeout_seconds)
        self.cache_ttl_seconds = float(cache_ttl_seconds)
        self.max_cache_size = int(max_cache_size)
        self._cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def _cache_get(self, mint: str) -> Optional[Dict[str, Any]]:
        hit = self._cache.get(mint)
        if not hit:
            return None
        ts, payload = hit
        if (time.time() - ts) > self.cache_ttl_seconds:
            return None
        return payload

    def _cache_put(self, mint: str, payload: Dict[str, Any]) -> None:
        if len(self._cache) >= self.max_cache_size:
            # naive eviction: clear all (simple + safe)
            self._cache.clear()
        self._cache[mint] = (time.time(), payload)

    def fetch_state(self, mint: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        mint = str(mint).strip()
        if not mint:
            return None
        cached = self._cache_get(mint)
        if cached is not None:
            return cached

        url = f"{self.executor_url}/state"
        try:
            r = requests.get(url, params={"mint": mint}, timeout=self.timeout_seconds)
            if r.status_code != 200:
                return None
            data = r.json() or {}
            if not isinstance(data, dict):
                return None
            if not bool(data.get("ok", False)):
                return None
            payload = data.get("state") or {}
            if isinstance(payload, dict) and payload:
                self._cache_put(mint, payload)
                return payload
        except Exception:
            return None
        return None

    def enrich(self, token: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        if not isinstance(token, dict):
            return
        mint = token.get("token_address") or token.get("mint") or token.get("address")
        if not mint:
            return

        state = self.fetch_state(str(mint))
        if not state:
            return

        # We try multiple key names to be robust across SDK versions.
        real_sol = state.get("realSolReservesSol")
        real_lamports = state.get("realSolReservesLamports")
        if real_sol is None and real_lamports is not None:
            try:
                real_sol = float(real_lamports) / 1e9
            except Exception:
                real_sol = None

        virt_sol = state.get("virtualSolReservesSol")
        virt_lamports = state.get("virtualSolReservesLamports")
        if virt_sol is None and virt_lamports is not None:
            try:
                virt_sol = float(virt_lamports) / 1e9
            except Exception:
                virt_sol = None

        if real_sol is not None:
            token["pump_curve_real_sol_reserves"] = float(real_sol)
        if real_lamports is not None:
            try:
                token["pump_curve_real_sol_reserves_lamports"] = int(real_lamports)
            except Exception:
                pass

        if virt_sol is not None:
            token["pump_curve_virtual_sol_reserves"] = float(virt_sol)
        if virt_lamports is not None:
            try:
                token["pump_curve_virtual_sol_reserves_lamports"] = int(virt_lamports)
            except Exception:
                pass

        token["pump_curve_state_source"] = "executor"
