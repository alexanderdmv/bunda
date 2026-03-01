from __future__ import annotations

import time
from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


class HeliusEnricher:
    """WATCH-only on-chain enrichment via standard Solana JSON-RPC over Helius.

    This uses generic Solana RPC methods (getAccountInfo/getTokenSupply/getTokenLargestAccounts)
    and therefore works on free/basic tiers.

    Added fields (best-effort):
      - supply_amount, supply_ui, decimals
      - largest_accounts_n, top1_holder_pct, top5_holder_pct, top10_holder_pct
      - token_program, is_token_2022
      - mint_authority, freeze_authority
      - mint_authority_revoked, freeze_authority_revoked
    """

    def __init__(self, api_key: str, rpc_base: str = "https://mainnet.helius-rpc.com", timeout: float = 20.0):
        api_key = str(api_key or "").strip()
        self.url = str(rpc_base or "").rstrip("/") + f"/?api-key={api_key}"
        self.timeout = float(timeout)

        self.requests_made = 0

        self._session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",),
            raise_on_status=False,
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    def _rpc(self, method: str, params: list) -> Any:
        payload = {"jsonrpc": "2.0", "id": "1", "method": method, "params": params}
        self.requests_made += 1
        r = self._session.post(self.url, json=payload, timeout=self.timeout)
        r.raise_for_status()
        js = r.json()
        if isinstance(js, dict) and js.get("error"):
            return None
        return js.get("result") if isinstance(js, dict) else None

    def startup_ping(self) -> bool:
        """Lightweight check that the RPC endpoint works."""
        try:
            res = self._rpc("getHealth", [])
            return bool(res)
        except Exception:
            return False

    def enrich_token(self, token: Dict[str, Any]) -> None:
        mint = token.get("token_address")
        if not mint:
            return
        mint = str(mint)

        # 0) Mint account (authorities + program)
        info = self._rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
        if isinstance(info, dict):
            value = info.get("value") or {}
            owner = value.get("owner")
            token["token_program"] = owner
            token["is_token_2022"] = bool(owner == TOKEN_2022_PROGRAM_ID)

            data = value.get("data") or {}
            parsed = None
            if isinstance(data, dict):
                parsed = (data.get("parsed") or {}).get("info")
            if isinstance(parsed, dict):
                token["mint_authority"] = parsed.get("mintAuthority")
                token["freeze_authority"] = parsed.get("freezeAuthority")
                token["mint_authority_revoked"] = parsed.get("mintAuthority") is None
                token["freeze_authority_revoked"] = parsed.get("freezeAuthority") is None

        # 1) Token supply
        supply = self._rpc("getTokenSupply", [mint])
        if isinstance(supply, dict):
            val = supply.get("value") or {}
            token["supply_amount"] = val.get("amount")
            token["supply_ui"] = val.get("uiAmount")
            token["decimals"] = val.get("decimals")

        # 2) Largest accounts (concentration proxy)
        largest = self._rpc("getTokenLargestAccounts", [mint])
        if isinstance(largest, dict):
            accounts = (largest.get("value") or [])
            amounts = []
            for a in accounts:
                try:
                    amounts.append(float((a or {}).get("amount") or 0))
                except Exception:
                    amounts.append(0.0)

            total = sum(amounts)
            if total > 0:
                def pct(n: int) -> float:
                    return 100.0 * (sum(amounts[:n]) / total)

                token["top1_holder_pct"] = pct(1)
                token["top5_holder_pct"] = pct(5)
                token["top10_holder_pct"] = pct(10)
            token["largest_accounts_n"] = len(accounts)

        token["_helius_enriched_at_unix"] = int(time.time())
