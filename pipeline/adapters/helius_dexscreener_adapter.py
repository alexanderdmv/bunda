# pipeline/adapters/helius_dexscreener_adapter.py
from __future__ import annotations

import base64
import json
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.state.dex_state import DexStateStore
from pipeline.adapters.dexscreener_adapter import (
    DEX_BASE,
    _get as _ds_get,
    _normalize_pair as _ds_normalize_pair,
    _pick_main_pair as _ds_pick_main_pair,
)


# Defaults (can be overridden in config/control)
DEFAULT_PUMPFUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
DEFAULT_RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

# Pump.fun Anchor discriminator for `global:create` (first 8 bytes)
# [24, 30, 200, 40, 5, 28, 7, 119] => 0x181EC828051C0777
PUMPFUN_CREATE_DISCRIMINATOR_HEX = "181ec828051c0777"

# Pump.fun BondingCurve account layout helpers (best-effort).
# Some bots validate first 8 bytes (account signature) and then read u64 fields.
PUMPFUN_CURVE_ACCOUNT_SIGNATURE = bytes([0x17, 0xB7, 0xF8, 0x37, 0x60, 0xD8, 0xAC, 0x60])
PUMPFUN_CURVE_OFFSETS = {
    "virtual_token_reserves": 8,
    "virtual_sol_reserves": 16,
    "real_token_reserves": 24,
    "real_sol_reserves": 32,
    "token_total_supply": 40,
    "complete": 48,
}

# Common mints to ignore as "base token candidates"
DEFAULT_IGNORE_MINTS = {
    # wSOL
    "So11111111111111111111111111111111111111112",
    # USDC
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    # USDT
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
}


def _now_unix() -> int:
    return int(time.time())


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            js = json.load(f)
        return js if isinstance(js, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _append_jsonl(path: str, obj: dict) -> None:
    """Append one JSON object as a line (best-effort)."""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        return


class SolanaRpc:
    """Minimal Solana JSON-RPC client (works with Helius RPC)."""

    def __init__(self, rpc_url: str, timeout: float = 20.0):
        self.url = str(rpc_url).strip()
        self.timeout = float(timeout)

        self.session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({"content-type": "application/json"})

        self.requests_made = 0

    def call(self, method: str, params: list) -> Any:
        self.requests_made += 1
        payload = {"jsonrpc": "2.0", "id": "1", "method": method, "params": params}
        r = self.session.post(self.url, json=payload, timeout=self.timeout)
        if r.status_code == 429:
            raise requests.HTTPError("rate_limited", response=r)
        r.raise_for_status()
        js = r.json()
        if isinstance(js, dict) and js.get("error"):
            return None
        return js.get("result") if isinstance(js, dict) else None


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_MAP = {c: i for i, c in enumerate(_B58_ALPHABET)}


def _b58decode(s: str) -> bytes:
    """Minimal base58 decode (no external dependency)."""
    s = str(s or "")
    num = 0
    for ch in s:
        num = num * 58 + _B58_MAP.get(ch, 0)
    full = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
    # Handle leading zeros
    n_pad = 0
    for ch in s:
        if ch == "1":
            n_pad += 1
        else:
            break
    return b"\x00" * n_pad + full


def _instr_data_prefix_hex(data: str, nbytes: int = 8) -> str:
    """Decode instruction `data` (base58 or base64), return first nbytes as hex."""
    if not isinstance(data, str) or not data:
        return ""
    try:
        if any(ch in data for ch in "+/="):
            raw = base64.b64decode(data)
            return raw[:nbytes].hex()
    except Exception:
        pass
    try:
        raw = _b58decode(data)
        return raw[:nbytes].hex()
    except Exception:
        return ""


def _u64_le(buf: bytes, off: int) -> int:
    try:
        return int.from_bytes(buf[off : off + 8], "little", signed=False)
    except Exception:
        return 0


def _bool_le(buf: bytes, off: int) -> bool:
    try:
        return bool(int.from_bytes(buf[off : off + 1], "little", signed=False))
    except Exception:
        return False


def _parse_pumpfun_curve_account(raw: bytes) -> Optional[Dict[str, Any]]:
    """Parse Pump.fun BondingCurve account bytes into reserves (best-effort)."""
    if not isinstance(raw, (bytes, bytearray)):
        return None
    raw = bytes(raw)
    if len(raw) < 56:
        return None
    if raw[:8] != PUMPFUN_CURVE_ACCOUNT_SIGNATURE:
        return None

    vt = _u64_le(raw, PUMPFUN_CURVE_OFFSETS["virtual_token_reserves"])
    vs = _u64_le(raw, PUMPFUN_CURVE_OFFSETS["virtual_sol_reserves"])
    rt = _u64_le(raw, PUMPFUN_CURVE_OFFSETS["real_token_reserves"])
    rs = _u64_le(raw, PUMPFUN_CURVE_OFFSETS["real_sol_reserves"])
    supply = _u64_le(raw, PUMPFUN_CURVE_OFFSETS["token_total_supply"])
    complete = _bool_le(raw, PUMPFUN_CURVE_OFFSETS["complete"])
    return {
        "pump_curve_virtual_token_reserves": vt,
        "pump_curve_virtual_sol_reserves_lamports": vs,
        "pump_curve_real_token_reserves": rt,
        "pump_curve_real_sol_reserves_lamports": rs,
        "pump_curve_token_total_supply": supply,
        "pump_curve_complete": complete,
        "pump_curve_real_sol_reserves": rs / 1_000_000_000.0,
    }


def _rpc_get_account_bytes(rpc: SolanaRpc, pubkey: str) -> Optional[bytes]:
    try:
        res = rpc.call("getAccountInfo", [str(pubkey), {"encoding": "base64"}])
        if not isinstance(res, dict):
            return None
        val = res.get("value")
        if not isinstance(val, dict):
            return None
        data = val.get("data")
        if isinstance(data, list) and data and isinstance(data[0], str):
            return base64.b64decode(data[0])
        if isinstance(data, str):
            return base64.b64decode(data)
        return None
    except Exception:
        return None


def _iter_program_instructions(tx: dict) -> Iterator[Tuple[str, dict]]:
    """Yield (program_id, instruction_dict) for top-level instructions."""
    try:
        msg = (tx.get("transaction") or {}).get("message") or {}
        account_keys = msg.get("accountKeys") or []
        keys: List[str] = []
        for k in account_keys:
            if isinstance(k, str):
                keys.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if isinstance(pk, str):
                    keys.append(pk)

        insts = msg.get("instructions") or []
        for inst in insts:
            if not isinstance(inst, dict):
                continue

            pid = inst.get("programId")
            if isinstance(pid, dict):
                pid = pid.get("pubkey") or pid.get("toString")
            if not (isinstance(pid, str) and pid.strip()):
                idx = inst.get("programIdIndex")
                if isinstance(idx, int) and 0 <= idx < len(keys):
                    pid = keys[idx]
            if isinstance(pid, str) and pid.strip():
                yield pid.strip(), inst
    except Exception:
        return


def _extract_mints_from_token_balances(tx: dict) -> Tuple[List[str], List[str]]:
    pre: List[str] = []
    post: List[str] = []
    meta = (tx.get("meta") or {})
    for item in (meta.get("preTokenBalances") or []):
        if isinstance(item, dict):
            m = item.get("mint")
            if isinstance(m, str) and m:
                pre.append(m)
    for item in (meta.get("postTokenBalances") or []):
        if isinstance(item, dict):
            m = item.get("mint")
            if isinstance(m, str) and m:
                post.append(m)
    return pre, post


def _pick_candidate_mints(
    pre_mints: Sequence[str],
    post_mints: Sequence[str],
    ignore_mints: set[str],
) -> List[str]:
    """Choose likely "new mint" candidates from token balance deltas."""
    pre_set = set(pre_mints)
    created = [m for m in post_mints if m and m not in pre_set]
    candidates = created or list(dict.fromkeys(post_mints))

    out: List[str] = []
    for m in candidates:
        if not isinstance(m, str) or not m:
            continue
        if m in ignore_mints:
            continue
        out.append(m)
    return out


def _extract_pumpfun_mint_from_create(tx: dict, pump_program_id: str) -> Optional[str]:
    """If tx contains a Pump.fun create instruction, return the mint pubkey (accounts[0]) when possible."""
    try:
        msg = (tx.get("transaction") or {}).get("message") or {}
        account_keys = msg.get("accountKeys") or []
        keys: List[str] = []
        for k in account_keys:
            if isinstance(k, str):
                keys.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if isinstance(pk, str):
                    keys.append(pk)

        for pid, inst in _iter_program_instructions(tx):
            if pid != pump_program_id:
                continue
            data = inst.get("data")
            if isinstance(data, str) and _instr_data_prefix_hex(data, 8) == PUMPFUN_CREATE_DISCRIMINATOR_HEX:
                accts = inst.get("accounts") or []
                if accts and isinstance(accts[0], str):
                    return accts[0]
                if accts and isinstance(accts[0], int) and 0 <= accts[0] < len(keys):
                    return keys[accts[0]]
        return None
    except Exception:
        return None


def _extract_pumpfun_create_accounts(tx: dict, pump_program_id: str) -> Optional[Dict[str, str]]:
    """Best-effort extractor for Pump.fun create accounts."""
    try:
        msg = (tx.get("transaction") or {}).get("message") or {}
        account_keys = msg.get("accountKeys") or []
        keys: List[str] = []
        for k in account_keys:
            if isinstance(k, str):
                keys.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if isinstance(pk, str):
                    keys.append(pk)

        for pid, inst in _iter_program_instructions(tx):
            if pid != pump_program_id:
                continue
            data = inst.get("data")
            if not (isinstance(data, str) and data):
                continue
            if _instr_data_prefix_hex(data, 8) != PUMPFUN_CREATE_DISCRIMINATOR_HEX:
                continue
            accts = inst.get("accounts") or []

            mint = curve = assoc = None
            if accts and isinstance(accts[0], str):
                mint = accts[0] if len(accts) > 0 else None
                curve = accts[2] if len(accts) > 2 else None
                assoc = accts[3] if len(accts) > 3 else None
            elif accts and isinstance(accts[0], int):
                def idx(i: int) -> Optional[str]:
                    if len(accts) <= i:
                        return None
                    j = accts[i]
                    if isinstance(j, int) and 0 <= j < len(keys):
                        return keys[j]
                    return None

                mint = idx(0)
                curve = idx(2)
                assoc = idx(3)

            out: Dict[str, str] = {}
            if mint:
                out["pumpfun_mint"] = str(mint)
            if curve:
                out["pumpfun_bonding_curve"] = str(curve)
            if assoc:
                out["pumpfun_associated_bonding_curve"] = str(assoc)
            return out or None
        return None
    except Exception:
        return None


def _is_pumpfun_create(tx: dict, pump_program_id: str) -> bool:
    for pid, inst in _iter_program_instructions(tx):
        if pid != pump_program_id:
            continue
        data = inst.get("data")
        if isinstance(data, str) and data:
            if _instr_data_prefix_hex(data, 8) == PUMPFUN_CREATE_DISCRIMINATOR_HEX:
                return True
    # Fallback: some RPCs don't include `data` (rare)
    meta = tx.get("meta") or {}
    logs = meta.get("logMessages") or []
    for line in logs:
        if isinstance(line, str) and "Instruction: Create" in line:
            return True
    return False


def _extract_raydium_mints_from_initialize(tx: dict, raydium_program_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Best-effort: tokenA/tokenB mints from Raydium initialize2 accounts[8], accounts[9]."""
    try:
        for pid, inst in _iter_program_instructions(tx):
            if pid != raydium_program_id:
                continue
            accts = inst.get("accounts") or []
            if len(accts) < 10:
                continue
            if isinstance(accts[8], str) and isinstance(accts[9], str):
                return accts[8], accts[9]
        return None, None
    except Exception:
        return None, None


def _is_raydium_initialize(tx: dict) -> bool:
    meta = tx.get("meta") or {}
    logs = meta.get("logMessages") or []
    for line in logs:
        if not isinstance(line, str):
            continue
        if "initialize2" in line.lower():
            return True
    return False


def _build_helius_rpc_url(rpc_base: str, api_key: str) -> str:
    base = str(rpc_base or "").rstrip("/")
    key = str(api_key or "").strip()
    if not key:
        return base
    if "api-key=" in base:
        return base
    return f"{base}/?api-key={key}"


def stream_tokens_helius_dexscreener(
    *,
    helius_api_key: str,
    rpc_base: str = "https://mainnet.helius-rpc.com",
    rpc_timeout_seconds: float = 20.0,
    poll_seconds: float = 2.0,
    signatures_limit: int = 200,
    max_new_mints_per_poll: int = 80,
    max_txs_per_poll: int = 80,
    discovery_state_path: str = "data/processed/helius_discovery_state.json",
    discovery_seen_jsonl_path: str = "data/processed/discovery_seen.jsonl",
    # Pump.fun metadata/cache (bonding curve pubkeys etc.)
    pumpfun_meta_path: str = "data/processed/pumpfun_meta.json",
    pumpfun_curve_enrich: bool = True,
    pumpfun_curve_refresh_seconds: float = 2.5,
    pumpfun_program_id: Optional[str] = DEFAULT_PUMPFUN_PROGRAM_ID,
    raydium_program_id: Optional[str] = DEFAULT_RAYDIUM_AMM_V4_PROGRAM_ID,
    ignore_mints: Optional[Sequence[str]] = None,
    # DexScreener hydration
    chain: str = "solana",
    dexscreener_timeout_seconds: float = 15.0,
    per_pair_sleep_seconds: float = 0.25,
    # Shared seen/pending store
    seen_path: str = "data/processed/seen_dexscreener.json",
    state_store: Optional[DexStateStore] = None,
    max_due_pending_per_poll: int = 50,
    debug: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Hybrid stream:

    1) Discover newest mints via Helius RPC by scanning:
       - Pump.fun program create instructions
       - Raydium AMM pool initialize2 logs
    2) Hydrate discovered mints via DexScreener /token-pairs endpoint
    3) Yield normalized tokens (source='dexscreener') so main.py can reuse existing pending logic.
    """

    api_key = str(helius_api_key or "").strip()
    if not api_key:
        raise RuntimeError("Helius discovery requires HELIUS_API_KEY (or config/secrets.yaml helius.api_key).")

    rpc_url = _build_helius_rpc_url(rpc_base, api_key)
    rpc = SolanaRpc(rpc_url=rpc_url, timeout=rpc_timeout_seconds)

    ignore_set = set(DEFAULT_IGNORE_MINTS)
    if ignore_mints:
        for m in ignore_mints:
            if isinstance(m, str) and m.strip():
                ignore_set.add(m.strip())

    if state_store is None:
        state_store = DexStateStore(seen_path)

    # Normalize program IDs (allow disable via empty string / 'disabled')
    pumpfun_enabled = True
    raydium_enabled = True

    if pumpfun_program_id is None:
        pumpfun_program_id = DEFAULT_PUMPFUN_PROGRAM_ID
    else:
        pumpfun_program_id = str(pumpfun_program_id).strip()
        if (not pumpfun_program_id) or pumpfun_program_id.lower() in ("disabled", "none", "false", "0"):
            pumpfun_enabled = False

    if raydium_program_id is None:
        raydium_program_id = DEFAULT_RAYDIUM_AMM_V4_PROGRAM_ID
    else:
        raydium_program_id = str(raydium_program_id).strip()
        if (not raydium_program_id) or raydium_program_id.lower() in ("disabled", "none", "false", "0"):
            raydium_enabled = False

    state = _load_json(discovery_state_path)
    last_pump_sig = str(state.get("pumpfun_last_sig") or "")
    last_raydium_sig = str(state.get("raydium_last_sig") or "")

    pump_meta = _load_json(pumpfun_meta_path)
    if not isinstance(pump_meta, dict):
        pump_meta = {}

    # In-memory cache for curve state (curve_pk -> (fetched_unix, parsed_dict))
    curve_cache: Dict[str, Tuple[int, Dict[str, Any]]] = {}

    def _attach_pump_meta_and_curve(token: Dict[str, Any], mint: str, now_unix: int) -> None:
        meta = pump_meta.get(mint)
        if isinstance(meta, dict):
            for k in (
                "pumpfun_bonding_curve",
                "pumpfun_associated_bonding_curve",
                "pumpfun_first_seen_unix",
                "pumpfun_first_seen_sig",
            ):
                if k in meta and meta.get(k) is not None:
                    token[k] = meta.get(k)

        curve_pk = token.get("pumpfun_bonding_curve")
        if not (pumpfun_curve_enrich and curve_pk):
            return
        curve_pk = str(curve_pk)

        cached = curve_cache.get(curve_pk)
        if cached is not None:
            fetched_unix, parsed = cached
            if (now_unix - int(fetched_unix)) <= int(max(0.0, float(pumpfun_curve_refresh_seconds))):
                token.update(parsed)
                return

        raw = _rpc_get_account_bytes(rpc, curve_pk)
        parsed = _parse_pumpfun_curve_account(raw) if raw else None
        if isinstance(parsed, dict):
            parsed["_pump_curve_fetched_unix"] = now_unix
            curve_cache[curve_pk] = (now_unix, parsed)
            token.update(parsed)

    def _fetch_new_sigs(program_id: str, last_sig: str) -> Tuple[List[str], str]:
        res = rpc.call("getSignaturesForAddress", [program_id, {"limit": int(signatures_limit)}])
        sigs: List[str] = []
        newest_sig: str = last_sig
        if isinstance(res, list) and res:
            first = res[0]
            if isinstance(first, dict) and isinstance(first.get("signature"), str):
                newest_sig = first["signature"]
            for item in res:
                if not isinstance(item, dict):
                    continue
                sig = item.get("signature")
                if not isinstance(sig, str) or not sig:
                    continue
                if last_sig and sig == last_sig:
                    break
                sigs.append(sig)
        return sigs, newest_sig

    while True:
        try:
            now = _now_unix()
            due_pending = state_store.due_pending_mints(now_unix=now, max_n=max_due_pending_per_poll)

            pump_sigs: List[str] = []
            ray_sigs: List[str] = []
            new_last_pump = last_pump_sig
            new_last_ray = last_raydium_sig

            if pumpfun_enabled:
                pump_sigs, new_last_pump = _fetch_new_sigs(pumpfun_program_id, last_pump_sig)
            if raydium_enabled:
                ray_sigs, new_last_ray = _fetch_new_sigs(raydium_program_id, last_raydium_sig)

            sigs_to_fetch = (pump_sigs + ray_sigs)[: int(max_txs_per_poll)]
            if debug:
                print(
                    f"[helius_dex] due_pending={len(due_pending)} pump_new_sigs={len(pump_sigs)} "
                    f"ray_new_sigs={len(ray_sigs)} tx_fetch={len(sigs_to_fetch)}"
                )

            discovered_mints: List[str] = []
            discovered_set: set[str] = set()

            for sig in sigs_to_fetch:
                tx = None
                try:
                    tx = rpc.call(
                        "getTransaction",
                        [
                            sig,
                            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
                        ],
                    )
                except requests.HTTPError as e:
                    if getattr(getattr(e, "response", None), "status_code", None) == 429:
                        if debug:
                            print("[helius_dex] Helius RPC 429; backing off")
                        time.sleep(max(2.0, float(poll_seconds)))
                        continue
                except Exception:
                    continue

                if not isinstance(tx, dict):
                    continue

                is_pump_create = pumpfun_enabled and _is_pumpfun_create(tx, pumpfun_program_id)
                is_raydium_init = raydium_enabled and _is_raydium_initialize(tx)

                if not (is_pump_create or is_raydium_init):
                    continue

                direct_mints: List[str] = []
                if is_pump_create:
                    create_meta = _extract_pumpfun_create_accounts(tx, pumpfun_program_id)
                    mint_from_create = None
                    if isinstance(create_meta, dict):
                        mint_from_create = create_meta.get("pumpfun_mint")
                    if not mint_from_create:
                        mint_from_create = _extract_pumpfun_mint_from_create(tx, pumpfun_program_id)
                    if mint_from_create:
                        direct_mints.append(mint_from_create)

                        # Persist pump.fun metadata for this mint
                        existing = pump_meta.get(mint_from_create)
                        if not isinstance(existing, dict):
                            existing = {}
                        merged = dict(existing)
                        if isinstance(create_meta, dict):
                            merged.update({k: v for k, v in create_meta.items() if v})
                        merged.setdefault("pumpfun_first_seen_unix", _now_unix())
                        merged.setdefault("pumpfun_first_seen_sig", sig)
                        pump_meta[mint_from_create] = merged

                if is_raydium_init:
                    a, b = _extract_raydium_mints_from_initialize(tx, raydium_program_id)
                    if a:
                        direct_mints.append(a)
                    if b:
                        direct_mints.append(b)

                pre_mints, post_mints = _extract_mints_from_token_balances(tx)
                candidates = direct_mints + _pick_candidate_mints(pre_mints, post_mints, ignore_set)
                # unique, keep order
                seen_local: set[str] = set()
                candidates = [m for m in candidates if (m and m not in ignore_set and not (m in seen_local or seen_local.add(m)))]
                if not candidates:
                    continue

                for m in candidates:
                    _append_jsonl(
                        discovery_seen_jsonl_path,
                        {"ts_unix": now, "sig": sig, "mint": m, "stage": "candidate"},
                    )

                    if len(discovered_mints) >= int(max_new_mints_per_poll):
                        _append_jsonl(
                            discovery_seen_jsonl_path,
                            {"ts_unix": now, "sig": sig, "mint": m, "stage": "drop", "reason": "cap_reached"},
                        )
                        continue

                    if state_store.is_known(m, now_unix=now):
                        _append_jsonl(
                            discovery_seen_jsonl_path,
                            {"ts_unix": now, "sig": sig, "mint": m, "stage": "drop", "reason": "known"},
                        )
                        continue

                    if m in discovered_set:
                        continue

                    discovered_set.add(m)
                    discovered_mints.append(m)
                    _append_jsonl(
                        discovery_seen_jsonl_path,
                        {"ts_unix": now, "sig": sig, "mint": m, "stage": "accepted", "reason": "new"},
                    )

            # Update last seen sig pointers
            if new_last_pump:
                last_pump_sig = new_last_pump
            if new_last_ray:
                last_raydium_sig = new_last_ray

            # Build processing list: due pending first, then freshly discovered
            processed_this_poll: set[str] = set()
            mints: List[str] = []
            discovered_this_poll = set(discovered_mints)
            for m in (due_pending + discovered_mints):
                if m in processed_this_poll:
                    continue
                processed_this_poll.add(m)
                mints.append(m)

            if debug:
                print(
                    f"[helius_dex] mints_due+new={len(mints)} (new={len(discovered_mints)}) "
                    f"known_seen={len(state_store.seen)} pending={len(state_store.pending)}"
                )

            for mint in mints:
                discovered_by = "helius" if mint in discovered_this_poll else "pending_recheck"
                try:
                    pairs = _ds_get(
                        f"{DEX_BASE}/token-pairs/v1/{chain}/{mint}",
                        timeout=dexscreener_timeout_seconds,
                    )
                except requests.HTTPError as e:
                    status = getattr(getattr(e, "response", None), "status_code", None)
                    if status == 429:
                        if debug:
                            print("[helius_dex] DexScreener 429; pausing")
                        state_store.save()
                        time.sleep(max(5.0, float(poll_seconds)))
                        break
                    token = {
                        "source": "dexscreener",
                        "token_address": mint,
                        "liquidity_usd": 0.0,
                        "_ds_status": "http_error",
                        "discovered_by": discovered_by,
                    }
                    _attach_pump_meta_and_curve(token, mint, now)
                    _append_jsonl(
                        discovery_seen_jsonl_path,
                        {"ts_unix": now, "mint": mint, "stage": "hydrate", "ds_status": "http_error", "discovered_by": discovered_by},
                    )
                    yield token
                    if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                        time.sleep(float(per_pair_sleep_seconds))
                    continue
                except requests.RequestException:
                    token = {
                        "source": "dexscreener",
                        "token_address": mint,
                        "liquidity_usd": 0.0,
                        "_ds_status": "request_error",
                        "discovered_by": discovered_by,
                    }
                    _attach_pump_meta_and_curve(token, mint, now)
                    _append_jsonl(
                        discovery_seen_jsonl_path,
                        {"ts_unix": now, "mint": mint, "stage": "hydrate", "ds_status": "request_error", "discovered_by": discovered_by},
                    )
                    yield token
                    if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                        time.sleep(float(per_pair_sleep_seconds))
                    continue

                if not isinstance(pairs, list) or not pairs:
                    token = {
                        "source": "dexscreener",
                        "token_address": mint,
                        "liquidity_usd": 0.0,
                        "_ds_status": "no_pairs",
                        "discovered_by": discovered_by,
                    }
                    _attach_pump_meta_and_curve(token, mint, now)
                    _append_jsonl(
                        discovery_seen_jsonl_path,
                        {"ts_unix": now, "mint": mint, "stage": "hydrate", "ds_status": "no_pairs", "discovered_by": discovered_by},
                    )
                    yield token
                    if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                        time.sleep(float(per_pair_sleep_seconds))
                    continue

                main_pair = _ds_pick_main_pair(pairs)
                if not isinstance(main_pair, dict):
                    token = {
                        "source": "dexscreener",
                        "token_address": mint,
                        "liquidity_usd": 0.0,
                        "_ds_status": "no_pairs",
                        "discovered_by": discovered_by,
                    }
                    _attach_pump_meta_and_curve(token, mint, now)
                    _append_jsonl(
                        discovery_seen_jsonl_path,
                        {"ts_unix": now, "mint": mint, "stage": "hydrate", "ds_status": "no_pairs", "discovered_by": discovered_by},
                    )
                    yield token
                    if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                        time.sleep(float(per_pair_sleep_seconds))
                    continue

                token = _ds_normalize_pair(mint, main_pair)
                token["_ds_status"] = "ok"
                token["discovered_by"] = discovered_by
                _attach_pump_meta_and_curve(token, mint, now)
                _append_jsonl(
                    discovery_seen_jsonl_path,
                    {"ts_unix": now, "mint": mint, "stage": "hydrate", "ds_status": "ok", "discovered_by": discovered_by},
                )
                yield token

                if per_pair_sleep_seconds and per_pair_sleep_seconds > 0:
                    time.sleep(float(per_pair_sleep_seconds))

            # Persist state stores
            state_store.save()
            try:
                _save_json(pumpfun_meta_path, pump_meta)
            except Exception:
                pass
            _save_json(
                discovery_state_path,
                {
                    "updated_at_unix": _now_unix(),
                    "pumpfun_program_id": pumpfun_program_id,
                    "raydium_program_id": raydium_program_id,
                    "pumpfun_enabled": bool(pumpfun_enabled),
                    "raydium_enabled": bool(raydium_enabled),
                    "pumpfun_last_sig": last_pump_sig,
                    "raydium_last_sig": last_raydium_sig,
                    "rpc_requests_made": rpc.requests_made,
                },
            )

        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if debug:
                print(f"[helius_dex] http error status={status}: {e!r}")
        except requests.RequestException as e:
            if debug:
                print(f"[helius_dex] request error: {e!r}")
        except Exception as e:
            if debug:
                print(f"[helius_dex] error: {e!r}")

        time.sleep(float(poll_seconds))
