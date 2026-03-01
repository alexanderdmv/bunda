"""Decision rules.

This project intentionally keeps rules *config-driven* so you can tweak thresholds
in `config/config.yaml` without touching code.

All rules are defensive:
- Missing token fields => fail only if that field is required by config.
- None/invalid values are treated as missing.
"""

from __future__ import annotations

from typing import Any, Optional


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None

# Back-compat aliases (older rule blocks used these helpers)
_i = _to_int
_f = _to_float



def _cfg_float(cfg: dict, key: str) -> Optional[float]:
    v = cfg.get(key, None)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _cfg_int(cfg: dict, key: str) -> Optional[int]:
    v = cfg.get(key, None)
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def hard_rules_explain(token: dict, cfg: dict) -> tuple[bool, str]:
    """Return (ok, reason).

    reason is a short, stable string you can use for debugging and stats.
    """

    # PENDING_RECHECK statuses from DexScreener adapter
    # Note: for pump.fun sniping you may want to allow "no_pairs" when bonding-curve data exists.
    ds_status = token.get("_ds_status")
    if ds_status in ("http_error", "request_error"):
        return False, str(ds_status)
    if ds_status == "no_pairs":
        allow_no_pairs_if_curve = bool(cfg.get("allow_no_pairs_if_bonding_curve", False))
        has_curve = token.get("pumpfun_bonding_curve") or token.get("pump_curve_real_sol_reserves") is not None or token.get("pump_curve_real_sol_reserves_lamports") is not None
        if not (allow_no_pairs_if_curve and has_curve):
            return False, "no_pairs"

    # Optional: require a non-empty symbol (avoid UNKNOWN)
    if bool(cfg.get("require_nonempty_symbol", False)):
        sym = token.get("symbol")
        if sym is None or not str(sym).strip():
            return False, "symbol_missing"

    # 1) Liquidity gate
    # Default behavior: require DexScreener liquidity_usd.
    # For pump.fun, you can switch to bonding-curve reserves:
    #   liquidity_source: bonding_curve|either
    #   min_bonding_curve_real_sol: <float SOL>
    liquidity_source = str(cfg.get("liquidity_source", "dexscreener") or "dexscreener").strip().lower()

    # Bonding curve reserve (SOL)
    curve_sol = _to_float(token.get("pump_curve_real_sol_reserves"))
    if curve_sol is None:
        lamports = _to_float(token.get("pump_curve_real_sol_reserves_lamports"))
        if lamports is not None:
            curve_sol = lamports / 1_000_000_000.0

    min_curve_sol = _cfg_float(cfg, "min_bonding_curve_real_sol")

    # DexScreener liquidity (USD)
    min_liq = _cfg_float(cfg, "min_liquidity_usd")
    liq = _to_float(token.get("liquidity_usd"))

    if liquidity_source in ("bondx", "dex", "dexscreener"):
        # Legacy strict: liquidity_usd must exist
        if min_liq is None:
            min_liq = 0.0
        if liq is None:
            return False, "liq_missing"
        if liq < float(min_liq):
            return False, "liq_lt_min"

    elif liquidity_source in ("bonding_curve", "curve", "pumpfun"):
        if min_curve_sol is None:
            # if not set, treat any curve as acceptable
            min_curve_sol = 0.0
        if curve_sol is None:
            return False, "curve_sol_missing"
        if curve_sol < float(min_curve_sol):
            return False, "curve_sol_lt_min"

    elif liquidity_source in ("either", "curve_or_dex", "dex_or_curve"):
        # Pass if EITHER gate passes; if both missing -> fail with a stable reason.
        ok_curve = False
        ok_liq = False
        if min_curve_sol is not None and curve_sol is not None and curve_sol >= float(min_curve_sol):
            ok_curve = True
        if min_liq is not None and liq is not None and liq >= float(min_liq):
            ok_liq = True
        if not (ok_curve or ok_liq):
            if curve_sol is None and liq is None:
                return False, "liq_and_curve_missing"
            if curve_sol is None and min_curve_sol is not None:
                return False, "curve_sol_missing"
            if liq is None and min_liq is not None:
                return False, "liq_missing"
            # Both exist but below thresholds
            if min_curve_sol is not None and curve_sol is not None and curve_sol < float(min_curve_sol):
                return False, "curve_sol_lt_min"
            return False, "liq_lt_min"

    else:
        # Unknown mode -> keep legacy strict behavior
        min_liq = float(min_liq or 0.0)
        if liq is None:
            return False, "liq_missing"
        if liq < float(min_liq):
            return False, "liq_lt_min"

    # 2) 24h Volume (optional)
    min_vol = _cfg_float(cfg, "min_volume_h24_usd")
    if min_vol is not None:
        vol = _to_float(token.get("volume_h24_usd"))
        if vol is None or vol < float(min_vol):
            return False, "vol_h24_lt_min"

    # 3) Trades in 24h (optional)
    min_trade = _cfg_int(cfg, "min_trade_h24")
    if min_trade is not None:
        trade = _to_int(token.get("trade_h24"))
        if trade is None or trade < int(min_trade):
            return False, "trade_h24_lt_min"

    # 4) Unique wallets in 24h (optional)
    min_uw = _cfg_int(cfg, "min_unique_wallet_h24")
    if min_uw is not None:
        uw = _to_int(token.get("unique_wallet_h24"))
        if uw is None or uw < int(min_uw):
            return False, "unique_wallet_h24_lt_min"

    # 5) Holders (optional)
    min_holders = _cfg_int(cfg, "min_holders")
    if min_holders is not None:
        holders = _to_int(token.get("holders"))
        if holders is None or holders < int(min_holders):
            return False, "holders_lt_min"

    # 6) Last trade recency in minutes (optional, upper bound)
    max_last_trade_min = _cfg_float(cfg, "max_last_trade_minutes")
    if max_last_trade_min is not None:
        last_trade_min = _to_float(token.get("last_trade_minutes"))
        if last_trade_min is None or last_trade_min > float(max_last_trade_min):
            return False, "last_trade_minutes_too_old"

    # 7) Optional: reject extreme +/- 24h price change (very often rugs / illiquid)
    max_abs_pc = _cfg_float(cfg, "max_abs_price_change_h24")
    if max_abs_pc is not None:
        pc = _to_float(token.get("price_change_h24"))
        if pc is None or abs(pc) > float(max_abs_pc):
            return False, "abs_price_change_h24_too_high"

    # 8) Optional: minimum price (avoid ultra-dust where metrics lie)
    min_price = _cfg_float(cfg, "min_price_usd")
    if min_price is not None:
        price = _to_float(token.get("price_usd"))
        if price is None or price < float(min_price):
            return False, "price_lt_min"

    # 9) Optional: age gate (minutes since LP creation)
    min_age = _cfg_float(cfg, "min_age_minutes")
    if min_age is not None:
        age = _to_float(token.get("age_minutes"))
        if age is None or age < float(min_age):
            return False, "age_lt_min"

    max_age = _cfg_float(cfg, "max_age_minutes")
    if max_age is not None:
        age = _to_float(token.get("age_minutes"))
        if age is None or age > float(max_age):
            return False, "age_gt_max"

    # 10) Optional: valuation gates
    min_mcap = _cfg_float(cfg, "min_market_cap_usd")
    if min_mcap is not None:
        mcap = _to_float(token.get("market_cap_usd"))
        if mcap is None or mcap < float(min_mcap):
            return False, "mcap_lt_min"

    max_mcap = _cfg_float(cfg, "max_market_cap_usd")
    if max_mcap is not None:
        mcap = _to_float(token.get("market_cap_usd"))
        if mcap is None or mcap > float(max_mcap):
            return False, "mcap_gt_max"

    min_fdv = _cfg_float(cfg, "min_fdv_usd")
    if min_fdv is not None:
        fdv = _to_float(token.get("fdv_usd"))
        if fdv is None or fdv < float(min_fdv):
            return False, "fdv_lt_min"

    max_fdv = _cfg_float(cfg, "max_fdv_usd")
    if max_fdv is not None:
        fdv = _to_float(token.get("fdv_usd"))
        if fdv is None or fdv > float(max_fdv):
            return False, "fdv_gt_max"

    # 11) Optional: buy/sell activity gates (DexScreener provides these)
    min_buy = _cfg_int(cfg, "min_buy_h24")
    if min_buy is not None:
        buys = _to_int(token.get("buy_h24"))
        if buys is None or buys < int(min_buy):
            return False, "buy_h24_lt_min"

    min_sell = _cfg_int(cfg, "min_sell_h24")
    if min_sell is not None:
        sells = _to_int(token.get("sell_h24"))
        if sells is None or sells < int(min_sell):
            return False, "sell_h24_lt_min"

    max_s2b = _cfg_float(cfg, "max_sell_to_buy_ratio")
    if max_s2b is not None:
        buys = _to_float(token.get("buy_h24"))
        sells = _to_float(token.get("sell_h24"))
        if buys is None or sells is None:
            return False, "sell_to_buy_missing"
        # avoid div by zero: if buys==0 but sells>0, ratio is infinite
        if buys <= 0 and sells > 0:
            return False, "sell_to_buy_ratio_too_high"
        if buys > 0 and (sells / buys) > float(max_s2b):
            return False, "sell_to_buy_ratio_too_high"

    # 11b) Early windows (m5/h1/h6) -- helps catch runners sooner and avoid dead tokens
    for w in ("m5", "h1", "h6"):
        min_trade_w = _cfg_int(cfg, f"min_trade_{w}")
        if min_trade_w is not None:
            tr = _to_int(token.get(f"trade_{w}"))
            if tr is None or tr < int(min_trade_w):
                return False, f"trade_{w}_lt_min"

        min_buy_w = _cfg_int(cfg, f"min_buy_{w}")
        if min_buy_w is not None:
            b = _to_int(token.get(f"buy_{w}"))
            if b is None or b < int(min_buy_w):
                return False, f"buy_{w}_lt_min"

        min_vol_w = _cfg_float(cfg, f"min_volume_{w}_usd")
        if min_vol_w is not None:
            v = _to_float(token.get(f"volume_{w}_usd"))
            if v is None or v < float(min_vol_w):
                return False, f"vol_{w}_lt_min"

        max_s2b_w = _cfg_float(cfg, f"max_sell_to_buy_ratio_{w}")
        if max_s2b_w is not None:
            b = _to_float(token.get(f"buy_{w}"))
            s = _to_float(token.get(f"sell_{w}"))
            if b is None or s is None:
                return False, f"sell_to_buy_{w}_missing"
            if b <= 0 and s > 0:
                return False, f"sell_to_buy_ratio_{w}_too_high"
            if b > 0 and (s / b) > float(max_s2b_w):
                return False, f"sell_to_buy_ratio_{w}_too_high"

        max_abs_pc_w = _cfg_float(cfg, f"max_abs_price_change_{w}")
        if max_abs_pc_w is not None:
            pc = _to_float(token.get(f"price_change_{w}"))
            if pc is not None and abs(pc) > float(max_abs_pc_w):
                return False, f"abs_price_change_{w}_too_high"

    # 11c) Social / website gates (DexScreener info.*)
    if bool(cfg.get("require_website", False)) and not bool(token.get("has_website")):
        return False, "no_website"
    if bool(cfg.get("require_twitter", False)) and not bool(token.get("has_twitter")):
        return False, "no_twitter"
    if bool(cfg.get("require_telegram", False)) and not bool(token.get("has_telegram")):
        return False, "no_telegram"
    if bool(cfg.get("require_discord", False)) and not bool(token.get("has_discord")):
        return False, "no_discord"
    min_socials = _cfg_int(cfg, "min_social_links")
    if min_socials is not None:
        count = 0
        for k in ("has_website", "has_twitter", "has_telegram", "has_discord"):
            if bool(token.get(k)):
                count += 1
        if count < int(min_socials):
            return False, "socials_lt_min"

    # 11d) Authority gates (requires Helius getAccountInfo(jsonParsed))
    # Note: jsonParsed returns None for revoked authorities.
    if bool(cfg.get("require_mint_authority_revoked", False)):
        ma = token.get("mint_authority")
        if ma is not None and str(ma).strip() and str(ma).lower() != "null":
            return False, "mint_authority_not_revoked"

    if bool(cfg.get("require_no_freeze_authority", False)):
        fa = token.get("freeze_authority")
        if fa is not None and str(fa).strip() and str(fa).lower() != "null":
            return False, "freeze_authority_present"

    # 12) Optional: volume/liquidity ratio (momentum proxy)
    min_v2l = _cfg_float(cfg, "min_volume_to_liquidity_ratio")
    if min_v2l is not None:
        vol = _to_float(token.get("volume_h24_usd"))
        liq = _to_float(token.get("liquidity_usd"))
        if vol is None or liq is None or liq <= 0:
            return False, "vol_to_liq_missing"
        if (vol / liq) < float(min_v2l):
            return False, "vol_to_liq_lt_min"

    # 13) Optional: holder concentration (requires enrichment)
    max_top10 = _cfg_float(cfg, "max_top10_holder_pct")
    if max_top10 is not None:
        top10 = _to_float(token.get("top10_holder_pct"))
        if top10 is None or top10 > float(max_top10):
            return False, "top10_holder_pct_too_high"

    # 14) Optional: DexScreener signals (placeholders until enabled)
    if bool(cfg.get("require_boosted", False)):
        boosted = token.get("is_boosted")
        if boosted is not True:
            return False, "not_boosted"
        min_boost = _cfg_float(cfg, "min_boost_total_amount")
        if min_boost is not None:
            amt = _to_float(token.get("boost_total_amount"))
            if amt is None or amt < float(min_boost):
                return False, "boost_amt_lt_min"

    if bool(cfg.get("require_dex_paid", False)):
        paid = token.get("is_dex_paid")
        if paid is not True:
            return False, "not_dex_paid"


    # --- Runner / early-impulse checks (DexScreener m5 / h1 / h6 windows) ---
    # These are optional; they only apply if the corresponding config key is set.

    min_trade_m5 = _i(cfg.get("min_trade_m5"))
    if min_trade_m5 is not None:
        trade_m5 = _i(token.get("trade_m5"))
        if trade_m5 is None or trade_m5 < min_trade_m5:
            return False, "trade_m5_lt_min"

    min_volume_m5_usd = _f(cfg.get("min_volume_m5_usd"))
    if min_volume_m5_usd is not None:
        vol_m5 = _f(token.get("volume_m5_usd"))
        if vol_m5 is None or vol_m5 < min_volume_m5_usd:
            return False, "volume_m5_lt_min"

    min_buy_m5 = _i(cfg.get("min_buy_m5"))
    if min_buy_m5 is not None:
        buy_m5 = _i(token.get("buy_m5"))
        if buy_m5 is None or buy_m5 < min_buy_m5:
            return False, "buy_m5_lt_min"

    min_sell_m5 = _i(cfg.get("min_sell_m5"))
    if min_sell_m5 is not None:
        sell_m5 = _i(token.get("sell_m5"))
        if sell_m5 is None or sell_m5 < min_sell_m5:
            return False, "sell_m5_lt_min"

    max_sell_to_buy_ratio_m5 = _f(cfg.get("max_sell_to_buy_ratio_m5"))
    if max_sell_to_buy_ratio_m5 is not None:
        buy_m5 = _i(token.get("buy_m5")) or 0
        sell_m5 = _i(token.get("sell_m5")) or 0
        denom = max(buy_m5, 1)
        ratio = float(sell_m5) / float(denom)
        if ratio > max_sell_to_buy_ratio_m5:
            return False, "sell_to_buy_ratio_m5_too_high"

    min_trade_h1 = _i(cfg.get("min_trade_h1"))
    if min_trade_h1 is not None:
        trade_h1 = _i(token.get("trade_h1"))
        if trade_h1 is None or trade_h1 < min_trade_h1:
            return False, "trade_h1_lt_min"

    min_volume_h1_usd = _f(cfg.get("min_volume_h1_usd"))
    if min_volume_h1_usd is not None:
        vol_h1 = _f(token.get("volume_h1_usd"))
        if vol_h1 is None or vol_h1 < min_volume_h1_usd:
            return False, "volume_h1_lt_min"


    max_sell_to_buy_ratio_h1 = _f(cfg.get("max_sell_to_buy_ratio_h1"))
    if max_sell_to_buy_ratio_h1 is not None:
        buy_h1 = _i(token.get("buy_h1")) or 0
        sell_h1 = _i(token.get("sell_h1")) or 0
        denom = max(buy_h1, 1)
        ratio = float(sell_h1) / float(denom)
        if ratio > max_sell_to_buy_ratio_h1:
            return False, "sell_to_buy_ratio_h1_too_high"

    min_trade_h6 = _i(cfg.get("min_trade_h6"))
    if min_trade_h6 is not None:
        trade_h6 = _i(token.get("trade_h6"))
        if trade_h6 is None or trade_h6 < min_trade_h6:
            return False, "trade_h6_lt_min"

    max_abs_price_change_h1 = _f(cfg.get("max_abs_price_change_h1"))
    if max_abs_price_change_h1 is not None:
        pc_h1 = _f(token.get("price_change_h1"))
        if pc_h1 is not None and abs(pc_h1) > max_abs_price_change_h1:
            return False, "abs_price_change_h1_too_high"

    max_abs_price_change_m5 = _f(cfg.get("max_abs_price_change_m5"))
    if max_abs_price_change_m5 is not None:
        pc_m5 = _f(token.get("price_change_m5"))
        if pc_m5 is not None and abs(pc_m5) > max_abs_price_change_m5:
            return False, "abs_price_change_m5_too_high"


    return True, "pass"


def hard_rules(token: dict, cfg: dict) -> bool:
    """Backward-compatible boolean wrapper."""
    ok, _ = hard_rules_explain(token, cfg)
    return ok
