import os
import yaml
import time
import requests

from evaluation.metrics import DecisionStats
from utils.logger import setup_logger
from pipeline.decision.filters import decide
from pipeline.decision.history import save_decision
from pipeline.decision.snapshot import append_snapshot

from pipeline.ml.scorer import MLScorer, MLScorerConfig

from pipeline.control import load_control, deep_get
from pipeline.secrets import load_secrets, get_secret

from pipeline.adapters.axiom_adapter import stream_tokens as stream_tokens_dummy
from pipeline.adapters.birdeye_adapter import stream_tokens_birdeye
from pipeline.adapters.dexscreener_adapter import stream_tokens_dexscreener
from pipeline.adapters.helius_dexscreener_adapter import stream_tokens_helius_dexscreener

from pipeline.state.dex_state import DexStateStore

from pipeline.enrichment.helius_enricher import HeliusEnricher
from pipeline.enrichment.dexscreener_orders_enricher import DexScreenerOrdersEnricher


def load_config() -> dict:
    # Base config (often contains API keys)
    candidates = ["config/config.yaml", "config.yaml"]
    for path in candidates:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    raise FileNotFoundError("Config not found. Expected 'config/config.yaml' or 'config.yaml'.")


def main() -> None:
    config = load_config()
    control = load_control()
    # Secrets are loaded from config/secrets.yaml (preferred) and/or environment variables.
    # This keeps API keys out of the control panel and out of git history.
    secrets = load_secrets(optional=True)

    runtime = config.get("runtime", {}) or {}
    snapshots_path = str(runtime.get("snapshots_path", "data/processed/snapshots.jsonl"))
    snapshots_enabled = bool(runtime.get("snapshots_enabled", True))
    # control.yaml can override the runtime source
    control_source = deep_get(control, "app.source", None)
    if control_source:
        runtime = dict(runtime)
        runtime["source"] = str(control_source).strip()
    logger = setup_logger(runtime.get("log_level", "INFO"))
    stats = DecisionStats()


    # Optional trading hook (sniper scaffold). Disabled by default.
    try:
        from pipeline.trading.engine import TradingEngine
        trading_engine = TradingEngine((control or {}).get("trading") or {})
    except Exception:
        trading_engine = None


    # Pump.fun bonding-curve state via local TS executor (recommended).
    # This avoids brittle byte-level parsing of bonding-curve accounts in Python.
    pump_state_enricher = None
    try:
        from pipeline.enrichment.pump_state_from_executor import PumpStateFromExecutor

        ex_cfg = (control or {}).get('enrichment', {}) or {}
        ex_state_cfg = (ex_cfg.get('pump_state_from_executor') or {}) if isinstance(ex_cfg, dict) else {}
        if not isinstance(ex_state_cfg, dict):
            ex_state_cfg = {}

        executor_url = ((control or {}).get('trading') or {}).get('executor_url')
        if executor_url and bool(ex_state_cfg.get('enabled', True)):
            pump_state_enricher = PumpStateFromExecutor(
                executor_url=str(executor_url),
                enabled=True,
                timeout_seconds=float(ex_state_cfg.get('timeout_seconds', 1.8) or 1.8),
                cache_ttl_seconds=float(ex_state_cfg.get('cache_ttl_seconds', 2.5) or 2.5),
            )
    except Exception:
        pump_state_enricher = None


    # Optional ML scorer (v1) - ranks WATCH candidates
    ml_cfg = config.get("ml", {}) or {}
    scorer = None
    if bool(ml_cfg.get("enabled", False)):
        cfg_obj = MLScorerConfig(
            enabled=True,
            model_path=str(ml_cfg.get("model_path", "models/tabular/scorer_lgbm.pkl")),
            score_threshold=float(ml_cfg.get("score_threshold", 0.75)),
            feature_cols=list(ml_cfg.get("feature_cols", []) or []),
        )
        scorer = MLScorer(cfg_obj)
        try:
            scorer.load()
            if scorer.model is None:
                logger.warning(f"ML scorer enabled but model file not found yet: {cfg_obj.model_path}")
        except Exception as e:
            logger.error(f"Failed to load ML scorer: {e}")
            scorer = None

    source = (runtime.get("source", "birdeye") or "birdeye").lower()

    ds_state_store = None  # set when using DexScreener
    ds_orders_enricher = None  # optional: DexScreener paid-orders checker

    if source == "birdeye":
        be_cfg = config.get("birdeye", {}) or {}
        # API key priority:
        # 1) environment variable BIRDEYE_API_KEY
        # 2) config/secrets.yaml -> birdeye.api_key
        # (we intentionally do NOT keep API keys in control.yaml)
        api_key = os.getenv("BIRDEYE_API_KEY") or get_secret(secrets, "birdeye.api_key")
        if not api_key or str(api_key).strip() in {"", "YOUR_API_KEY", "PASTE_KEY_HERE"}:
            raise RuntimeError(
                "Birdeye source selected but BIRDEYE_API_KEY is missing. "
                "Set env var BIRDEYE_API_KEY or create config/secrets.yaml with birdeye.api_key"
            )

        token_stream = stream_tokens_birdeye(
            api_key=str(api_key).strip(),
            chain=str(deep_get(control, "app.chain", be_cfg.get("chain", "solana"))).strip(),
            poll_seconds=int(be_cfg.get("poll_seconds", 3)),
            limit=be_cfg.get("limit", None),
            debug=bool(be_cfg.get("debug", False)),
            enrich_overview=bool(be_cfg.get("enrich_overview", True)),
            enrich_min_liquidity_usd=float(be_cfg.get("enrich_min_liquidity_usd", 5) or 5),
            enrich_sleep_seconds=float(be_cfg.get("enrich_sleep_seconds", 0.1) or 0.1),
            enrich_timeout_seconds=float(be_cfg.get("enrich_timeout_seconds", 8) or 8),
        )

    elif source == "dexscreener":
        ds_cfg = config.get("dexscreener", {}) or {}

        # Allow optional overrides from control.yaml (single control panel)
        ds_over = deep_get(control, "sources.dexscreener", {})
        if not isinstance(ds_over, dict):
            ds_over = {}

        def pick(key: str, default):
            v = ds_over.get(key, None)
            return default if v is None else v

        seen_path = str(pick("seen_path", ds_cfg.get("seen_path", "data/processed/seen_dexscreener.json")))

        # PENDING_RECHECK state store (prevents missing early runners that initially fail thresholds)
        pr = ds_over.get("pending_recheck", {})
        if not isinstance(pr, dict):
            pr = {}

        ds_state_store = DexStateStore(
            path=seen_path,
            enabled=bool(pr.get("enabled", True)),
            interval_seconds=int(pr.get("interval_seconds", 45) or 45),
            ttl_seconds=int(pr.get("ttl_seconds", 1800) or 1800),
            max_pending=int(pr.get("max_pending", 5000) or 5000),
            temporary_skip_reasons=pr.get(
                "temporary_skip_reasons",
                [
                    "no_pairs",
                    "http_error",
                    "request_error",
                    "liq_lt_min",
                    "vol_h24_lt_min",
                    "trade_h24_lt_min",
                    "buy_h24_lt_min",
                    "sell_h24_lt_min",
                    "sell_to_buy_h24_too_high",
                    "vol_to_liq_lt_min",

                    # Short windows
                    "trade_m5_lt_min",
                    "vol_m5_lt_min",
                    "abs_price_change_m5_too_high",
                    "sell_to_buy_m5_too_high",
                    "trade_h1_lt_min",
                    "vol_h1_lt_min",
                    "abs_price_change_h1_too_high",
                    "sell_to_buy_h1_too_high",
                    "trade_h6_lt_min",
                    "vol_h6_lt_min",
                    "abs_price_change_h6_too_high",
                    "sell_to_buy_h6_too_high",

                    "age_lt_min",
                    "mcap_lt_min",
                ],
            ),
            autosave_every=int(pr.get("autosave_every", 25) or 25),
            max_tries_total=int(pr.get("max_tries_total", 10) or 10),
            max_tries_errors=int(pr.get("max_tries_errors", 20) or 20),
            max_tries_no_pairs=int(pr.get("max_tries_no_pairs", 10) or 10),
            max_tries_metrics=int(pr.get("max_tries_metrics", 6) or 6),
        )

        token_stream = stream_tokens_dexscreener(
            poll_seconds=int(pick("poll_seconds", ds_cfg.get("poll_seconds", 20))),
            profiles_limit=int(pick("profiles_limit", ds_cfg.get("profiles_limit", 500))),
            chain=str(deep_get(control, "app.chain", ds_cfg.get("chain", "solana"))).strip(),
            timeout_seconds=float(pick("timeout_seconds", ds_cfg.get("timeout_seconds", 15) or 15)),
            seen_path=seen_path,
            max_new_tokens_per_poll=int(pick("max_new_tokens_per_poll", ds_cfg.get("max_new_tokens_per_poll", 60) or 60)),
            max_due_pending_per_poll=int(pr.get("max_due_pending_per_poll", 50) or 50),
            per_pair_sleep_seconds=float(pick("per_pair_sleep_seconds", ds_cfg.get("per_pair_sleep_seconds", 0.25) or 0.25)),
            limit=ds_cfg.get("limit", None),
            debug=bool(ds_cfg.get("debug", False)),
            state_store=ds_state_store,
        )

        # Optional: check paid orders for WATCH candidates (free DexScreener endpoint, 60 rpm)
        po_cfg = ds_over.get("paid_orders", {}) if isinstance(ds_over, dict) else {}
        if isinstance(po_cfg, dict) and bool(po_cfg.get("enabled", False)):
            ds_orders_enricher = DexScreenerOrdersEnricher(
                ttl_seconds=int(po_cfg.get("ttl_seconds", 300)),
                timeout_seconds=float(po_cfg.get("timeout_seconds", 10)),
            )


    elif source == "helius_dexscreener":
        hds_cfg = config.get("helius_dexscreener", {}) or {}

        # Allow optional overrides from control.yaml (single control panel)
        hds_over = deep_get(control, "sources.helius_dexscreener", {})
        if not isinstance(hds_over, dict):
            hds_over = {}

        def pick(key: str, default):
            v = hds_over.get(key, None)
            return default if v is None else v

        seen_path = str(pick("seen_path", hds_cfg.get("seen_path", "data/processed/seen_dexscreener.json")))

        # Reuse the same pending-recheck store used by the DexScreener adapter
        pr = hds_over.get("pending_recheck", {})
        if not isinstance(pr, dict):
            pr = {}

        ds_state_store = DexStateStore(
            path=seen_path,
            enabled=bool(pr.get("enabled", True)),
            interval_seconds=int(pr.get("interval_seconds", 45) or 45),
            ttl_seconds=int(pr.get("ttl_seconds", 1800) or 1800),
            max_pending=int(pr.get("max_pending", 5000) or 5000),
            temporary_skip_reasons=pr.get(
                "temporary_skip_reasons",
                [
                    "no_pairs",
                    "http_error",
                    "request_error",
                    "liq_lt_min",
                    "vol_h24_lt_min",
                    "trade_h24_lt_min",
                    "buy_h24_lt_min",
                    "sell_h24_lt_min",
                    "sell_to_buy_h24_too_high",
                    "vol_to_liq_lt_min",

                    # Short windows
                    "trade_m5_lt_min",
                    "vol_m5_lt_min",
                    "abs_price_change_m5_too_high",
                    "sell_to_buy_m5_too_high",
                    "trade_h1_lt_min",
                    "vol_h1_lt_min",
                    "abs_price_change_h1_too_high",
                    "sell_to_buy_h1_too_high",
                    "trade_h6_lt_min",
                    "vol_h6_lt_min",
                    "abs_price_change_h6_too_high",
                    "sell_to_buy_h6_too_high",

                    "age_lt_min",
                    "mcap_lt_min",
                ],
            ),
            autosave_every=int(pr.get("autosave_every", 25) or 25),
            max_tries_total=int(pr.get("max_tries_total", 10) or 10),
            max_tries_errors=int(pr.get("max_tries_errors", 20) or 20),
            max_tries_no_pairs=int(pr.get("max_tries_no_pairs", 10) or 10),
            max_tries_metrics=int(pr.get("max_tries_metrics", 6) or 6),
        )

        # Helius key is required for discovery
        # Separate keys are supported (recommended):
        # - HELIUS_API_KEY_PYTHON / secrets.yaml helius.api_key_python for the Python scanner
        # - executor can use HELIUS_API_KEY_EXECUTOR / helius.api_key_executor
        helius_key = (
            os.getenv("HELIUS_API_KEY_PYTHON")
            or get_secret(secrets, "helius.api_key_python")
            or os.getenv("HELIUS_API_KEY")
            or get_secret(secrets, "helius.api_key")
            or deep_get(control, "enrichment.helius.api_key", None)
        )
        if not helius_key or not str(helius_key).strip() or str(helius_key).strip() in {"YOUR_API_KEY", "PASTE_KEY_HERE"}:
            raise RuntimeError(
                "Helius+DexScreener source selected but HELIUS_API_KEY is missing. "
                "Set env var HELIUS_API_KEY or create config/secrets.yaml with helius.api_key"
            )

        programs = hds_over.get("programs", {}) if isinstance(hds_over.get("programs", {}), dict) else {}
        pumpfun_program_id = str(programs.get("pumpfun_program_id", hds_cfg.get("pumpfun_program_id", ""))).strip()
        raydium_program_id = str(programs.get("raydium_amm_v4_program_id", hds_cfg.get("raydium_amm_v4_program_id", ""))).strip()

        ignore_mints = hds_over.get("ignore_mints", hds_cfg.get("ignore_mints", None))

        token_stream = stream_tokens_helius_dexscreener(
            helius_api_key=str(helius_key).strip(),
            rpc_base=str(pick("rpc_base", hds_cfg.get("rpc_base", "https://mainnet.helius-rpc.com"))),
            rpc_timeout_seconds=float(pick("rpc_timeout_seconds", hds_cfg.get("rpc_timeout_seconds", 20) or 20)),
            poll_seconds=float(pick("poll_seconds", hds_cfg.get("poll_seconds", 2) or 2)),
            signatures_limit=int(pick("signatures_limit", hds_cfg.get("signatures_limit", 200) or 200)),
            max_new_mints_per_poll=int(pick("max_new_mints_per_poll", hds_cfg.get("max_new_mints_per_poll", 80) or 80)),
            max_txs_per_poll=int(pick("max_txs_per_poll", hds_cfg.get("max_txs_per_poll", 80) or 80)),
            discovery_state_path=str(pick("discovery_state_path", hds_cfg.get("discovery_state_path", "data/processed/helius_discovery_state.json"))),
            pumpfun_meta_path=str(pick("pumpfun_meta_path", hds_cfg.get("pumpfun_meta_path", "data/processed/pumpfun_meta.json"))),
            pumpfun_curve_enrich=bool(pick("pumpfun_curve_enrich", hds_cfg.get("pumpfun_curve_enrich", True))),
            pumpfun_curve_refresh_seconds=float(pick("pumpfun_curve_refresh_seconds", hds_cfg.get("pumpfun_curve_refresh_seconds", 2.5) or 2.5)),
            pumpfun_program_id=pumpfun_program_id or None,
            raydium_program_id=raydium_program_id or None,
            ignore_mints=ignore_mints,
            chain=str(deep_get(control, "app.chain", hds_cfg.get("chain", "solana"))).strip(),
            dexscreener_timeout_seconds=float(pick("dexscreener_timeout_seconds", hds_cfg.get("dexscreener_timeout_seconds", 15) or 15)),
            per_pair_sleep_seconds=float(pick("per_pair_sleep_seconds", hds_cfg.get("per_pair_sleep_seconds", 0.25) or 0.25)),
            seen_path=seen_path,
            state_store=ds_state_store,
            max_due_pending_per_poll=int(pr.get("max_due_pending_per_poll", 50) or 50),
            debug=bool(pick("debug", hds_cfg.get("debug", False))),
        )

        # Optional: check paid orders for WATCH candidates (free DexScreener endpoint, 60 rpm)
        po_cfg = hds_over.get("paid_orders", {}) if isinstance(hds_over, dict) else {}
        if isinstance(po_cfg, dict) and bool(po_cfg.get("enabled", False)):
            ds_orders_enricher = DexScreenerOrdersEnricher(
                ttl_seconds=int(po_cfg.get("ttl_seconds", 300)),
                timeout_seconds=float(po_cfg.get("timeout_seconds", 10)),
            )

    elif source == "axiom":
        token_stream = stream_tokens_dummy()

    else:
        raise ValueError(f"Unknown runtime.source '{source}'. Use 'birdeye', 'dexscreener', 'helius_dexscreener' or 'axiom'.")

    # Filter control comes from control.yaml if present (single control panel)
    filter_cfg = deep_get(control, "filter", None)
    if not isinstance(filter_cfg, dict):
        filter_cfg = config.get("filter", {}) or {}


    # Prefilter config: disable Safe Gate for the FIRST pass so we don't SKIP on missing on-chain fields.
    # We apply Safe Gate only after Helius enrichment on WATCH candidates.
    filter_cfg_prefilter = filter_cfg
    try:
        layers = (filter_cfg or {}).get("layers") or {}
        safe_gate = layers.get("safe_gate") or {}
        if bool(safe_gate.get("enabled", False)):
            import copy as _copy
            filter_cfg_prefilter = _copy.deepcopy(filter_cfg)
            filter_cfg_prefilter.setdefault("layers", {}).setdefault("safe_gate", {})["enabled"] = False
    except Exception:
        filter_cfg_prefilter = filter_cfg

    debug_features = bool(runtime.get("debug_features", False))
    debug_limit = int(runtime.get("debug_limit", 50))


    # Optional: WATCH-only Helius enrichment
    enrich_cfg = config.get("enrichment", {}) or {}
    helius_cfg = enrich_cfg.get("helius", {}) or {}
    # control.yaml can override helius.enabled/rpc_base/timeout
    helius_over = deep_get(control, "enrichment.helius", {})
    if isinstance(helius_over, dict):
        helius_cfg = dict(helius_cfg)
        for k in ["enabled", "rpc_base", "timeout_seconds"]:
            if helius_over.get(k, None) is not None:
                helius_cfg[k] = helius_over.get(k)

    helius_enabled = bool(helius_cfg.get("enabled", False))
    helius_enricher = None
    if helius_enabled:
        # Key priority: env HELIUS_API_KEY -> secrets.yaml helius.api_key -> control.yaml enrichment.helius.api_key
        helius_key = (
            os.getenv("HELIUS_API_KEY_PYTHON")
            or get_secret(secrets, "helius.api_key_python")
            or os.getenv("HELIUS_API_KEY")
            or get_secret(secrets, "helius.api_key")
            or deep_get(control, "enrichment.helius.api_key", None)
        )
        if helius_key and str(helius_key).strip():
            helius_enricher = HeliusEnricher(
                api_key=str(helius_key).strip(),
                rpc_base=str(helius_cfg.get("rpc_base", "https://mainnet.helius-rpc.com")),
                timeout=float(helius_cfg.get("timeout_seconds", 20) or 20),
            )
            if bool(helius_cfg.get("startup_ping", True)):
                ok = helius_enricher.startup_ping()
                if ok:
                    logger.info("Helius RPC: connected (startup ping OK)")
                else:
                    logger.warning("Helius RPC: startup ping failed (check rpc_base/api_key)")
        else:
            logger.warning(
                "Helius enrichment is enabled but HELIUS_API_KEY is missing. "
                "Set env var HELIUS_API_KEY or add helius.api_key to config/secrets.yaml."
            )

    # Notifications (disabled by default)
    notify_console = deep_get(control, "notify.console", {})
    if not isinstance(notify_console, dict):
        notify_console = {}
    notify_discord = deep_get(control, "notify.discord", {})
    if not isinstance(notify_discord, dict):
        notify_discord = {}
    discord_enabled = bool(notify_discord.get("enabled", False))
    discord_only_watch = bool(notify_discord.get("only_watch", True))
    discord_min_interval = float(notify_discord.get("min_interval_seconds", 3) or 3)
    discord_webhook_url = (
        os.getenv("DISCORD_WEBHOOK_URL")
        or get_secret(secrets, "notify.discord.webhook_url")
        or ""
    ).strip()
    last_discord_sent = 0.0

    i = 0
    helius_failures = 0
    try:
        for token in token_stream:
            i += 1

            if debug_features:
                print(
                    "DEBUG FEATURES:",
                    {
                        k: token.get(k)
                        for k in [
                            "symbol",
                            "token_address",
                            "liquidity_usd",
                            "price_usd",
                            "volume_h24_usd",
                            "price_change_h24",
                            "market_cap_usd",
                            "fdv_usd",
                            "trade_h24",
                            "unique_wallet_h24",
                            "holders",
                            "last_trade_minutes",
                        ]
                    },
                )

            # Pump.fun curve state (real SOL reserves) from local executor
            if pump_state_enricher is not None:
                try:
                    pump_state_enricher.enrich(token)
                except Exception:
                    pass

            decision, reason = decide(token, filter_cfg_prefilter, ml_scorer=scorer, ml_cfg=ml_cfg)

            # WATCH-only: DexScreener paid-orders enrichment (no key).
            # Use it only once your base filter is strict; otherwise it may hit the 60 rpm cap.
            if decision == "WATCH" and ds_orders_enricher is not None and token.get("source") == "dexscreener":
                try:
                    ca_tmp = token.get("token_address")
                    chain_tmp = token.get("chain") or "solana"
                    if ca_tmp:
                        ds_orders_enricher.enrich_token(token, str(chain_tmp), str(ca_tmp))
                        # Re-run decision so require_paid_orders can take effect.
                        decision, reason = decide(token, filter_cfg, ml_scorer=scorer, ml_cfg=ml_cfg)
                except Exception as e:
                    # Don't crash the loop for an optional enrichment.
                    logger.debug(f"DexScreener paid-orders enrichment failed: {e}")

            # WATCH-only enrichment + optional re-decision
            if decision == "WATCH" and helius_enricher is not None:
                try:
                    helius_enricher.enrich_token(token)
                    # Re-run decision in case you start using enriched fields in rules later.
                    decision, reason = decide(token, filter_cfg, ml_scorer=scorer, ml_cfg=ml_cfg)
                except Exception as e:
                    helius_failures += 1
                    if helius_failures <= 5:
                        logger.warning(f"Helius enrichment failed: {e}")

            symbol = token.get("symbol") or "UNKNOWN"
            ca = token.get("token_address")
            ds_url = token.get("dexscreener_url")
            # Handle DexScreener pending-recheck smartly (avoid log/data spam)
            ds_state_result = None
            is_ds = (token.get("source") == "dexscreener")

            if ds_state_store is not None and is_ds and ca and str(decision).upper() == "SKIP":
                # Update state first so we can display next recheck time and stop duplicates.
                ds_state_result = ds_state_store.update_after_decision(str(ca), decision, reason)
                ds_state_store.maybe_autosave()

                if ds_state_result == "pending":
                    p = ds_state_store.pending.get(str(ca))
                    next_in = None
                    if p is not None:
                        try:
                            next_in = max(0, int(p.next_check_unix - time.time()))
                        except Exception:
                            next_in = None

                    # Log INFO only on first pending try; subsequent repeats go to DEBUG.
                    if p is not None and int(p.tries) <= 1:
                        msg = f"{symbol} -> PENDING ({reason}) | CA={ca}"
                        if next_in is not None:
                            msg += f" | next_check_in={next_in}s"
                        ml_score = token.get("ml_score")
                        if ml_score is not None:
                            try:
                                msg += f" | ml_score={float(ml_score):.3f}"
                            except Exception:
                                pass
                        if bool(notify_console.get("show_dexscreener_url", True)) and ds_url:
                            msg += f" | {ds_url}"
                        logger.info(msg)
                    else:
                        logger.debug(f"{symbol} -> PENDING ({reason}) | CA={ca}")

                    # Do not save to decisions.csv while pending (prevents 'million decisions.csv').
                    # Snapshot logging (needed for labels / ML). Includes PENDING too.
                    if snapshots_enabled:
                        try:
                            snap = dict(token)
                            snap["decision"] = "PENDING"
                            snap["reason"] = reason
                            if next_in is not None:
                                snap["next_check_in"] = next_in
                            append_snapshot(snap, snapshots_path)
                        except Exception as e:
                            logger.debug(f"Snapshot append failed (pending): {e}")

                    stats.update("PENDING")
                    stats.report()

                    # Trading hook (SPRAY mode): allow executor to react on very-early tokens
                    # while they are still PENDING in DexScreener (no_pairs / liq_lt_min etc.).
                    # In FILTER mode this will be ignored by TradingEngine.
                    try:
                        if trading_engine is not None:
                            trading_engine.maybe_trade(token, "PENDING", reason)
                            trading_engine.tick()
                    except Exception:
                        pass
                    continue

            # Make WATCH copy-paste friendly
            if decision == "WATCH" and bool(notify_console.get("show_ca_on_watch", True)):
                msg = f"{symbol} -> WATCH ({reason}) | CA={ca}"
                ml_score = token.get("ml_score")
                if ml_score is not None:
                    try:
                        msg += f" | ml_score={float(ml_score):.3f}"
                    except Exception:
                        pass
                if bool(notify_console.get("show_dexscreener_url", True)) and ds_url:
                    msg += f" | {ds_url}"
                logger.info(msg)
            else:
                ml_score = token.get("ml_score")
                extra = ""
                if ml_score is not None:
                    try:
                        extra = f" | ml_score={float(ml_score):.3f}"
                    except Exception:
                        extra = ""
                logger.info(f"{symbol} -> {decision} ({reason}){extra}")

            # Optional Discord webhook (keep OFF until filter is strict)
            if discord_enabled and discord_webhook_url:
                if (not discord_only_watch) or decision == "WATCH":
                    now = time.time()
                    if (now - last_discord_sent) >= discord_min_interval:
                        try:
                            content = f"{symbol} {decision} ({reason})\nCA: {ca}"
                            if ds_url:
                                content += f"\n{ds_url}"
                            requests.post(discord_webhook_url, json={"content": content}, timeout=10)
                            last_discord_sent = now
                        except Exception:
                            pass

            # Update DexScreener state store (PENDING_RECHECK) after final decision (non-pending path)
            if ds_state_store is not None and is_ds and ca and ds_state_result is None:
                ds_state_store.update_after_decision(str(ca), decision, reason)
                ds_state_store.maybe_autosave()

            # Snapshot logging (non-pending path)
            if snapshots_enabled:
                try:
                    snap = dict(token)
                    snap["decision"] = decision
                    snap["reason"] = reason
                    append_snapshot(snap, snapshots_path)
                except Exception as e:
                    logger.debug(f"Snapshot append failed: {e}")

            # Trading hook (safe: does nothing unless enabled)
            try:
                if trading_engine is not None:
                    trading_engine.maybe_trade(token, decision, reason)
                    trading_engine.tick()
            except Exception:
                pass

            save_decision(token, decision, reason)
            stats.update(decision)
            stats.report()

            if debug_features and i >= debug_limit:
                break

    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C).")
        stats.report()
    finally:
        # Best-effort flush state to disk
        try:
            if ds_state_store is not None:
                ds_state_store.save()
        except Exception:
            pass
        try:
            if helius_enricher is not None:
                logger.info(f"Helius RPC calls this run: {getattr(helius_enricher, 'requests_made', 0)}")
        except Exception:
            pass


if __name__ == "__main__":
    main()