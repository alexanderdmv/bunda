from pipeline.decision.rules import hard_rules_explain


def _maybe_attach_ml_score(token, ml_scorer=None, ml_cfg=None):
    """Compute ML score if enabled and attach to token as token['ml_score'].
    Returns score or None. Never raises."""
    try:
        if not (ml_cfg and isinstance(ml_cfg, dict) and ml_cfg.get("enabled")):
            return None
        if ml_scorer is None:
            return None
        score = ml_scorer.score_one(token)
        token["ml_score"] = score
        return score
    except Exception:
        return None
def decide(token, cfg, ml_scorer=None, ml_cfg=None):
    """Return (decision, reason).

    Supports two modes:

    1) Single-layer (backward compatible):
       filter: { ... }  -> uses hard_rules_explain(token, cfg)

    2) Two-layer gating:
       filter:
         layers:
           safe_gate:   { ... }   # hard safety checks (never pending)
           runner_gate: { ... }   # early-impulse checks (can be pending)

    Notes:
    - If cfg.enabled == false -> all tokens are WATCH (data-collection mode).
    - safe_gate failures are returned as SKIP with 'safe_<reason>' prefix
      so they are not treated as temporary/pending by DexStateStore.
    """
    if isinstance(cfg, dict) and cfg.get("enabled") is False:
        return "WATCH", "disabled"

    cfg = cfg or {}
    layers = cfg.get("layers") if isinstance(cfg, dict) else None

    # Two-layer mode
    if isinstance(layers, dict):
        safe_cfg = layers.get("safe_gate") or {}
        runner_cfg = layers.get("runner_gate") or {}

        if safe_cfg.get("enabled", True) is not False:
            ok, reason = hard_rules_explain(token, safe_cfg)
            if not ok:
                _maybe_attach_ml_score(token, ml_scorer, ml_cfg)
                return "SKIP", f"safe_{reason}"

        if runner_cfg.get("enabled", True) is not False:
            ok, reason = hard_rules_explain(token, runner_cfg)
            if not ok:
                _maybe_attach_ml_score(token, ml_scorer, ml_cfg)
                return "SKIP", reason

        # Optional ML scorer layer (post-rules)
        if ml_cfg and isinstance(ml_cfg, dict) and ml_cfg.get("enabled") and ml_scorer is not None:
            score = ml_scorer.score_one(token)
            token["ml_score"] = score
            thr = float(ml_cfg.get("score_threshold", 0.75))
            if ml_scorer.is_valid_score(score) and score >= thr:
                return "ALLOW", "ml_score_ge_thr"
            return "WATCH", "ml_score_lt_thr"

        return "WATCH", "pass"

    # Single-layer mode (legacy)
    ok, reason = hard_rules_explain(token, cfg)
    if not ok:
        _maybe_attach_ml_score(token, ml_scorer, ml_cfg)
        return "SKIP", reason

# Optional ML scorer layer (post-rules)
    if ml_cfg and isinstance(ml_cfg, dict) and ml_cfg.get("enabled") and ml_scorer is not None:
        score = ml_scorer.score_one(token)
        token["ml_score"] = score
        thr = float(ml_cfg.get("score_threshold", 0.75))
        if ml_scorer.is_valid_score(score) and score >= thr:
            return "ALLOW", "ml_score_ge_thr"
        return "WATCH", "ml_score_lt_thr"

    return "WATCH", reason
