# Sniper bot scaffold (for crypto_filter_v15)

This folder adds a **SAFETY-FIRST** trading hook to the existing scanner **plus** Pump.fun
early-stage helpers (bonding curve reserves).

- Default: **trading disabled**
- When enabled: supports **DRY_RUN** mode so you can verify triggers without risking funds.
- Execution is delegated to an external local service (TypeScript) via HTTP:
  - `POST /order { chain, venue, action, mint, amount_sol, max_slippage_bps, dry_run }`

Why external executor?
- pump.fun/pumpswap programs and their IDLs evolve frequently.
- Official SDKs exist in TypeScript and are usually updated first.
- Separating "decision" (Python) from "execution" (TS) keeps your main project stable.

What is included:
- `pipeline/trading/engine.py`: Python trading hook with **two modes**:
  - `filter`: trades only on `WATCH` (normal "scanner->trade")
  - `spray`: can also attempt trades while a token is `PENDING` (e.g. `no_pairs`, `liq_lt_min`) for very early snipes
- `executor_ts/`: a local TypeScript executor scaffold that can send **Jito bundles** (JSON-RPC `sendBundle`).
- Pump.fun bonding curve enrichment:
  - stores Pump.fun create metadata in `data/processed/pumpfun_meta.json`
  - optionally fetches bonding curve reserves via RPC so you can gate by `min_bonding_curve_real_sol`

Next steps:
1) Enable `trading:` in `config/control.yaml` (see example below).
2) Run the TypeScript executor in `executor_ts`.
3) Implement real Pump.fun buy/sell instruction building in TS (see `executor_ts/src/index.ts`).

Example config (control.yaml):

trading:
  enabled: false
  dry_run: true
  mode: spray
  buy_on_decision: WATCH
  buy_amount_sol: 0.02
  max_slippage_bps: 1500
  take_profit_pct: 50
  stop_loss_pct: 25
  max_open_positions: 3
  min_interval_per_mint_seconds: 90
  min_interval_global_seconds: 0.25
  spray_allow_reasons:
    - no_pairs
    - liq_lt_min
    - liq_missing
  executor_url: "http://127.0.0.1:8787"

Bonding curve gate example (runner_gate):

filter:
  layers:
    runner_gate:
      liquidity_source: bonding_curve
      allow_no_pairs_if_bonding_curve: true
      min_bonding_curve_real_sol: 2.0
