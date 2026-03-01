# Sniper v4 (Pump.fun + TS Executor + Jito bundles)

This project is split into two parts:

1) **Python** (`main.py`) — discovery + filtering + position logic (spray / filter modes)
2) **TypeScript executor** (`executor_ts`) — builds and (optionally) sends **Pump.fun buy/sell** transactions (supports **Jito bundles**)

You normally run **two terminals** (one for TS executor, one for Python).

## 0) What’s new vs v2

- ✅ **Real Pump.fun state** in Python via executor `GET /state` (fixes `curve_sol_missing`)
- ✅ Executor supports **buy/sell** with `@pump-fun/pump-sdk` and Jito bundles
- ✅ Two operating modes:
  - **spray**: trade on very early mints (even `no_pairs`) with minimal gating
  - **filter**: only trade when your filters return WATCH
- ✅ Optional TP/SL logic using executor `GET /quote` (sell-all estimate)

## 1) Python setup

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Create `config/secrets.yaml` (copy from example) and add **Helius API key**:

- `config/secrets.example.yaml` → `config/secrets.yaml`

Run:

```powershell
python main.py
```

## 2) Executor setup (TypeScript)

Open a second terminal:

```powershell
cd executor_ts
npm install
cp .env.example .env
npm run dev
```

### Create the keypair file (`id.json`)

In `executor_ts` you can generate it:

```powershell
node .\gen_keypair.mjs
```

It creates `../id.json` (project root) and prints the public key.

## 3) Safety (so you don’t accidentally spend)

**Default is safe**:

- executor `.env`: `EXECUTOR_DRY_RUN=true` and `EXECUTOR_LIVE=false`
- python `config/control.yaml`: `trading.dry_run=true`

To go live, you must explicitly do BOTH:

- executor: `EXECUTOR_DRY_RUN=false` **and** `EXECUTOR_LIVE=true`
- python: `trading.dry_run=false`

Then restart the executor.

## 4) How the bot decides to trade

### Filter pipeline (Python)

`main.py` discovers new Pump.fun mints via Helius and builds a token feature dict.
Then:

1) **Enrichment**: calls executor `GET /state?mint=...` and fills:
   - `pump_curve_real_sol_reserves` (+ lamports)
2) **Decision**: applies rules from `config/control.yaml`:
   - `WATCH` / `SKIP` (+ reason)
3) **Trading hook** (if enabled):
   - **spray mode**: can buy even when decision is `PENDING(no_pairs)` or `SKIP(liq_lt_min)` etc (config allow-list)
   - **filter mode**: buys only on `WATCH`
4) **Exit**:
   - time-based (`hold_seconds`)
   - optional TP/SL using `GET /quote` (sell-all estimate)

### Important knobs (`config/control.yaml`)

- `trading.mode`: `spray` or `filter`
- `filter.layers.runner_gate.min_bonding_curve_real_sol`: minimal Pump.fun “real SOL” in curve
- `trading.buy_amount_sol`: SOL per entry
- `trading.hold_seconds`: time to hold before sell
- `trading.take_profit_pct` / `trading.stop_loss_pct` and `trading.sell_mode: tp_sl_time`

## Windows firewall popup (Node)

When you start the executor, Windows may ask to allow Node.js networking.
Because the executor binds to `127.0.0.1` (localhost), it’s safe to:

- ✅ allow on **Private networks**
- ❌ keep blocked on **Public networks**

The Python bot talks to it locally.
