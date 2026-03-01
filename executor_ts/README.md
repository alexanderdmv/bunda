# Sniper Executor (TypeScript)

Local HTTP executor that can **build + (optionally) send** Pump.fun buy/sell transactions.

- Reads your Solana keypair from `id.json` (array of ints)
- Exposes:
  - `GET /health`
  - `GET /state?mint=...` (bonding-curve reserves)
  - `GET /quote?mint=...&side=buy|sell&sol_in=...` (rough expected out)
  - `POST /order` (buy / sell)
- Can submit through **Jito Bundle** (two tx: trade + tip)

## Safety defaults

The executor has two layers of safety:

- `EXECUTOR_DRY_RUN=true` (default): build/sign tx, but do **not** send
- `EXECUTOR_LIVE=false` (default): even if you set `EXECUTOR_DRY_RUN=false`, executor still **won't send** until this is `true`

## Setup

```bash
cd executor_ts
npm install
cp .env.example .env
```

Edit `.env`:

- `KEYPAIR_PATH` → path to your keypair file (root `id.json`)
- `SOL_RPC_URL` → your RPC (Helius etc.)
- `JITO_BLOCK_ENGINE_URL` → pick nearest region

## Run

```bash
npm run dev
```

## Test (dry-run)

```bash
curl http://127.0.0.1:8787/state?mint=<MINT>
```

```bash
curl http://127.0.0.1:8787/order \
  -H "Content-Type: application/json" \
  -d '{"side":"buy","venue":"pumpfun","mint":"<MINT>","amount_in":0.02,"slippage_bps":1200,"dry_run":true}'
```

When ready for live, set:

- `EXECUTOR_DRY_RUN=false`
- `EXECUTOR_LIVE=true`

Then restart the executor.
