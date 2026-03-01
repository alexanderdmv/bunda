import fs from "node:fs";
import path from "node:path";
import express from "express";
import dotenv from "dotenv";
import {
  Connection,
  Keypair,
  PublicKey,
  Transaction,
  VersionedTransaction,
} from "@solana/web3.js";

import { OrderRequestSchema } from "./types.js";
import { getTipAccounts, sendBundle, type JitoConfig } from "./jito.js";
import {
  buildBuyTx,
  buildSellTx,
  buildTipTx,
  fetchBondingCurveState,
  quoteBuy,
  quoteSellAll,
  quoteSellTokenAmount,
} from "./pumpfun.js";

dotenv.config();

function requireEnv(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (!v || v.trim().length === 0) throw new Error(`Missing env ${name}`);
  return v;
}

function maskRpc(url: string): string {
  // Avoid leaking API keys in /health
  try {
    const u = new URL(url);
    if (u.searchParams.has("api-key")) u.searchParams.set("api-key", "***");
    return u.toString();
  } catch {
    return url.replace(/api-key=[^&]+/i, "api-key=***");
  }
}

function apiKeyTail(url: string): string | null {
  // Return last 4 chars of the api-key query param (or null). Helpful to verify which key is used.
  try {
    const u = new URL(url);
    const k = u.searchParams.get("api-key");
    if (!k) return null;
    const s = String(k);
    if (s.length <= 4) return s;
    return s.slice(-4);
  } catch {
    const m = url.match(/api-key=([^&]+)/i);
    if (!m) return null;
    const s = String(m[1] ?? "");
    if (!s) return null;
    if (s.length <= 4) return s;
    return s.slice(-4);
  }
}

function readTextIfExists(p: string): string | null {
  try {
    if (!fs.existsSync(p)) return null;
    return fs.readFileSync(p, "utf8");
  } catch {
    return null;
  }
}

function parseControlTradingDryRun(): boolean | undefined {
  // Executor is usually started from ./executor_ts, so control.yaml lives at ../config/control.yaml
  const p = process.env.CONTROL_YAML_PATH ?? path.resolve(process.cwd(), "..", "config", "control.yaml");
  const txt = readTextIfExists(p);
  if (!txt) return undefined;

  let inTrading = false;
  let tradingIndent = 0;
  for (const raw of txt.split(/\r?\n/)) {
    const noComment = raw.split("#")[0];
    if (!noComment.trim()) continue;
    const indent = (noComment.match(/^\s*/)?.[0]?.length ?? 0);
    const line = noComment.trim();

    if (/^trading\s*:\s*$/.test(line)) {
      inTrading = true;
      tradingIndent = indent;
      continue;
    }
    if (inTrading && indent <= tradingIndent) {
      inTrading = false;
    }
    if (inTrading && /^dry_run\s*:\s*/.test(line)) {
      const v = line.split(":").slice(1).join(":").trim().toLowerCase();
      if (v.startsWith("true")) return true;
      if (v.startsWith("false")) return false;
    }
  }
  return undefined;
}

function parseSecretsHeliusKey(): { executor?: string; python?: string; fallback?: string } {
  const p = process.env.SECRETS_YAML_PATH ?? path.resolve(process.cwd(), "..", "config", "secrets.yaml");
  const txt = readTextIfExists(p);
  if (!txt) return {};

  let inHelius = false;
  let heliusIndent = 0;
  let executor: string | undefined;
  let python: string | undefined;
  let fallback: string | undefined;

  for (const raw of txt.split(/\r?\n/)) {
    const noComment = raw.split("#")[0];
    if (!noComment.trim()) continue;
    const indent = (noComment.match(/^\s*/)?.[0]?.length ?? 0);
    const line = noComment.trim();

    if (/^helius\s*:\s*$/.test(line)) {
      inHelius = true;
      heliusIndent = indent;
      continue;
    }
    if (inHelius && indent <= heliusIndent) inHelius = false;
    if (!inHelius) continue;

    const m = line.match(/^([a-zA-Z0-9_]+)\s*:\s*(.*)$/);
    if (!m) continue;
    const k = m[1];
    let v = (m[2] ?? "").trim();
    if (!v) continue;
    // strip quotes
    v = v.replace(/^"(.*)"$/, "$1").replace(/^'(.*)'$/, "$1");
    if (!v) continue;

    if (k === "api_key_executor") executor = v;
    else if (k === "api_key_python") python = v;
    else if (k === "api_key") fallback = v;
  }
  return { executor, python, fallback };
}

function resolveRpcUrl(): string {
  const envUrl = process.env.SOL_RPC_URL ?? process.env.RPC_URL ?? process.env.SOLANA_RPC_URL;
  const keys = parseSecretsHeliusKey();
  const key = process.env.HELIUS_API_KEY_EXECUTOR ?? keys.executor ?? process.env.HELIUS_API_KEY ?? keys.fallback;

  const isHelius = (u: string) => /helius-rpc\.com/i.test(u);

  if (envUrl && envUrl.trim()) {
    const u = envUrl.trim();
    if (!isHelius(u) || /api-key=/i.test(u) || !key) return u;
    try {
      const url = new URL(u);
      if (!url.searchParams.has("api-key")) url.searchParams.set("api-key", key);
      return url.toString();
    } catch {
      return u.includes("?") ? `${u}&api-key=${key}` : `${u}/?api-key=${key}`;
    }
  }

  if (key && String(key).trim()) {
    return `https://mainnet.helius-rpc.com/?api-key=${String(key).trim()}`;
  }
  throw new Error(
    "Missing SOL_RPC_URL (or HELIUS_API_KEY_EXECUTOR / secrets.yaml helius.api_key_executor)."
  );
}


const HOST = process.env.EXECUTOR_HOST ?? "127.0.0.1";
const PORT = Number(process.env.EXECUTOR_PORT ?? "8787");
const controlDryRun = parseControlTradingDryRun();
const DEFAULT_DRY_RUN =
  process.env.EXECUTOR_DRY_RUN !== undefined
    ? (process.env.EXECUTOR_DRY_RUN ?? "true").toLowerCase() !== "false"
    : (controlDryRun ?? true);
// Single-switch mode:
// - If EXECUTOR_LIVE is set, it overrides.
// - Otherwise live is enabled whenever control.yaml trading.dry_run is false.
const LIVE_ENABLED =
  process.env.EXECUTOR_LIVE !== undefined
    ? (process.env.EXECUTOR_LIVE ?? "false").toLowerCase() === "true"
    : !DEFAULT_DRY_RUN;

const KEYPAIR_PATH = process.env.KEYPAIR_PATH ?? "../id.json";
const SOL_RPC_URL = resolveRpcUrl();

// Jito
const JITO_ENABLED = (process.env.JITO_ENABLED ?? "true").toLowerCase() !== "false";
const JITO_BLOCK_ENGINE_URL =
  process.env.JITO_BLOCK_ENGINE_URL ?? "https://mainnet.block-engine.jito.wtf";
const JITO_UUID = process.env.JITO_UUID;
const JITO_TIP_LAMPORTS = Number(process.env.JITO_TIP_LAMPORTS ?? "10000");

function loadIdJsonKeypair(p: string): Keypair {
  const abs = path.isAbsolute(p) ? p : path.resolve(process.cwd(), p);
  const raw = fs.readFileSync(abs, "utf8");
  const arr = JSON.parse(raw) as number[];
  return Keypair.fromSecretKey(Uint8Array.from(arr));
}

const payer = loadIdJsonKeypair(KEYPAIR_PATH);
// ---- RPC telemetry (10s window) ----
let rpcCallsTotal = 0;
let rpcCallsWin = 0;
let rpc429Total = 0;
let rpc429Win = 0;
const RPC_WIN_MS = 10_000;

// Wrap fetch used by @solana/web3.js so we count *real* JSON-RPC HTTP calls and real 429s.
const baseFetch: typeof fetch = (globalThis.fetch as any).bind(globalThis);
const wrappedFetch: typeof fetch = async (input: any, init?: any) => {
  rpcCallsTotal += 1;
  rpcCallsWin += 1;
  const res: any = await baseFetch(input, init);
  if (res && typeof res.status === "number" && res.status === 429) {
    rpc429Total += 1;
    rpc429Win += 1;
  }
  return res;
};

const connection = new Connection(SOL_RPC_URL, {
  commitment: "confirmed",
  fetch: wrappedFetch as any,
});

// Emit a compact line every 10 seconds
setInterval(() => {
  const cps = rpcCallsWin / (RPC_WIN_MS / 1000);
  console.log(`[rpc] 10s calls=${rpcCallsWin} (~${cps.toFixed(1)}/s) 429=${rpc429Win} totals calls=${rpcCallsTotal} 429=${rpc429Total}`);
  rpcCallsWin = 0;
  rpc429Win = 0;
}, RPC_WIN_MS);

function asPubkey(s: string): PublicKey {
  return new PublicKey(s);
}

async function simulateTx(tx: Transaction | VersionedTransaction) {
  // Some RPC providers are picky about simulateTransaction config fields.
  // We already build the tx with a fresh blockhash, so keep config minimal.
  const sim = await (connection as any).simulateTransaction(tx, {
    sigVerify: false,
  });
  return sim?.value ?? sim;
}

/**
 * pumpfun.ts exports in this project have changed a few times:
 * - some versions use positional args: (connection, payer, mint, ...)
 * - some versions use a single options object: ({ connection, payer, mint, ... })
 * This adapter supports both without you having to manually align versions.
 */
function callMaybeOpts(fn: any, positional: any[], opts: any) {
  return fn.length <= 1 ? fn(opts) : fn(...positional);
}

const app = express();
app.use(express.json({ limit: "2mb" }));

app.get("/health", (_req, res) => {
  return res.json({
    ok: true,
    pubkey: payer.publicKey.toBase58(),
    dry_run_default: DEFAULT_DRY_RUN,
    live_enabled: LIVE_ENABLED,
    jito_enabled: JITO_ENABLED,
    rpc: maskRpc(SOL_RPC_URL),
    rpc_key_tail: apiKeyTail(SOL_RPC_URL),
    rpc_calls_total: rpcCallsTotal,
    rpc_calls_last_10s: rpcCallsWin,
    rpc_calls_per_sec_last_10s: Number((rpcCallsWin / (RPC_WIN_MS / 1000)).toFixed(2)),
    rpc_429_total: rpc429Total,
    rpc_429_last_10s: rpc429Win,
  });
});

app.get("/state", async (req, res) => {
  try {
    const mintStr = String(req.query.mint ?? "");
    if (!mintStr) return res.status(400).json({ ok: false, message: "missing mint" });
    const mint = asPubkey(mintStr);
    const state = await fetchBondingCurveState(connection, mint);
    return res.json({ ok: true, state });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return res.status(500).json({ ok: false, message: msg });
  }
});

app.get("/quote", async (req, res) => {
  try {
    const mintStr = String(req.query.mint ?? "");
    if (!mintStr) return res.status(400).json({ ok: false, message: "missing mint" });
    const mint = asPubkey(mintStr);

    const side = String(req.query.side ?? "buy").toLowerCase();
    const slipBps = Number(
      req.query.slippage_bps ??
        Math.round(Number(req.query.slippage ?? "1") * 100)
    );
    const slippageBps = Math.max(1, Math.min(10_000, Math.round(slipBps)));

    if (side === "buy") {
      const solIn = Number(req.query.amount_in ?? req.query.sol_in ?? "0");
      if (!solIn || solIn <= 0)
        return res.status(400).json({ ok: false, message: "missing amount_in" });
      const solLamports = BigInt(Math.round(solIn * 1e9));

      const q = await callMaybeOpts(
        quoteBuy as any,
        [connection, payer.publicKey, mint, solLamports, slippageBps],
        { connection, payer: payer.publicKey, mint, solInLamports: solLamports, slippageBps }
      );
      return res.json({ ok: true, quote: q });
    } else {
      const tokenInStr = String(req.query.amount_in ?? req.query.token_in ?? "0");
      const tokenInStrTrim = tokenInStr.trim();
      // If amount_in > 0: quote explicit token amount (paper trading).
      if (tokenInStrTrim && tokenInStrTrim !== "0") {
        const tokenIn = BigInt(tokenInStrTrim);
        const q = await callMaybeOpts(
          quoteSellTokenAmount as any,
          [connection, payer.publicKey, mint, tokenIn, slippageBps],
          { connection, payer: payer.publicKey, mint, tokenIn, slippageBps }
        );
        return res.json({ ok: true, quote: q });
      }
      // Otherwise quote SELL ALL based on wallet balance.
      const q = await callMaybeOpts(
        quoteSellAll as any,
        [connection, payer.publicKey, mint, slippageBps],
        { connection, payer: payer.publicKey, mint, slippageBps }
      );
      return res.json({ ok: true, quote: q });
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return res.status(500).json({ ok: false, message: msg });
  }
});

app.post("/trade", async (req, res) => {
  try {
    const parsed = OrderRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      return res
        .status(400)
        .json({ ok: false, message: parsed.error.message, dry_run: true });
    }
    const o = parsed.data;

    const side = String((o as any).side ?? "").toLowerCase();

    if (side !== "buy" && side !== "sell") {
      return res.status(400).json({ ok: false, dry_run: true, message: "side must be buy|sell" });
    }
    const mint = asPubkey(o.mint);

    const amountInSol = Number((o as any).amount_in ?? (o as any).solIn ?? 0);
    const slippageBps = Number(
      (o as any).slippage_bps ?? Math.round(Number((o as any).slippage ?? 1) * 100)
    );
    const useJito = Boolean((o as any).useJito ?? true);
    const simulate = Boolean((o as any).simulate ?? false);

    const bodyDryRun = (o as any).dry_run;
    const reqDryRun =
      typeof bodyDryRun === "boolean" ? bodyDryRun : (o as any).dryRun;
    const dryRun = typeof reqDryRun === "boolean" ? reqDryRun : DEFAULT_DRY_RUN;

    if (side === "buy" && (!amountInSol || amountInSol <= 0)) {
      return res
        .status(400)
        .json({ ok: false, dry_run: true, message: "buy requires amount_in (SOL)" });
    }

    const solLamports = BigInt(Math.round(amountInSol * 1e9));

    let tx: Transaction;
    let quoteMeta: any = undefined;
    if (side === "buy") {
      tx = await callMaybeOpts(
        buildBuyTx as any,
        [connection, payer, mint, solLamports, slippageBps],
        { connection, payer, mint, amountInLamports: solLamports, slippageBps }
      );
      quoteMeta = (tx as any).__quote;
    } else if (side === "sell") {
      tx = await callMaybeOpts(
        buildSellTx as any,
        [connection, payer, mint, slippageBps],
        { connection, payer, mint, slippageBps }
      );
    } else {
      return res
        .status(400)
        .json({ ok: false, dry_run: true, message: "side must be buy|sell" });
    }

    
    if (simulate) {
      try {
        const sim = await simulateTx(tx);
        return res.json({ ok: true, dry_run: true, simulate: sim, quote: quoteMeta });
      } catch (e) {
        const simMsg = e instanceof Error ? e.message : String(e);
        // Best-effort: don't fail the whole request if the RPC rejects simulation.
        return res.json({
          ok: true,
          dry_run: true,
          quote: quoteMeta,
          message: "[SIMULATE_FAILED] built tx only (simulation error)",
          simulate_error: simMsg,
        });
      }
    }


    if (dryRun || !LIVE_ENABLED) {
      return res.json({
        ok: true,
        dry_run: true,
        quote: quoteMeta,
        message: "[DRY_RUN] built tx only (not sent)",
      });
    }

    if (JITO_ENABLED && useJito) {
      const jitoCfg: JitoConfig = {
        blockEngineUrl: JITO_BLOCK_ENGINE_URL,
        uuid: JITO_UUID,
        enabled: true,
      };
      const tips = await getTipAccounts(jitoCfg);

      const tipTx = await callMaybeOpts(
        buildTipTx as any,
        [connection, payer, tips, JITO_TIP_LAMPORTS],
        { connection, payer, tipAccounts: tips, tipLamports: JITO_TIP_LAMPORTS }
      );

      const sig = await sendBundle(jitoCfg, [tx, tipTx]);
      return res.json({ ok: true, dry_run: false, signature: sig, sent_via: "jito_bundle" });
    } else {
      const sig = await connection.sendTransaction(tx, [payer], { skipPreflight: false });
      return res.json({ ok: true, dry_run: false, signature: sig, sent_via: "rpc" });
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return res.status(500).json({ ok: false, dry_run: true, message: msg });
  }
});

app.listen(PORT, HOST, () => {
  console.log(`[executor] listening on http://${HOST}:${PORT}`);
  console.log(`[executor] pubkey: ${payer.publicKey.toBase58()}`);
  console.log(`[executor] dry-run default: ${DEFAULT_DRY_RUN}`);
  console.log(`[executor] live enabled: ${LIVE_ENABLED}`);
  console.log(`[executor] jito enabled: ${JITO_ENABLED} (${JITO_BLOCK_ENGINE_URL})`);
});