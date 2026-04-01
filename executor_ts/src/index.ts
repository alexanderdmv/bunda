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
  SystemProgram,
  TransactionMessage,
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

import { createToken } from "./pumpfun.js";   // если нет — создадим ниже
import bs58 from "bs58";


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
const PORT = Number(process.env.EXECUTOR_PORT ?? "8790");
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

async function sendBuiltTx(
  connection: Connection,
  tx: Transaction | VersionedTransaction,
  signer?: Keypair
): Promise<string> {
  const sendOpts = { skipPreflight: false, maxRetries: 3 };

  if (tx instanceof VersionedTransaction) {
    return await connection.sendTransaction(tx, sendOpts);
  }

  // Legacy Transaction
  if (signer) {
    return await connection.sendTransaction(tx, [signer], sendOpts);
  }

  const hasAnySignature =
    Array.isArray(tx.signatures) &&
    tx.signatures.some((s: any) => !!s?.signature);

  if (!hasAnySignature) {
    throw new Error("No signers and transaction is not pre-signed");
  }

  return await connection.sendRawTransaction(tx.serialize(), sendOpts);
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
      return res.status(400).json({ ok: false, message: parsed.error.message, dry_run: true });
    }
    const o = parsed.data;
    const anyO = o as any;

    const customSecretB58 = typeof anyO.secret_b58 === "string" ? anyO.secret_b58 : undefined;
    let txSigner = payer;
    if (customSecretB58 && customSecretB58.trim().length > 0) {
      console.log(`[debug] customSecretB58 length: ${customSecretB58.length}`);
      try {
        const decoded = bs58.decode(customSecretB58);
        console.log(`[debug] decoded secret length: ${decoded.length}`);
        if (decoded.length === 64) {
          txSigner = Keypair.fromSecretKey(Uint8Array.from(decoded));
        } else if (decoded.length === 32) {
          txSigner = Keypair.fromSeed(Uint8Array.from(decoded));
        } else {
          throw new Error(`Invalid secret key size: ${decoded.length} bytes, expected 32 or 64`);
        }
      } catch (e: any) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error(`[error] Custom key load failed: ${msg}`);
        return res.status(400).json({ ok: false, message: `Invalid secret_b58: ${msg}`, dry_run: true });
      }
    }

    const side = (o.side ?? o.action ?? anyO.side ?? anyO.action) as
      | "buy"
      | "sell"
      | "transfer"
      | undefined;
    if (!side) {
      return res.status(400).json({ ok: false, message: "side/action required", dry_run: true });
    }

    const mintStr = typeof o.mint === "string" ? o.mint : undefined;
    const toStr = typeof anyO.to === "string" ? anyO.to : undefined;
    const amountIn =
      o.amount_in ??
      o.amount_sol ??
      (typeof anyO.amount === "number" ? anyO.amount : undefined);
    const bodyDryRun =
      (typeof anyO.dryRun === "boolean" ? anyO.dryRun : undefined) ??
      o.dry_run ??
      o.simulate;
    const dryRun = typeof bodyDryRun === "boolean" ? bodyDryRun : DEFAULT_DRY_RUN;

    const slippage = Number(anyO.slippage ?? 0);
    const slippageBpsInput = Number(anyO.slippageBps ?? o.slippage_bps ?? 0);
    const slippageBps = slippage > 0 ? slippage * 100 : (slippageBpsInput > 0 ? slippageBpsInput : 1500);
    const useJito = Boolean(anyO.useJito ?? JITO_ENABLED);
    const simulate = Boolean(o.simulate ?? false);

    console.log(`[trade] side=${side} dryRun=${dryRun} live=${LIVE_ENABLED ?? false}`);

    if (side === "transfer") {
      if (!toStr) {
        return res.status(400).json({ ok: false, message: "to required for transfer", dry_run: true });
      }
      if (amountIn === undefined || amountIn <= 0) {
        return res.status(400).json({ ok: false, message: "amount_in required and >0 for transfer", dry_run: true });
      }

      const toPubkey = new PublicKey(toStr);
      const amountLamports = Math.floor(amountIn * 1e9);
      const tx = await buildTransferTx(connection, txSigner, toPubkey, amountLamports);

      if (dryRun || !LIVE_ENABLED) {
        const base64Tx = Buffer.from(tx.serialize()).toString("base64");
        return res.json({ ok: true, dry_run: true, tx_base64: base64Tx });
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
          [connection, txSigner, tips, JITO_TIP_LAMPORTS],
          { connection, payer: txSigner, tipAccounts: tips, tipLamports: JITO_TIP_LAMPORTS }
        );
        const base64TxStr = Buffer.from(tx.serialize()).toString("base64");
        const base64TipStr = Buffer.from(tipTx.serialize()).toString("base64");
        const sig = await sendBundle(jitoCfg, [base64TxStr, base64TipStr]);
        return res.json({ ok: true, dry_run: false, signature: sig, sent_via: "jito_bundle" });
      }

      const sig = await sendBuiltTx(connection, tx, txSigner);
      return res.json({ ok: true, dry_run: false, signature: sig, sent_via: "rpc" });
    }

    if (side !== "buy" && side !== "sell") {
      return res.status(400).json({ ok: false, message: "side must be buy/sell/transfer", dry_run: true });
    }

    if (!mintStr) {
      return res.status(400).json({ ok: false, message: "mint required for buy/sell", dry_run: true });
    }

    const mint = new PublicKey(mintStr);
    const amountInSol = Number(amountIn ?? 0);

    if (side === "buy" && (!amountInSol || amountInSol <= 0)) {
      return res.status(400).json({ ok: false, dry_run: true, message: "buy requires amount_in (SOL)" });
    }

    const solLamports = BigInt(Math.round(amountInSol * 1e9));

    let tx: Transaction | VersionedTransaction;
    let quoteMeta: any = undefined;
    if (side === "buy") {
      tx = await callMaybeOpts(
        buildBuyTx as any,
        [connection, txSigner, mint, solLamports, slippageBps],
        { connection, payer: txSigner, mint, amountInLamports: solLamports, slippageBps }
      );
      quoteMeta = (tx as any).__quote;
    } else {
      tx = await callMaybeOpts(
        buildSellTx as any,
        [connection, txSigner, mint, slippageBps],
        { connection, payer: txSigner, mint, slippageBps }
      );
    }

    if (simulate) {
      try {
        const sim = await simulateTx(tx);
        return res.json({ ok: true, dry_run: true, simulate: sim, quote: quoteMeta });
      } catch (e) {
        const simMsg = e instanceof Error ? e.message : String(e);
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
        [connection, txSigner, tips, JITO_TIP_LAMPORTS],
        { connection, payer: txSigner, tipAccounts: tips, tipLamports: JITO_TIP_LAMPORTS }
      );

      const txBase64 = Buffer.from(tx.serialize()).toString("base64");
      const tipBase64 = Buffer.from(tipTx.serialize()).toString("base64");
      const sig = await sendBundle(jitoCfg, [txBase64, tipBase64]);
      return res.json({ ok: true, dry_run: false, signature: sig, sent_via: "jito_bundle" });
    }

    const sig = await sendBuiltTx(connection, tx, txSigner);
    return res.json({ ok: true, dry_run: false, signature: sig, sent_via: "rpc" });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("[trade] failed:", msg);
    if (e instanceof Error && e.stack) console.error(e.stack);

    const status =
      msg.includes("Bonding curve account not found") ||
      msg.includes("Mint account not found on RPC") ||
      msg.includes("No token balance to sell") ||
      msg.includes("Invalid arguments") ||
      msg.includes("Simulation failed") ||
      msg.includes("custom program error: 0x1788") ||
      msg.includes("Error Code: Overflow")
        ? 422
        : 500;

    return res.status(status).json({ ok: false, message: msg, dry_run: true });
  }
});
const rpcTelemetryTimer = setInterval(() => {
  const cps = rpcCallsWin / (RPC_WIN_MS / 1000);
  console.log(`[rpc] 10s calls=${rpcCallsWin} (~${cps.toFixed(1)}/s) 429=${rpc429Win} totals calls=${rpcCallsTotal} 429=${rpc429Total}`);
  rpcCallsWin = 0;
  rpc429Win = 0;
}, RPC_WIN_MS);
(rpcTelemetryTimer as any).unref?.();

const server = app.listen(PORT, HOST, () => {
  console.log(`[executor] listening on http://${HOST}:${PORT}`);
  console.log(`[executor] pubkey: ${payer.publicKey.toBase58()}`);
  console.log(`[executor] dry-run default: ${DEFAULT_DRY_RUN}`);
  console.log(`[executor] live enabled: ${LIVE_ENABLED}`);
  console.log(`[executor] jito enabled: ${JITO_ENABLED} (${JITO_BLOCK_ENGINE_URL})`);
});

let shuttingDown = false;
function shutdown(sig: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.error(`[fatal] ${sig} received`);
  clearInterval(rpcTelemetryTimer);
  (server as any).closeIdleConnections?.();
  (server as any).closeAllConnections?.();
  try { process.stdin.pause(); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 150).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("uncaughtException", (err) => {
  console.error("[fatal] uncaughtException:", err);
});
process.on("unhandledRejection", (reason) => {
  console.error("[fatal] unhandledRejection:", reason);
});
// ====================== TRANSFER SOL ======================
async function buildTransferTx(connection: Connection, fromKp: Keypair, to: PublicKey, amountLamports: number): Promise<VersionedTransaction> {  // Изменили тип возврата
  const ix = SystemProgram.transfer({
    fromPubkey: fromKp.publicKey,
    toPubkey: to,
    lamports: amountLamports
  });
  const bh = await connection.getLatestBlockhash("confirmed");
  const messageV0 = new TransactionMessage({
    payerKey: fromKp.publicKey,
    recentBlockhash: bh.blockhash,
    instructions: [ix]
  }).compileToV0Message();
  const tx = new VersionedTransaction(messageV0);
  tx.sign([fromKp]);
  return tx;
}
// ====================== BUNDLE LAUNCH ENDPOINT (10-20 wallets) ======================
app.post("/launch", async (req, res) => {
  try {
    const {
      name,
      symbol,
      description,
      image_path,
      wallets,
      dev_buy_sol,
      buy_amounts,
      jito_tips,
      dry_run
    } = req.body;

    const devBuySol = Math.max(0, Number(dev_buy_sol ?? 0));
    const devBuyLamports = BigInt(Math.floor(devBuySol * 1e9));
    const isDryRun = dry_run ?? (DEFAULT_DRY_RUN ?? true);

    if (isDryRun) {
      console.log(`[DRY-RUN] Симулируем bundle launch | wallets: ${wallets?.length || 0}`);
      return res.json({
        ok: true,
        mint: "DRY_RUN_MINT_" + Date.now(),
        bundle_sig: "dry-run-ok",
        anti_detect: true
      });
    }

    console.log(`[LIVE] Запускаем bundle с анти-детектом | wallets: ${wallets.length}`);

    // 1. Создаём токен
    const createTx = await createToken(connection, payer, {
      name,
      symbol,
      description,
      file: image_path
    });

    const launchMint = (createTx as any).mint || new PublicKey("11111111111111111111111111111111");

    // 2. Если Jito выключен — сначала CREATE, ждём готовности mint/state,
    //    потом строим BUY уже как обычные post-create buys (isNewToken=false).
    if (!JITO_ENABLED) {
      console.warn("[launch] JITO_ENABLED=false, using sequential RPC fallback");
      const signatures: string[] = [];

      const createSig = await sendBuiltTx(connection, createTx, payer);
      signatures.push(createSig);

      try {
        await connection.confirmTransaction(createSig, "confirmed");
      } catch {}

      const readyDeadline = Date.now() + 25_000;
      let ready = false;
      let lastReadyErr = "";
      while (Date.now() < readyDeadline) {
        try {
          const mintInfo = await connection.getAccountInfo(launchMint, "confirmed");
          if (mintInfo) {
            await fetchBondingCurveState(connection, launchMint);
            ready = true;
            break;
          }
        } catch (e: any) {
          lastReadyErr = e?.message ? String(e.message) : String(e);
        }
        await new Promise(r => setTimeout(r, 800));
      }

      if (!ready) {
        throw new Error(
          `Launch mint was created but not ready on RPC for buys: ${launchMint.toBase58()}${lastReadyErr ? ` | last=${lastReadyErr}` : ""}`
        );
      }

      if (devBuySol > 0) {
        const devBuyTx = await buildBuyTx(
          connection,
          payer,
          launchMint,
          devBuyLamports,
          1500,
          false
        );

        const devBuySig = await sendBuiltTx(connection, devBuyTx, payer);
        signatures.push(devBuySig);

        try {
          await connection.confirmTransaction(devBuySig, "confirmed");
        } catch {}

        await new Promise(r => setTimeout(r, randomInt(500, 1200)));
      }

      for (let i = 0; i < wallets.length; i++) {
        const w = wallets[i];
        const buyerKp = Keypair.fromSecretKey(bs58.decode(w.secret_b58));
        const buyAmountSol = buy_amounts && Array.isArray(buy_amounts) ? buy_amounts[i] : 0.03;

        const buyTx = await buildBuyTx(
          connection,
          buyerKp,
          launchMint,
          BigInt(Math.floor(buyAmountSol * 1e9)),
          1500,
          false
        );

        const sig = await sendBuiltTx(connection, buyTx, buyerKp);
        signatures.push(sig);

        if (i < wallets.length - 1) {
          await new Promise(r => setTimeout(r, randomInt(450, 1850)));
        }
      }

      return res.json({
        ok: true,
        mint: launchMint.toBase58(),
        bundle_sig: signatures[0] || "rpc-launch-ok",
        anti_detect: true,
        wallets_count: wallets.length,
        sent_via: "rpc_fallback",
        signatures
      });
    }

    // 3. Jito path: prebuild CREATE + buys as one bundle for the brand-new token case.
    const bundleTxs: any[] = [createTx];

    if (devBuySol > 0) {
      const devBuyTx = await buildBuyTx(
        connection,
        payer,
        launchMint,
        devBuyLamports,
        1500,
        true
      );
      bundleTxs.push(devBuyTx);
    }

    for (let i = 0; i < wallets.length; i++) {
      const w = wallets[i];
      const buyerKp = Keypair.fromSecretKey(bs58.decode(w.secret_b58));
      const buyAmountSol = buy_amounts && Array.isArray(buy_amounts) ? buy_amounts[i] : 0.03;
      const buyTx = await buildBuyTx(
        connection,
        buyerKp,
        launchMint,
        BigInt(Math.floor(buyAmountSol * 1e9)),
        1500,
        true
      );
      bundleTxs.push(buyTx);
      if (i < wallets.length - 1) {
        await new Promise(r => setTimeout(r, randomInt(450, 1850)));
      }
    }

    // 4. Jito tip
    const tipAccountsStr = await getTipAccounts({
      blockEngineUrl: JITO_BLOCK_ENGINE_URL,
      uuid: JITO_UUID
    });
    const tipAccounts = tipAccountsStr.map((a: string) => new PublicKey(a));
    const finalTip = jito_tips && Array.isArray(jito_tips) 
      ? Math.floor(jito_tips.reduce((sum: number, tip: number) => sum + tip, 0) * 1e9) 
      : 50000;
    const tipTx = await buildTipTx(connection, payer, tipAccounts, finalTip);
    bundleTxs.push(tipTx);

    // 5. Отправляем бандл
    const signedBase64: string[] = bundleTxs.map((tx: any) =>
      Buffer.from(tx.serialize()).toString("base64")
    );
    const bundleSig = await sendBundle({
      blockEngineUrl: JITO_BLOCK_ENGINE_URL,
      uuid: JITO_UUID
    }, signedBase64);

    return res.json({
      ok: true,
      mint: launchMint.toBase58(),
      bundle_sig: bundleSig,
      anti_detect: true,
      wallets_count: wallets.length,
      sent_via: "jito_bundle"
    });
  } catch (e: unknown) {
    console.error("Launch bundle error:", e);
    const msg = e instanceof Error ? e.message : String(e);
    res.status(500).json({ ok: false, error: msg });
  }
});

// Вспомогательная функция
function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}