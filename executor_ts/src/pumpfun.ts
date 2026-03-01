import BN from "bn.js";
import {
  Connection,
  Keypair,
  PublicKey,
  Transaction,
} from "@solana/web3.js";
import {
  TOKEN_2022_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
  getAccount,
  getAssociatedTokenAddressSync,
} from "@solana/spl-token";

/**
 * NOTE (important):
 * Newer versions of @pump-fun/pump-sdk export **ONLY a default object**.
 * Dynamic import will show keys: ["default"].
 * So we normalize everything through `getPumpBase()`.
 */
import * as PumpNS from "@pump-fun/pump-sdk";

type PumpBase = Record<string, any>;

function getPumpBase(): PumpBase {
  // ESM import namespace will usually be { default: {...} }
  // Older builds might have named exports; merge both safely.
  const anyNs: any = PumpNS as any;
  const def: any = anyNs?.default ?? {};
  return { ...def, ...anyNs };
}

function exportsHint(): string {
  try {
    const base = getPumpBase();
    return Object.keys(base).sort().join(",");
  } catch {
    return "";
  }
}

function asBN(v: bigint | number | string | BN): BN {
  if (BN.isBN(v)) return v as BN;
  if (typeof v === "bigint") return new BN(v.toString());
  if (typeof v === "number") return new BN(Math.trunc(v).toString());
  return new BN(String(v));
}

async function detectTokenProgram(connection: Connection, mint: PublicKey): Promise<PublicKey> {
  const info = await connection.getAccountInfo(mint, "processed");
  const owner = info?.owner;
  if (owner?.equals(TOKEN_2022_PROGRAM_ID)) return TOKEN_2022_PROGRAM_ID;
  return TOKEN_PROGRAM_ID;
}



function pickPdaPubkey(r: any): PublicKey | null {
  if (!r) return null;
  if (r instanceof PublicKey) return r;
  if (Array.isArray(r) && r[0] instanceof PublicKey) return r[0];
  if (r.publicKey instanceof PublicKey) return r.publicKey;
  if (r.pda instanceof PublicKey) return r.pda;
  if (typeof r === "string") {
    try { return new PublicKey(r); } catch { return null; }
  }
  return null;
}

function tryCall(fn: any, args: any[]): any {
  try { return fn(...args); } catch { return undefined; }
}

function deriveBondingCurvePda(base: any, mint: PublicKey, tokenProgram: PublicKey): PublicKey | null {
  const fn = base?.bondingCurvePda;
  if (typeof fn !== 'function') return null;

  const attempts = [
    () => tryCall(fn, [mint]),
    () => tryCall(fn, [mint, tokenProgram]),
    () => tryCall(fn, [{ mint, tokenProgram }]),
    () => tryCall(fn, [{ mint }]),
  ];

  for (const a of attempts) {
    const pk = pickPdaPubkey(a());
    if (pk) return pk;
  }
  return null;
}

async function getAccountInfoSafe(connection: Connection, pubkey: PublicKey | null): Promise<any | null> {
  if (!pubkey) return null;
  try {
    return await connection.getAccountInfo(pubkey, 'processed');
  } catch {
    return null;
  }
}
type SdkCombo = {
  online: any;
  offline: any;
  fetchGlobal: () => Promise<any>;
  fetchFeeConfig: () => Promise<any | null>;
  fetchBuyState: (mint: PublicKey, user: PublicKey, tokenProgram: PublicKey) => Promise<any>;
  fetchSellState: (mint: PublicKey, user: PublicKey, tokenProgram: PublicKey) => Promise<any>;
};

const SDK_CACHE = new Map<string, SdkCombo>();


const CACHE_TTL_MS = Number(process.env.PUMP_CACHE_MS ?? "1500");
const STATE_CACHE_TTL_MS = Number(process.env.PUMP_STATE_CACHE_MS ?? "400");

type CacheEntry<T> = { ts: number; v: T };
const GLOBAL_CACHE = new Map<string, CacheEntry<any>>();
const FEECFG_CACHE = new Map<string, CacheEntry<any | null>>();
const STATE_CACHE = new Map<string, CacheEntry<any>>();
const BC_CACHE = new Map<string, CacheEntry<any>>();

function getCached<T>(m: Map<string, CacheEntry<T>>, key: string, ttl: number): T | undefined {
  const e = m.get(key);
  if (!e) return undefined;
  if (Date.now() - e.ts > ttl) {
    m.delete(key);
    return undefined;
  }
  return e.v;
}

function setCached<T>(m: Map<string, CacheEntry<T>>, key: string, v: T) {
  m.set(key, { ts: Date.now(), v });
}

function bnToStr(x: any): string {
  try {
    if (x && typeof x.toString === "function") return x.toString();
    return String(x ?? "0");
  } catch {
    return "0";
  }
}

function lamportsStrToSol(lamportsStr: string): number {
  try {
    // Precision is not critical for filters; Python can recompute from lamports anyway.
    return Number(lamportsStr) / 1e9;
  } catch {
    return 0;
  }
}


function getSdk(connection: Connection): SdkCombo {
  const key = (connection as any).rpcEndpoint ?? "rpc";
  const cached = SDK_CACHE.get(key);
  if (cached) return cached;

  const base = getPumpBase();

  const OnlinePumpSdk = base.OnlinePumpSdk;
  const PumpSdk = base.PumpSdk;
  const PUMP_SDK = base.PUMP_SDK;

  if (typeof OnlinePumpSdk !== "function") {
    throw new Error(
      `Pump SDK init failed: OnlinePumpSdk missing. exportsHint=${exportsHint()}`
    );
  }

  // Offline SDK instance: prefer exported singleton (PUMP_SDK), else construct PumpSdk.
  const offline =
    (PUMP_SDK && typeof PUMP_SDK === "object") ? PUMP_SDK
    : (typeof PumpSdk === "function" ? new PumpSdk() : null);

  if (!offline) {
    throw new Error(
      `Pump SDK init failed: no offline SDK. exportsHint=${exportsHint()}`
    );
  }

  const online = new OnlinePumpSdk(connection);

  
const combo: SdkCombo = {
  online,
  offline,
  fetchGlobal: async () => {
    const ck = key + ":global";
    const c = getCached(GLOBAL_CACHE, ck, CACHE_TTL_MS);
    if (c) return c;
    const v = await online.fetchGlobal();
    setCached(GLOBAL_CACHE, ck, v);
    return v;
  },
  fetchFeeConfig: async () => {
    const ck = key + ":feecfg";
    const c = getCached(FEECFG_CACHE, ck, CACHE_TTL_MS);
    if (c !== undefined) return c;
    try {
      const v = await online.fetchFeeConfig();
      setCached(FEECFG_CACHE, ck, v);
      return v;
    } catch {
      setCached(FEECFG_CACHE, ck, null);
      return null;
    }
  },
  fetchBuyState: async (mint: PublicKey, user: PublicKey, tokenProgram: PublicKey) => {
    const ck = `${key}:buy:${mint.toBase58()}:${user.toBase58()}:${tokenProgram.toBase58()}`;
    const c = getCached(STATE_CACHE, ck, STATE_CACHE_TTL_MS);
    if (c) return c;
    const v = await online.fetchBuyState(mint, user, tokenProgram);
    setCached(STATE_CACHE, ck, v);
    return v;
  },
  fetchSellState: async (mint: PublicKey, user: PublicKey, tokenProgram: PublicKey) => {
    const ck = `${key}:sell:${mint.toBase58()}:${user.toBase58()}:${tokenProgram.toBase58()}`;
    const c = getCached(STATE_CACHE, ck, STATE_CACHE_TTL_MS);
    if (c) return c;
    const v = await online.fetchSellState(mint, user, tokenProgram);
    setCached(STATE_CACHE, ck, v);
    return v;
  },
};


  SDK_CACHE.set(key, combo);
  return combo;
}

async function getTokenBalanceBN(
  connection: Connection,
  mint: PublicKey,
  owner: PublicKey,
  tokenProgram: PublicKey
): Promise<BN> {
  const ata = getAssociatedTokenAddressSync(mint, owner, true, tokenProgram);
  try {
    const acct = await getAccount(connection, ata, "processed", tokenProgram);
    return new BN(acct.amount.toString());
  } catch {
    return new BN(0);
  }
}

export async function quoteBuy(
  connection: Connection,
  payerPublicKey: PublicKey,
  mint: PublicKey,
  solInLamports: bigint,
  slippageBps: number
): Promise<{ sol_in_lamports: string; token_out: string; slippage_bps: number }> {
  const base = getPumpBase();
  const fn = base.getBuyTokenAmountFromSolAmount;
  if (typeof fn !== "function") {
    throw new Error(
      `pump-sdk missing getBuyTokenAmountFromSolAmount. exportsHint=${exportsHint()}`
    );
  }

  const { fetchGlobal, fetchFeeConfig, fetchBuyState } = getSdk(connection);
  const tokenProgram = await detectTokenProgram(connection, mint);

  const [global, feeConfig, st] = await Promise.all([
    fetchGlobal(),
    fetchFeeConfig(),
    fetchBuyState(mint, payerPublicKey, tokenProgram),
  ]);

  const solAmount = asBN(solInLamports);
  const bondingCurve = st?.bondingCurve ?? null;
  const mintSupply = bondingCurve?.tokenTotalSupply ?? global?.tokenTotalSupply ?? null;

  const tokenOut: BN = fn({
    global,
    feeConfig,
    mintSupply,
    bondingCurve,
    amount: solAmount,
  });

  return {
    sol_in_lamports: solAmount.toString(10),
    token_out: tokenOut.toString(10),
    slippage_bps: slippageBps,
  };
}

/**
 * Fetch Pump.fun bonding-curve reserves/state for a mint.
 * Used by Python enricher via executor GET /state.
 */
export async function fetchBondingCurveState(
  connection: Connection,
  mint: PublicKey
): Promise<Record<string, any>> {
  const key = (connection as any).rpcEndpoint ?? "rpc";
  const ck = `${key}:bc:${mint.toBase58()}`;
  const cached = getCached(BC_CACHE, ck, STATE_CACHE_TTL_MS);
  if (cached) return cached;

  const sdk = getSdk(connection);
  const bc: any = await sdk.online.fetchBondingCurve(mint);

  const realSolLamports = bnToStr(bc?.realSolReserves);
  const virtSolLamports = bnToStr(bc?.virtualSolReserves);

  const out: Record<string, any> = {
    realSolReservesLamports: realSolLamports,
    virtualSolReservesLamports: virtSolLamports,
    realSolReservesSol: lamportsStrToSol(realSolLamports),
    virtualSolReservesSol: lamportsStrToSol(virtSolLamports),

    realTokenReserves: bnToStr(bc?.realTokenReserves),
    virtualTokenReserves: bnToStr(bc?.virtualTokenReserves),
    tokenTotalSupply: bnToStr(bc?.tokenTotalSupply),

    complete: Boolean(bc?.complete),
    creator: bc?.creator?.toBase58 ? bc.creator.toBase58() : undefined,
    isMayhemMode: Boolean(bc?.isMayhemMode),
  };

  setCached(BC_CACHE, ck, out);
  return out;
}

export async function quoteSellAll(
  connection: Connection,
  payerPublicKey: PublicKey,
  mint: PublicKey,
  slippageBps: number
): Promise<{ token_in: string; sol_out: string; slippage_bps: number }> {
  const base = getPumpBase();
  const fn = base.getSellSolAmountFromTokenAmount;
  if (typeof fn !== "function") {
    throw new Error(
      `pump-sdk missing getSellSolAmountFromTokenAmount. exportsHint=${exportsHint()}`
    );
  }

  const { fetchGlobal, fetchFeeConfig, fetchSellState } = getSdk(connection);
  const tokenProgram = await detectTokenProgram(connection, mint);

  // If user has no ATA / balance, return zeros (don't call fetchSellState which throws).
  const tokenAmount = await getTokenBalanceBN(connection, mint, payerPublicKey, tokenProgram);
  if (tokenAmount.isZero()) {
    return { token_in: "0", sol_out: "0", slippage_bps: slippageBps };
  }

  const [global, feeConfig, st] = await Promise.all([
    fetchGlobal(),
    fetchFeeConfig(),
    fetchSellState(mint, payerPublicKey, tokenProgram),
  ]);

  const bondingCurve = st?.bondingCurve;
  const mintSupply: BN = bondingCurve?.tokenTotalSupply ?? global?.tokenTotalSupply;
  const solOut: BN = fn({
    global,
    feeConfig,
    mintSupply,
    bondingCurve,
    amount: tokenAmount,
  });

  return {
    token_in: tokenAmount.toString(10),
    sol_out: solOut.toString(10),
    slippage_bps: slippageBps,
  };
}

// Paper-trading / risk engine quotes: estimate SOL out for selling an explicit token amount.
// This does NOT require the wallet to actually hold the tokens (no balance check).
export async function quoteSellTokenAmount(
  connection: Connection,
  payerPublicKey: PublicKey,
  mint: PublicKey,
  tokenIn: bigint,
  slippageBps: number
): Promise<{ token_in: string; sol_out: string; slippage_bps: number }> {
  const base = getPumpBase();
  const fn = base.getSellSolAmountFromTokenAmount;
  if (typeof fn !== "function") {
    throw new Error(
      `pump-sdk missing getSellSolAmountFromTokenAmount. exportsHint=${exportsHint()}`
    );
  }

  const { fetchGlobal, fetchFeeConfig, fetchBuyState } = getSdk(connection);
  const tokenProgram = await detectTokenProgram(connection, mint);

  const [global, feeConfig, st] = await Promise.all([
    fetchGlobal(),
    fetchFeeConfig(),
    // Use buy state: it tends to be available even if the user's ATA doesn't exist yet.
    fetchBuyState(mint, payerPublicKey, tokenProgram),
  ]);

  const bondingCurve = st?.bondingCurve ?? null;
  if (!bondingCurve) throw new Error("bondingCurve missing (cannot quote sell)");
  const mintSupply: BN = bondingCurve?.tokenTotalSupply ?? global?.tokenTotalSupply;

  const tokenAmount = asBN(tokenIn);
  const solOut: BN = fn({
    global,
    feeConfig,
    mintSupply,
    bondingCurve,
    amount: tokenAmount,
  });

  return {
    token_in: tokenAmount.toString(10),
    sol_out: solOut.toString(10),
    slippage_bps: slippageBps,
  };
}

export async function buildBuyTx(
  connection: Connection,
  payer: Keypair,
  mint: PublicKey,
  solInLamports: bigint,
  slippageBps: number
): Promise<Transaction> {
  const base = getPumpBase();
  const quoteFn = base.getBuyTokenAmountFromSolAmount;
  if (typeof quoteFn !== "function") {
    throw new Error(
      `pump-sdk missing getBuyTokenAmountFromSolAmount. exportsHint=${exportsHint()}`
    );
  }

  const { offline, fetchGlobal, fetchFeeConfig, fetchBuyState } = getSdk(connection);
  const tokenProgram = await detectTokenProgram(connection, mint);

  const [global, feeConfig, st] = await Promise.all([
    fetchGlobal(),
    fetchFeeConfig(),
    fetchBuyState(mint, payer.publicKey, tokenProgram),
  ]);

  const solAmount = asBN(solInLamports);
  const bondingCurve = st?.bondingCurve ?? null;
  const mintSupply = bondingCurve?.tokenTotalSupply ?? global?.tokenTotalSupply ?? null;

  const tokenOut: BN = quoteFn({
    global,
    feeConfig,
    mintSupply,
    bondingCurve,
    amount: solAmount,
  });

  const base2 = getPumpBase();
  const curvePda = deriveBondingCurvePda(base2, mint, tokenProgram);
  const bondingCurveAccountInfo = await getAccountInfoSafe(connection, curvePda) ?? st?.bondingCurveAccountInfo ?? null;

  const userAta = getAssociatedTokenAddressSync(mint, payer.publicKey, true, tokenProgram);
  const associatedUserAccountInfo = await getAccountInfoSafe(connection, userAta) ?? st?.associatedUserAccountInfo ?? null;

  const slippagePct = Math.max(0.1, slippageBps / 100);

  if (typeof offline.buyInstructions !== "function") {
    throw new Error(
      `offline SDK missing buyInstructions(). keys=${Object.keys(offline).join(",")}`
    );
  }

  // Build args carefully: some SDK builds validate types and reject explicit nulls.
  const args: any = {
    global,
    mint,
    user: payer.publicKey,
    amount: tokenOut,
    solAmount,
    slippage: slippagePct,
    tokenProgram,
  };
  if (st?.bondingCurve) args.bondingCurve = st.bondingCurve;
  if (bondingCurveAccountInfo) args.bondingCurveAccountInfo = bondingCurveAccountInfo;
  if (associatedUserAccountInfo) args.associatedUserAccountInfo = associatedUserAccountInfo;

  // Some SDK versions require fee config during instruction build.
  if (feeConfig) {
    if (feeConfig.feeConfig) args.feeConfig = feeConfig.feeConfig;
    else args.feeConfig = feeConfig;
    if (feeConfig.feeConfigAccountInfo) args.feeConfigAccountInfo = feeConfig.feeConfigAccountInfo;
    if (feeConfig.accountInfo && !args.feeConfigAccountInfo) args.feeConfigAccountInfo = feeConfig.accountInfo;
  }

  // Extra context if state didn't include needed infos
  if (!args.bondingCurveAccountInfo) {
    throw new Error("Buy state missing bondingCurveAccountInfo (cannot build tx)");
  }

  let ixs: any[];
  try {
    ixs = await offline.buyInstructions(args);
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    throw new Error(`buyInstructions failed: ${msg}`);
  }

  const tx = new Transaction();
  for (const ix of ixs) tx.add(ix);

  // Attach quote-like metadata so callers (executor) can return it without another RPC-heavy /quote call.
  // NOTE: token_out is in raw token units (base units), consistent with /quote.
  (tx as any).__quote = {
    sol_in_lamports: solAmount.toString(10),
    token_out: tokenOut.toString(10),
    slippage_bps: slippageBps,
  };

  const bh = await connection.getLatestBlockhash("confirmed");
  tx.recentBlockhash = bh.blockhash;
  tx.feePayer = payer.publicKey;
  tx.sign(payer);
  return tx;
}

export async function buildSellTx(
  connection: Connection,
  payer: Keypair,
  mint: PublicKey,
  slippageBps: number
): Promise<Transaction> {
  const base = getPumpBase();
  const quoteFn = base.getSellSolAmountFromTokenAmount;
  if (typeof quoteFn !== "function") {
    throw new Error(
      `pump-sdk missing getSellSolAmountFromTokenAmount. exportsHint=${exportsHint()}`
    );
  }

  const { offline, fetchGlobal, fetchFeeConfig, fetchSellState } = getSdk(connection);
  const tokenProgram = await detectTokenProgram(connection, mint);

  // Get balance first; if none, fail early (don't call fetchSellState which throws).
  const tokenAmount = await getTokenBalanceBN(connection, mint, payer.publicKey, tokenProgram);
  if (tokenAmount.isZero()) throw new Error("No token balance to sell");

  const [global, feeConfig, st] = await Promise.all([
    fetchGlobal(),
    fetchFeeConfig(),
    fetchSellState(mint, payer.publicKey, tokenProgram),
  ]);

  const bondingCurve = st?.bondingCurve;
  const mintSupply: BN = bondingCurve?.tokenTotalSupply ?? global?.tokenTotalSupply;
  const expectedSolOut: BN = quoteFn({
    global,
    feeConfig,
    mintSupply,
    bondingCurve,
    amount: tokenAmount,
  });

  const base2 = getPumpBase();
  const curvePda = deriveBondingCurvePda(base2, mint, tokenProgram);
  const bondingCurveAccountInfo = await getAccountInfoSafe(connection, curvePda) ?? st?.bondingCurveAccountInfo ?? null;

  const slippagePct = Math.max(0.1, slippageBps / 100);

  if (typeof offline.sellInstructions !== "function") {
    throw new Error(
      `offline SDK missing sellInstructions(). keys=${Object.keys(offline).join(",")}`
    );
  }

  const args: any = {
    global,
    mint,
    user: payer.publicKey,
    amount: tokenAmount,
    solAmount: expectedSolOut,
    slippage: slippagePct,
    tokenProgram,
    mayhemMode: Boolean(st?.bondingCurve?.isMayhemMode),
  };
  if (st?.bondingCurve) args.bondingCurve = st.bondingCurve;
  if (bondingCurveAccountInfo) args.bondingCurveAccountInfo = bondingCurveAccountInfo;

  if (feeConfig) {
    if (feeConfig.feeConfig) args.feeConfig = feeConfig.feeConfig;
    else args.feeConfig = feeConfig;
    if (feeConfig.feeConfigAccountInfo) args.feeConfigAccountInfo = feeConfig.feeConfigAccountInfo;
    if (feeConfig.accountInfo && !args.feeConfigAccountInfo) args.feeConfigAccountInfo = feeConfig.accountInfo;
  }

  if (!args.bondingCurveAccountInfo) {
    throw new Error("Sell state missing bondingCurveAccountInfo (cannot build tx)");
  }

  let ixs: any[];
  try {
    ixs = await offline.sellInstructions(args);
  } catch (e: any) {
    const msg = e?.message ? String(e.message) : String(e);
    throw new Error(`sellInstructions failed: ${msg}`);
  }

  const tx = new Transaction();
  for (const ix of ixs) tx.add(ix);

  const bh = await connection.getLatestBlockhash("confirmed");
  tx.recentBlockhash = bh.blockhash;
  tx.feePayer = payer.publicKey;
  tx.sign(payer);
  return tx;
}

export async function buildTipTx(
  connection: Connection,
  payer: Keypair,
  tipAccounts: PublicKey[],
  tipLamports: number
): Promise<Transaction> {
  const { SystemProgram } = await import("@solana/web3.js");
  if (!tipAccounts?.length) throw new Error("No tip accounts");
  const to = tipAccounts[0];

  const tx = new Transaction().add(
    SystemProgram.transfer({
      fromPubkey: payer.publicKey,
      toPubkey: to,
      lamports: Math.trunc(tipLamports),
    })
  );

  const bh = await connection.getLatestBlockhash("confirmed");
  tx.recentBlockhash = bh.blockhash;
  tx.feePayer = payer.publicKey;
  tx.sign(payer);
  return tx;
}
