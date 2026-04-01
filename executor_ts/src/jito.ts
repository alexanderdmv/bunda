import crypto from "node:crypto";

export type JitoConfig = {
  // Used only for Jito block-engine RPC calls
  blockEngineUrl: string;
  uuid?: string;

  // Backwards/for UI only (not used by RPC helpers)
  enabled?: boolean;
};

type RpcResponse<T> = {
  jsonrpc: string;
  id: number;
  result?: T;
  error?: { code: number; message: string; data?: unknown };
};

// Per Jito docs, tip accounts are static. Keep a built-in fallback so launch
// does not fail when getTipAccounts is globally rate limited.
const STATIC_JITO_TIP_ACCOUNTS = [
  "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
  "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
  "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
  "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
  "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
  "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
  "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
  "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
] as const;

let cachedTipAccounts: string[] | null = null;
let cachedAt = 0;
const TIP_CACHE_MS = 10 * 60 * 1000;

function bundlesEndpoint(baseUrl: string, uuid?: string): string {
  const clean = baseUrl.replace(/\/$/, "");
  const url = `${clean}/api/v1/bundles`;
  if (!uuid) return url;
  // The docs allow uuid as query parameter as well
  const sep = url.includes("?") ? "&" : "?";
  return `${url}${sep}uuid=${encodeURIComponent(uuid)}`;
}

async function jitoRpc<T>(cfg: JitoConfig, method: string, params: unknown[]): Promise<T> {
  const url = bundlesEndpoint(cfg.blockEngineUrl, cfg.uuid);
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (cfg.uuid) {
    // Docs: UUID can also be passed via x-jito-auth header
    headers["x-jito-auth"] = cfg.uuid;
  }

  const body = {
    jsonrpc: "2.0",
    id: 1,
    method,
    params
  };

  const res = await fetch(url, { method: "POST", headers, body: JSON.stringify(body) });
  const json = (await res.json()) as RpcResponse<T>;
  if (!res.ok || json.error) {
    const msg = json.error?.message ?? `HTTP ${res.status}`;
    throw new Error(`Jito RPC ${method} failed: ${msg}`);
  }
  if (json.result === undefined) throw new Error(`Jito RPC ${method} missing result`);
  return json.result;
}

export async function getTipAccounts(cfg: JitoConfig): Promise<string[]> {
  const now = Date.now();
  if (cachedTipAccounts && now - cachedAt < TIP_CACHE_MS) {
    return cachedTipAccounts;
  }

  try {
    const live = await jitoRpc<string[]>(cfg, "getTipAccounts", []);
    if (Array.isArray(live) && live.length > 0) {
      cachedTipAccounts = live;
      cachedAt = now;
      return live;
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.warn(`[jito] getTipAccounts failed, using static fallback: ${msg}`);
  }

  cachedTipAccounts = [...STATIC_JITO_TIP_ACCOUNTS];
  cachedAt = now;
  return cachedTipAccounts;
}

export async function sendBundle(cfg: JitoConfig, signedTxBase64: string[]): Promise<string> {
  // Per docs: params = [ [tx1, tx2, ...], { encoding: "base64" } ]
  return jitoRpc<string>(cfg, "sendBundle", [signedTxBase64, { encoding: "base64" }]);
}

export function computeBundleIdFromSignatures(signaturesBase58: string[]): string {
  // Jito docs: bundle id is SHA-256 hash of the bundle's tx signatures (base58 strings).
  // We expose this helper for logging/debugging.
  const joined = signaturesBase58.join("");
  return crypto.createHash("sha256").update(joined).digest("hex");
}
