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
  return jitoRpc<string[]>(cfg, "getTipAccounts", []);
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
