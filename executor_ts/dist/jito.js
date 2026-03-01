import crypto from "node:crypto";
function bundlesEndpoint(baseUrl, uuid) {
    const clean = baseUrl.replace(/\/$/, "");
    const url = `${clean}/api/v1/bundles`;
    if (!uuid)
        return url;
    // The docs allow uuid as query parameter as well
    const sep = url.includes("?") ? "&" : "?";
    return `${url}${sep}uuid=${encodeURIComponent(uuid)}`;
}
async function jitoRpc(cfg, method, params) {
    const url = bundlesEndpoint(cfg.blockEngineUrl, cfg.uuid);
    const headers = { "Content-Type": "application/json" };
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
    const json = (await res.json());
    if (!res.ok || json.error) {
        const msg = json.error?.message ?? `HTTP ${res.status}`;
        throw new Error(`Jito RPC ${method} failed: ${msg}`);
    }
    if (json.result === undefined)
        throw new Error(`Jito RPC ${method} missing result`);
    return json.result;
}
export async function getTipAccounts(cfg) {
    return jitoRpc(cfg, "getTipAccounts", []);
}
export async function sendBundle(cfg, signedTxBase64) {
    // Per docs: params = [ [tx1, tx2, ...], { encoding: "base64" } ]
    return jitoRpc(cfg, "sendBundle", [signedTxBase64, { encoding: "base64" }]);
}
export function computeBundleIdFromSignatures(signaturesBase58) {
    // Jito docs: bundle id is SHA-256 hash of the bundle's tx signatures (base58 strings).
    // We expose this helper for logging/debugging.
    const joined = signaturesBase58.join("");
    return crypto.createHash("sha256").update(joined).digest("hex");
}
