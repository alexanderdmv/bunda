import { z } from "zod";
import { PublicKey } from "@solana/web3.js";

function isValidPublicKey(v: string): boolean {
  try {
    new PublicKey(v);
    return true;
  } catch {
    return false;
  }
}

export const OrderRequestSchema = z
  .object({
    // New envelope
    chain: z.string().optional(),
    venue: z.string().optional(),
    side: z.enum(["buy", "sell", "transfer"]).optional(),
    mint: z
      .string()
      .optional()
      .refine((v) => !v || isValidPublicKey(v), {
        message: "invalid mint public key",
      }),
    amount_in: z.number().nonnegative().optional(),
    amount: z.number().nonnegative().optional(),
    slippage_bps: z.number().int().nonnegative().optional(),
    slippageBps: z.number().int().nonnegative().optional(),
    to: z.string().optional(),
    secret_b58: z.string().optional(),
    useJito: z.boolean().optional(),
    meta: z.record(z.any()).optional(),

    // Legacy / compat keys
    action: z.enum(["buy", "sell", "transfer"]).optional(),
    amount_sol: z.number().nonnegative().optional(),
    sell_all: z.boolean().optional(),
    dry_run: z.boolean().optional(),
    dryRun: z.boolean().optional(),
    simulate: z.boolean().optional(),
  })
  // Keep unknown keys so index.ts can read compatibility flags without schema churn.
  .passthrough()
  .superRefine((v, ctx) => {
    const s = v.side ?? v.action;
    if (!s) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "missing side/action" });
      return;
    }
    if ((s === "buy" || s === "sell") && !v.mint) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: "mint required for buy/sell" });
    }
  });

export type OrderRequest = z.infer<typeof OrderRequestSchema>;

export type OrderResponse = {
  ok: boolean;
  dry_run: boolean;
  signature?: string;
  bundle_id?: string;
  message?: string;
  details?: any;
  tx_base64?: string;
  tip_tx_base64?: string;
};
