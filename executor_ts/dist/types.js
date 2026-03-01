import { z } from "zod";
export const OrderRequestSchema = z
    .object({
    // New envelope
    chain: z.string().optional(),
    venue: z.string().optional(),
    side: z.enum(["buy", "sell"]).optional(),
    mint: z.string(),
    amount_in: z.number().nonnegative().optional(),
    slippage_bps: z.number().int().nonnegative().optional(),
    meta: z.record(z.any()).optional(),
    // Legacy / compat keys
    action: z.enum(["buy", "sell"]).optional(),
    amount_sol: z.number().nonnegative().optional(),
    sell_all: z.boolean().optional(),
    dry_run: z.boolean().optional(),
    simulate: z.boolean().optional(),
})
    // Keep unknown keys (we use a few optional flags like useJito/dryRun in index.ts)
    .passthrough()
    .superRefine((v, ctx) => {
    const s = v.side ?? v.action;
    if (!s)
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "missing side/action" });
});
