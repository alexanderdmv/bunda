from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from loguru import logger

try:
    import requests  # already in requirements.txt
except Exception:  # pragma: no cover
    requests = None  # type: ignore


LAMPORTS_PER_SOL = 1_000_000_000


@dataclass
class OrderRequest:
    """A minimal order envelope sent to an executor service."""

    chain: str
    venue: str           # e.g. "pumpfun"
    side: str            # "buy" or "sell"
    mint: str            # token mint (CA)
    amount_in: float     # SOL amount for buy, token amount for sell (0 => sell all)
    slippage_bps: int = 1500
    meta: Optional[Dict[str, Any]] = None


class ExecutorClient:
    """Thin HTTP client for the local TypeScript executor."""

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = str(base_url).rstrip("/")
        self.timeout = float(timeout)

    def place_order(self, order: OrderRequest) -> Tuple[bool, Dict[str, Any]]:
        if requests is None:
            return False, {"error": "requests_not_available"}
        try:
            meta = order.meta or {}

            # These MUST be explicitly set by TradingEngine so one flag controls behavior.
            req_dry_run = bool(meta.get("dry_run", True))
            req_simulate = bool(meta.get("simulate", False))
            req_use_jito = bool(meta.get("useJito", True))

            payload: Dict[str, Any] = {
                # new envelope (Python side)
                "chain": order.chain,
                "venue": order.venue,
                "side": order.side,
                "mint": order.mint,
                "amount_in": float(order.amount_in),
                "slippage_bps": int(order.slippage_bps),
                "useJito": req_use_jito,
                "dry_run": req_dry_run,
                "simulate": req_simulate,
                "meta": meta,
                # compat with older executor schema
                "action": order.side,
            }

            # Only include legacy fields when meaningful.
            # Sending explicit null breaks zod validation on the TS side.
            if order.side == "buy":
                payload["amount_sol"] = float(order.amount_in)
            if bool(meta.get("sell_all", False)) or (order.side == "sell" and float(order.amount_in) == 0.0):
                payload["sell_all"] = True

            r = requests.post(
                f"{self.base_url}/trade",
                json=payload,
                timeout=self.timeout,
            )
            js = r.json() if "application/json" in (r.headers.get("content-type") or "") else {}
            if r.status_code >= 400:
                return False, {"error": f"http_{r.status_code}", "body": js}
            if not isinstance(js, dict):
                return False, {"error": "bad_json"}
            ok = bool(js.get("ok", True))
            return ok, js
        except Exception as e:
            return False, {"error": f"executor_error:{e}"}

    def quote_buy(self, mint: str, amount_sol: float, slippage_bps: int = 100) -> Optional[Dict[str, int]]:
        """Quote BUY: returns integers (lamports/token units) to avoid float precision loss."""
        if requests is None:
            return None
        try:
            r = requests.get(
                f"{self.base_url}/quote",
                params={"side": "buy", "mint": mint, "amount_in": amount_sol, "slippage_bps": slippage_bps},
                timeout=self.timeout,
            )
            if r.status_code >= 400:
                return None
            js = r.json() if "application/json" in (r.headers.get("content-type") or "") else {}
            if not isinstance(js, dict) or not bool(js.get("ok", False)):
                return None
            q = js.get("quote") or {}
            if not isinstance(q, dict):
                return None
            sol_in_lamports = q.get("sol_in_lamports") or q.get("solInLamports")
            token_out = q.get("token_out") or q.get("tokenOut")
            if sol_in_lamports is None or token_out is None:
                return None
            return {
                "sol_in_lamports": int(sol_in_lamports),
                "token_out": int(token_out),
            }
        except Exception:
            return None

    def quote_sell_token_amount(self, mint: str, token_amount: int, slippage_bps: int = 100) -> Optional[int]:
        """Quote SELL: token_amount -> sol_out_lamports."""
        if requests is None:
            return None
        try:
            r = requests.get(
                f"{self.base_url}/quote",
                params={"side": "sell", "mint": mint, "amount_in": str(int(token_amount)), "slippage_bps": slippage_bps},
                timeout=self.timeout,
            )
            if r.status_code >= 400:
                return None
            js = r.json() if "application/json" in (r.headers.get("content-type") or "") else {}
            if not isinstance(js, dict) or not bool(js.get("ok", False)):
                return None
            q = js.get("quote") or {}
            if not isinstance(q, dict):
                return None
            sol_out = q.get("sol_out") or q.get("solOut")
            if sol_out is None:
                return None
            return int(sol_out)
        except Exception:
            return None

    def quote_sell_all(self, mint: str, slippage_bps: int = 100) -> Optional[int]:
        """Quote SELL ALL (wallet): returns sol_out_lamports."""
        if requests is None:
            return None
        try:
            r = requests.get(
                f"{self.base_url}/quote",
                params={"side": "sell", "mint": mint, "amount_in": 0, "slippage_bps": slippage_bps},
                timeout=self.timeout,
            )
            if r.status_code >= 400:
                return None
            js = r.json() if "application/json" in (r.headers.get("content-type") or "") else {}
            if not isinstance(js, dict) or not bool(js.get("ok", False)):
                return None
            q = js.get("quote") or {}
            if not isinstance(q, dict):
                return None
            sol_out = q.get("sol_out") or q.get("solOut")
            if sol_out is None:
                return None
            return int(sol_out)
        except Exception:
            return None


class TradingEngine:
    """Trading hook with a SINGLE mode switch.

    One flag in config/control.yaml:

      trading.dry_run:
        - true  => paper trading (realistic: build BUY tx, keep virtual token balance, compute P&L via /quote)
        - false => live trading (executor sends BUY/SELL on-chain)

    Modes:
      - filter: buy only on WATCH (or configured decision)
      - spray: allow early buys even when DexScreener hasn't populated pairs yet

    Sell modes:
      - time: sell after hold_seconds
      - tp_sl: sell when take_profit or stop_loss is hit
      - tp_sl_time: tp/sl with a max hold time fallback
    """

    def __init__(self, cfg: Dict[str, Any]):
        cfg = cfg or {}
        self.enabled: bool = bool(cfg.get("enabled", False))
        self.dry_run: bool = bool(cfg.get("dry_run", True))

        self.chain: str = str(cfg.get("chain", "solana"))
        self.venue: str = str(cfg.get("venue", "pumpfun"))

        self.mode: str = str(cfg.get("mode", "filter")).strip().lower()
        self.buy_on: str = str(cfg.get("buy_on_decision", "WATCH")).upper()

        self.executor_url: Optional[str] = (cfg.get("executor_url") or None)
        self.executor = (
            ExecutorClient(self.executor_url, timeout=float(cfg.get("executor_timeout", 10.0)))
            if self.executor_url
            else None
        )

        self.buy_amount_sol: float = float(cfg.get("buy_amount_sol", 0.02))
        self.slippage_bps: int = int(cfg.get("slippage_bps", 1200))

        self.take_profit_pct: float = float(cfg.get("take_profit_pct", 50))
        self.stop_loss_pct: float = float(cfg.get("stop_loss_pct", 25))

        self.sell_mode: str = str(cfg.get("sell_mode", "time")).strip().lower()
        self.hold_seconds: int = int(cfg.get("hold_seconds", 25))

        self.max_open_positions: int = int(cfg.get("max_open_positions", 2))
        self.min_interval_per_mint_seconds: float = float(cfg.get("min_interval_per_mint_seconds", 90))
        self.min_interval_global_seconds: float = float(cfg.get("min_interval_global_seconds", 0.25))

        self.spray_allow_reasons = set(
            (cfg.get("spray_allow_reasons") or [])
            if isinstance(cfg.get("spray_allow_reasons"), list)
            else []
        )

        # Optional knobs (independent of dry_run)
        self.simulate: bool = bool(cfg.get("simulate", False))
        self.use_jito: bool = bool(cfg.get("use_jito", True))

        self._last_action_by_mint: Dict[str, float] = {}
        self._last_action_global: float = 0.0

        # positions[mint] = {...}
        self._open_positions: Dict[str, Dict[str, Any]] = {}

        # Tick scheduling: in the scaffold, tick() is typically called from the main loop
        # while tokens are flowing. If the stream pauses, positions may sit longer than
        # hold_seconds. A lightweight background *time-only* ticker fixes that without
        # spamming extra /quote calls.
        self.background_tick: bool = bool(cfg.get("background_tick", True))
        self.tick_interval_seconds: float = float(cfg.get("tick_interval_seconds", 1.0))

        # Thread safety: tick() may run in a background thread.
        self._lock = threading.RLock()
        self._ticker_thread: Optional[threading.Thread] = None
        self._ticker_stop = threading.Event()

        if self.enabled and self.background_tick and self.tick_interval_seconds > 0:
            self._ticker_thread = threading.Thread(
                target=self._ticker_loop,
                name="trading_tick",
                daemon=True,
            )
            self._ticker_thread.start()

    def _ticker_loop(self) -> None:
        while not self._ticker_stop.is_set():
            try:
                self.tick_time_only()
            except Exception:
                pass
            time.sleep(self.tick_interval_seconds)

    def tick_time_only(self) -> None:
        """Close positions by hold_seconds even when token stream is idle.

        This intentionally does NOT compute TP/SL quotes; those are handled by tick()
        which the main loop calls when it is active.
        """
        if not self.enabled:
            return

        if self.sell_mode not in {"time", "tp_sl_time"}:
            return

        now = time.time()
        with self._lock:
            items = list(self._open_positions.items())

        for mint, pos in items:
            opened_at = float(pos.get("opened_at", now))
            age = now - opened_at
            if age >= self.hold_seconds:
                self._close_position(mint, "time")

    def stop(self) -> None:
        """Optional: stop background ticker."""
        try:
            self._ticker_stop.set()
        except Exception:
            pass

    def maybe_trade(self, token: Dict[str, Any], decision: str, reason: str) -> None:
        if not self.enabled:
            return

        mint = token.get("token_address")
        if not mint:
            return

        now = time.time()

        with self._lock:
            last_m = float(self._last_action_by_mint.get(mint, 0.0))
            if (now - last_m) < self.min_interval_per_mint_seconds:
                return
            if (now - self._last_action_global) < self.min_interval_global_seconds:
                return

            decision_u = (decision or "").upper()
            reason_l = (reason or "").strip().lower()

            should_buy = False
            if self.mode == "filter":
                should_buy = decision_u == self.buy_on
            elif self.mode == "spray":
                # allow some SKIP/PENDING reasons for early snipe attempts
                if decision_u in ("PENDING", "PENDING_RECHECK", "SKIP") and (reason_l in self.spray_allow_reasons):
                    should_buy = True
                elif decision_u == "WATCH":
                    should_buy = True

            if not should_buy:
                return

            if mint not in self._open_positions and len(self._open_positions) >= self.max_open_positions:
                logger.info(f"[TRADING] skip buy (max_open_positions reached) mint={mint}")
                return

            # Reserve this action slot
            self._last_action_by_mint[mint] = now
            self._last_action_global = now

        order = OrderRequest(
            chain=self.chain,
            venue=self.venue,
            side="buy",
            mint=str(mint),
            amount_in=self.buy_amount_sol,
            slippage_bps=self.slippage_bps,
            meta={
                "symbol": token.get("symbol"),
                "reason": reason,
                "decision": decision_u,
                "dexscreener_url": token.get("dexscreener_url"),
                "ts": int(time.time()),
                # SINGLE SWITCH propagated to executor
                "dry_run": bool(self.dry_run),
                "simulate": bool(self.simulate),
                "useJito": bool(self.use_jito),
            },
        )

        if self.executor is None:
            # No executor: can't quote or build tx
            if self.dry_run:
                logger.warning(f"[TRADING][DRY_RUN] BUY (no executor) {order.amount_in} SOL mint={mint} ({reason})")
                with self._lock:
                    self._open_positions.setdefault(
                        mint,
                        {"opened_at": time.time(), "buy_amount_sol": self.buy_amount_sol},
                    )
            else:
                logger.warning(f"[TRADING] BUY skipped (no executor configured) mint={mint}")
            return

        ok, resp = self.executor.place_order(order)
        if not ok:
            logger.warning(f"[TRADING] BUY failed mint={mint} err={resp}")
            return

        sig = str(resp.get("signature") or resp.get("txid") or "")

        paper_token_amt: Optional[int] = None
        paper_buy_lamports: Optional[int] = None
        if self.dry_run:
            # Prefer meta returned by /trade (no extra /quote call => fewer RPC requests).
            q = resp.get("quote") if isinstance(resp, dict) else None
            if isinstance(q, dict):
                try:
                    paper_token_amt = int(q.get("token_out") or q.get("tokenOut") or 0) or None
                    paper_buy_lamports = int(q.get("sol_in_lamports") or q.get("solInLamports") or 0) or None
                except Exception:
                    paper_token_amt = None
                    paper_buy_lamports = None

            # Fallback to /quote if executor did not return quote meta.
            if paper_token_amt is None or paper_buy_lamports is None:
                qb = self.executor.quote_buy(str(mint), float(self.buy_amount_sol), slippage_bps=100)
                if qb:
                    paper_token_amt = int(qb.get("token_out", 0) or 0) or None
                    paper_buy_lamports = int(qb.get("sol_in_lamports", 0) or 0) or None

        if self.dry_run:
            logger.info(
                f"[TRADING][DRY_RUN] BUY built mint={mint} token_out={paper_token_amt if paper_token_amt is not None else 'n/a'}"
            )
        else:
            logger.info(f"[TRADING] BUY sent mint={mint} sig={sig or 'ok'}")

        with self._lock:
            self._open_positions.setdefault(
                mint,
                {
                    "opened_at": time.time(),
                    "buy_amount_sol": self.buy_amount_sol,
                    "buy_lamports": int(round(self.buy_amount_sol * LAMPORTS_PER_SOL)),
                    "sig_buy": sig,
                    "buy_resp": resp,
                    "executor_dry_run": bool(resp.get("dry_run", False)),
                    # Paper fields
                    "paper_token_amount": paper_token_amt,
                    "paper_buy_lamports": paper_buy_lamports,
                },
            )

    def _close_position(self, mint: str, why: str) -> None:
        with self._lock:
            pos = self._open_positions.get(mint)
            if not pos:
                return

        if self.executor is None:
            logger.warning(f"[TRADING] close position (no executor) mint={mint} why={why}")
            with self._lock:
                self._open_positions.pop(mint, None)
            return

        if self.dry_run:
            token_amt = pos.get("paper_token_amount")
            buy_lamports = int(pos.get("paper_buy_lamports") or pos.get("buy_lamports") or 0)
            sol_out_lamports = None
            if token_amt:
                sol_out_lamports = self.executor.quote_sell_token_amount(mint, int(token_amt), slippage_bps=100)

            if sol_out_lamports is not None and buy_lamports > 0:
                pnl_pct = (float(sol_out_lamports) - float(buy_lamports)) / float(buy_lamports) * 100.0
                logger.warning(
                    f"[TRADING][DRY_RUN] SELL ({why}) mint={mint} sol_out={sol_out_lamports} pnl={pnl_pct:.1f}%"
                )
            else:
                logger.warning(f"[TRADING][DRY_RUN] SELL ({why}) mint={mint} (no quote)")

            with self._lock:
                self._open_positions.pop(mint, None)
            return

        # Live sell-all
        order = OrderRequest(
            chain=self.chain,
            venue=self.venue,
            side="sell",
            mint=mint,
            amount_in=0.0,
            slippage_bps=self.slippage_bps,
            meta={
                "sell_all": True,
                "why": why,
                "ts": int(time.time()),
                "dry_run": False,
                "simulate": bool(self.simulate),
                "useJito": bool(self.use_jito),
            },
        )

        ok, resp = self.executor.place_order(order)
        if ok:
            sig = str(resp.get("signature") or resp.get("txid") or "")
            logger.info(f"[TRADING] SELL sent mint={mint} sig={sig or 'ok'} why={why}")
        else:
            logger.warning(f"[TRADING] SELL failed mint={mint} err={resp} why={why}")

        with self._lock:
            self._open_positions.pop(mint, None)

    def tick(self) -> None:
        if not self.enabled:
            return

        now = time.time()
        with self._lock:
            items = list(self._open_positions.items())

        for mint, pos in items:
            opened_at = float(pos.get("opened_at", now))
            age = now - opened_at

            # TP/SL branch
            if self.sell_mode in {"tp_sl", "tp_sl_time"}:
                sol_out_lamports: Optional[int] = None
                buy_lamports: int = int(pos.get("paper_buy_lamports") or pos.get("buy_lamports") or 0)

                if self.executor is not None:
                    if self.dry_run:
                        token_amt = pos.get("paper_token_amount")
                        if token_amt:
                            sol_out_lamports = self.executor.quote_sell_token_amount(mint, int(token_amt), slippage_bps=100)
                    else:
                        sol_out_lamports = self.executor.quote_sell_all(mint, slippage_bps=100)

                # In live mode, right after BUY, token balance may still be 0 => quote 0.
                if (not self.dry_run) and (sol_out_lamports is not None) and sol_out_lamports == 0 and age < 6:
                    continue

                if sol_out_lamports is not None and buy_lamports > 0:
                    pnl_pct = (float(sol_out_lamports) - float(buy_lamports)) / float(buy_lamports) * 100.0
                    pos["last_quote_sol_out_lamports"] = sol_out_lamports
                    pos["last_pnl_pct"] = pnl_pct

                    if pnl_pct >= self.take_profit_pct:
                        self._close_position(mint, f"take_profit_{pnl_pct:.1f}%")
                        continue
                    if pnl_pct <= -abs(self.stop_loss_pct):
                        self._close_position(mint, f"stop_loss_{pnl_pct:.1f}%")
                        continue

                if self.sell_mode == "tp_sl_time" and age >= self.hold_seconds:
                    self._close_position(mint, "time")
                    continue

                # tp_sl without time fallback -> just keep holding
                continue

            # Time-only sell
            if self.sell_mode == "time" and age >= self.hold_seconds:
                self._close_position(mint, "time")
