from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class PendingEntry:
    next_check_unix: int
    expires_unix: int
    first_seen_unix: int
    last_reason: str
    tries: int = 0


class DexStateStore:
    """Persistent state for DexScreener discovery.

    Why this exists:
      - A token can be *too early* (liq/vol/trades not yet there) and should be re-checked.
      - We must NOT mark such tokens as permanently seen, otherwise we miss early runners.

    File format (JSON):
      {
        "version": 2,
        "seen": ["mint1", ...],
        "pending": {
           "mint2": {"next": 1700000000, "expires": 1700000600, "first": 1700000000,
                     "reason": "liq_lt_min", "tries": 2}
        }
      }

    Backward compatible:
      - If the file is a JSON list => treated as permanent seen list.
    """

    def __init__(
        self,
        path: str,
        enabled: bool = True,
        interval_seconds: int = 45,
        ttl_seconds: int = 20 * 60,
        max_pending: int = 5000,
        temporary_skip_reasons: Optional[List[str]] = None,
        autosave_every: int = 25,
        max_tries_total: int = 10,
        max_tries_errors: int = 20,
        max_tries_no_pairs: int = 10,
        max_tries_metrics: int = 6,
    ):
        self.path = str(path)
        self.enabled = bool(enabled)
        self.interval_seconds = int(interval_seconds)
        self.ttl_seconds = int(ttl_seconds)
        self.max_pending = int(max_pending)
        self.autosave_every = int(autosave_every)

        # Smart pending: stop infinite rechecks.
        self.max_tries_total = int(max_tries_total)
        self.max_tries_errors = int(max_tries_errors)
        self.max_tries_no_pairs = int(max_tries_no_pairs)
        self.max_tries_metrics = int(max_tries_metrics)

        self.temporary_skip_reasons = set(
            temporary_skip_reasons
            or [
                # Common early-stage reasons that often resolve within minutes
                "liq_lt_min",
                "vol_h24_lt_min",
                "trade_h24_lt_min",
                "buy_h24_lt_min",
                "sell_h24_lt_min",
                "vol_to_liq_lt_min",
                "age_lt_min",
                "mcap_lt_min",
                "fdv_lt_min",
                # When DS doesn't have pair data yet
                "no_pairs",
            ]
        )

        self.seen: Set[str] = set()
        self.pending: Dict[str, PendingEntry] = {}

        self._dirty_updates = 0
        self.load()

    # ---------------------
    # Persistence
    # ---------------------
    def load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            self.seen = set()
            self.pending = {}
            return
        except Exception:
            self.seen = set()
            self.pending = {}
            return

        # Backward compatible list
        if isinstance(data, list):
            self.seen = set(str(x) for x in data)
            self.pending = {}
            return

        if not isinstance(data, dict):
            self.seen = set()
            self.pending = {}
            return

        seen_list = data.get("seen")
        if isinstance(seen_list, list):
            self.seen = set(str(x) for x in seen_list)
        else:
            self.seen = set()

        pend = data.get("pending")
        self.pending = {}
        if isinstance(pend, dict):
            for mint, v in pend.items():
                if not isinstance(v, dict):
                    continue
                try:
                    self.pending[str(mint)] = PendingEntry(
                        next_check_unix=int(v.get("next") or 0),
                        expires_unix=int(v.get("expires") or 0),
                        first_seen_unix=int(v.get("first") or 0),
                        last_reason=str(v.get("reason") or ""),
                        tries=int(v.get("tries") or 0),
                    )
                except Exception:
                    continue

        self._prune_expired(int(time.time()))

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        data = {
            "version": 2,
            "seen": sorted(self.seen),
            "pending": {
                mint: {
                    "next": int(p.next_check_unix),
                    "expires": int(p.expires_unix),
                    "first": int(p.first_seen_unix),
                    "reason": p.last_reason,
                    "tries": int(p.tries),
                }
                for mint, p in self.pending.items()
            },
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, self.path)
        self._dirty_updates = 0

    def maybe_autosave(self) -> None:
        if self.autosave_every <= 0:
            return
        if self._dirty_updates >= self.autosave_every:
            self.save()

    # ---------------------
    # Queries
    # ---------------------
    def is_permanent_seen(self, mint: str) -> bool:
        return str(mint) in self.seen

    def is_pending(self, mint: str) -> bool:
        return str(mint) in self.pending

    
    def is_known(self, mint: str, now_unix: Optional[int] = None) -> bool:
        """Return True if mint is either permanently seen OR currently pending.

        Pending should be treated as 'known' for the latest-profiles feed, otherwise
        the same tokens appear as 'new' over and over.
        """
        m = str(mint)
        if m in self.seen:
            return True
        now = int(now_unix or time.time())
        self._prune_expired(now)
        return m in self.pending

    def due_pending_mints(self, now_unix: Optional[int] = None, max_n: Optional[int] = None) -> List[str]:
        if not self.enabled:
            return []
        now = int(now_unix or time.time())
        self._prune_expired(now)
        due = [m for m, p in self.pending.items() if int(p.next_check_unix) <= now]
        # Oldest first (by first_seen)
        due.sort(key=lambda m: self.pending[m].first_seen_unix)
        if max_n is not None and max_n > 0:
            due = due[: int(max_n)]
        return due

    def _prune_expired(self, now: int) -> None:
        if not self.pending:
            return
        expired = [m for m, p in self.pending.items() if int(p.expires_unix) <= now]
        for m in expired:
            self.pending.pop(m, None)

        # Guardrail against unbounded growth
        if self.max_pending and len(self.pending) > self.max_pending:
            # Drop the oldest entries
            mints = sorted(self.pending.keys(), key=lambda m: self.pending[m].first_seen_unix)
            drop = len(self.pending) - self.max_pending
            for m in mints[:drop]:
                self.pending.pop(m, None)

    # ---------------------
    # Updates
    # ---------------------
    def mark_seen_permanent(self, mint: str) -> None:
        mint = str(mint)
        self.seen.add(mint)
        self.pending.pop(mint, None)
        self._dirty_updates += 1
        self.maybe_autosave()

    def mark_pending(self, mint: str, reason: str, now_unix: Optional[int] = None) -> None:
        if not self.enabled:
            # If pending is disabled, fall back to permanent seen to avoid repeated work.
            self.mark_seen_permanent(mint)
            return

        now = int(now_unix or time.time())
        mint = str(mint)

        p = self.pending.get(mint)
        if p is None:
            p = PendingEntry(
                next_check_unix=now + self.interval_seconds,
                expires_unix=now + self.ttl_seconds,
                first_seen_unix=now,
                last_reason=str(reason),
                tries=0,
            )
            self.pending[mint] = p
        else:
            p.next_check_unix = now + self.interval_seconds
            p.expires_unix = max(p.expires_unix, now + self.ttl_seconds)
            p.last_reason = str(reason)
        p.tries += 1

        self._dirty_updates += 1
        self.maybe_autosave()

    def update_after_decision(self, mint: str, decision: str, reason: str) -> str:
        """Update state based on final decision + reason.

        Returns:
          - "pending" if the mint was put/kept in pending recheck
          - "seen" if the mint was marked permanently seen
          - "noop" if mint empty
        """
        if not mint:
            return "noop"
        mint = str(mint)
        decision_u = str(decision or "").upper()
        reason_s = str(reason or "")

        if decision_u == "WATCH":
            self.mark_seen_permanent(mint)
            return "seen"

        if decision_u != "SKIP":
            # Unknown decisions treated as permanent to avoid repeats
            self.mark_seen_permanent(mint)
            return "seen"

        # SKIP: decide if it is temporary
        if reason_s in self.temporary_skip_reasons:
            # Smart caps on retries to stop infinite loops.
            limit = self._max_tries_for_reason(reason_s)
            existing = self.pending.get(mint)
            existing_tries = int(existing.tries) if existing else 0

            # If we've already retried too many times, make it permanent.
            if existing_tries >= limit or existing_tries >= self.max_tries_total:
                self.mark_seen_permanent(mint)
                return "seen"

            self.mark_pending(mint, reason_s)
            # After mark_pending, we have incremented tries.
            p = self.pending.get(mint)
            if p and int(p.tries) >= limit:
                # Next time would be wasteful — lock it now.
                self.mark_seen_permanent(mint)
                return "seen"
            return "pending"

        # Permanent skip
        self.mark_seen_permanent(mint)
        return "seen"

    def _max_tries_for_reason(self, reason: str) -> int:
        r = str(reason or "")
        if r in {"http_error", "request_error"}:
            return max(1, self.max_tries_errors)
        if r == "no_pairs":
            return max(1, self.max_tries_no_pairs)
        # Default for metric/threshold reasons
        return max(1, self.max_tries_metrics)
