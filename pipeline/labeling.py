from __future__ import annotations

import pandas as pd


def build_binary_labels(
    decisions_csv: str,
    horizon_min: int,
    return_threshold: float,
    out_path: str = "data/processed/dataset_labeled.csv",
) -> str:
    """
    Build a simple labeled dataset for ML scorer v1.

    Requirements:
    - decisions_csv must contain repeated snapshots per token_address over time
      with columns: token_address, timestamp_utc, price_usd
    - For each snapshot t0 we look ahead to t0 + horizon_min and take the closest
      available snapshot at or after that time to estimate forward return.

    Label:
      y = 1 if forward_return >= return_threshold else 0

    This is intentionally simple and deterministic for v1.
    """
    if str(decisions_csv).lower().endswith(".jsonl"):
        df = pd.read_json(decisions_csv, lines=True)
    else:
        df = pd.read_csv(decisions_csv)
    if df.empty:
        raise ValueError("decisions_csv is empty")

    required = {"token_address", "timestamp_utc", "price_usd"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for labeling: {sorted(missing)}")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc", "token_address", "price_usd"]).copy()
    df = df.sort_values(["token_address", "timestamp_utc"])

    # For each row, find future row at/after horizon
    df["t_future"] = df["timestamp_utc"] + pd.to_timedelta(horizon_min, unit="m")

    # Self-merge by token_address using merge_asof to find nearest future snapshot
    out_rows = []
    for token, g in df.groupby("token_address", sort=False):
        g = g.sort_values("timestamp_utc")
        future = g[["timestamp_utc", "price_usd"]].rename(
            columns={"timestamp_utc": "timestamp_utc_future", "price_usd": "price_usd_future"}
        )
        # merge_asof: left key = t_future, right key = timestamp_utc_future
        merged = pd.merge_asof(
            g,
            future,
            left_on="t_future",
            right_on="timestamp_utc_future",
            direction="forward",
            allow_exact_matches=True,
        )
        merged = merged.dropna(subset=["price_usd_future"])
        if merged.empty:
            continue
        merged["fwd_return"] = (merged["price_usd_future"] / merged["price_usd"]) - 1.0
        merged["label"] = (merged["fwd_return"] >= float(return_threshold)).astype(int)
        out_rows.append(merged)

    if not out_rows:
        raise ValueError("Not enough repeated snapshots to build labels. Collect more data (rechecks over time).")

    out = pd.concat(out_rows, ignore_index=True)
    out.to_csv(out_path, index=False)
    return out_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build binary labels for ML scorer v1 from decisions.csv")
    parser.add_argument("--decisions", default="data/processed/decisions.csv", help="Path to decisions CSV")
    parser.add_argument("--horizon-min", type=int, default=60, help="Forward horizon in minutes")
    parser.add_argument("--return-threshold", type=float, default=0.20, help="Return threshold for positive label")
    parser.add_argument("--out", default="data/processed/dataset_labeled.csv", help="Output labeled dataset CSV path")
    args = parser.parse_args()

    out = build_binary_labels(
        decisions_csv=args.decisions,
        horizon_min=args.horizon_min,
        return_threshold=args.return_threshold,
        out_path=args.out,
    )
    print(out)
