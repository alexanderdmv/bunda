from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd


@dataclass
class TrainConfig:
    dataset_csv: str = "data/processed/dataset_labeled.csv"
    model_out: str = "models/tabular/scorer_lgbm.pkl"
    feature_cols: List[str] = None
    label_col: str = "label"
    timestamp_col: str = "timestamp_utc"
    test_size: float = 0.2
    random_state: int = 42

    # Evaluation settings
    decision_threshold: float = 0.5
    topk_list: Tuple[int, ...] = (5, 10, 20)

    def __post_init__(self):
        if self.feature_cols is None:
            self.feature_cols = []


def infer_feature_cols(df: pd.DataFrame, label_col: str = "label") -> List[str]:
    """Infer a safe set of feature columns for tabular scorer v1.

    - numeric only
    - exclude identifiers/metadata
    - exclude any *_future / *future* columns (leakage)
    - exclude URL-ish columns even if encoded as numbers
    """
    hard_drop = {
        label_col,
        "fwd_return",
        "token_address",
        "symbol",
        "name",
        "chain",
        "source",
        "decision",
        "reason",
        "timestamp",
        "timestamp_utc",
        "timestamp_utc_future",
        "t_future",
    }

    cols: List[str] = []
    for c in df.columns:
        if c in hard_drop:
            continue

        lc = c.lower()

        # leakage / future info
        if lc.endswith("_future") or "future" in lc:
            continue

        # urls / text artifacts that sometimes get encoded
        if lc.endswith("_url") or "url" in lc:
            continue

        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)

    return cols


def _time_aware_split(df: pd.DataFrame, ts_col: str, test_size: float):
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df.dropna(subset=[ts_col]).sort_values(ts_col)
    split = int(len(df) * (1.0 - test_size))
    split = max(1, min(split, len(df) - 1))
    return df.iloc[:split], df.iloc[split:]


def train_lgbm_classifier(cfg: TrainConfig) -> str:
    """Train ML scorer v1 (LightGBM) and save as joblib .pkl.

    Fixes vs previous:
    - prevents leakage (drops *_future / *future*)
    - numeric-only X with coercion
    - class_weight='balanced'
    - ranking metrics (precision@K) in addition to threshold metrics
    - time-aware split with fallback to stratified if test has only 1 class
    """
    try:
        from lightgbm import LGBMClassifier  # type: ignore
    except Exception as e:
        raise RuntimeError("lightgbm is required. Install: pip install lightgbm") from e

    try:
        import joblib  # type: ignore
    except Exception as e:
        raise RuntimeError("joblib is required. Install: pip install joblib") from e

    try:
        from sklearn.model_selection import train_test_split  # type: ignore
        from sklearn.metrics import roc_auc_score, precision_recall_fscore_support  # type: ignore
    except Exception as e:
        raise RuntimeError("scikit-learn is required. Install: pip install scikit-learn") from e

    df = pd.read_csv(cfg.dataset_csv)
    if df.empty:
        raise ValueError("dataset is empty")

    if cfg.label_col not in df.columns:
        raise ValueError(f"missing label column: {cfg.label_col}")

    y = pd.to_numeric(df[cfg.label_col], errors="coerce").fillna(0).astype(int)

    if not cfg.feature_cols:
        cfg.feature_cols = infer_feature_cols(df, label_col=cfg.label_col)
        if not cfg.feature_cols:
            raise ValueError("No numeric feature columns found to train on. Check your dataset columns.")
        print(f"Using inferred feature columns ({len(cfg.feature_cols)}): {cfg.feature_cols}")

    X = df[cfg.feature_cols].copy()

    # Coerce to numeric; keep NaNs (LightGBM handles them)
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    # Drop fully-empty columns
    empty_cols = [c for c in X.columns if X[c].notna().sum() == 0]
    if empty_cols:
        X = X.drop(columns=empty_cols)
        cfg.feature_cols = [c for c in cfg.feature_cols if c not in empty_cols]
        print(f"Dropped empty feature columns: {empty_cols}")

    # Split
    used_time_split = False
    if cfg.timestamp_col in df.columns:
        combo = pd.concat([X, y.rename(cfg.label_col), df[[cfg.timestamp_col]]], axis=1)
        train_df, test_df = _time_aware_split(combo, cfg.timestamp_col, cfg.test_size)
        X_train = train_df[cfg.feature_cols]
        y_train = train_df[cfg.label_col].astype(int)
        X_test = test_df[cfg.feature_cols]
        y_test = test_df[cfg.label_col].astype(int)
        used_time_split = True
        if y_test.nunique() < 2:
            used_time_split = False

    if not used_time_split:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=cfg.test_size,
            random_state=cfg.random_state,
            stratify=y if y.nunique() > 1 else None,
        )

    model = LGBMClassifier(
        n_estimators=400,
        learning_rate=0.05,
        num_leaves=63,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=cfg.random_state,
        class_weight="balanced",
        min_child_samples=15,
    )
    model.fit(X_train, y_train)

    # Evaluate
    proba = model.predict_proba(X_test)[:, 1] if len(X_test) else np.array([])
    if len(proba) and y_test.nunique() > 1:
        auc = float(roc_auc_score(y_test, proba))
    else:
        auc = float("nan")

    if len(proba):
        pred = (proba >= cfg.decision_threshold).astype(int)
        p, r, f, _ = precision_recall_fscore_support(y_test, pred, average="binary", zero_division=0)
    else:
        p = r = f = 0.0

    print(f"Split: {'time-aware' if used_time_split else 'stratified'} | Test size: {len(y_test)}")
    print(f"ROC-AUC: {auc:.4f} | Precision@{cfg.decision_threshold:.2f}: {p:.3f} | Recall: {r:.3f} | F1: {f:.3f}")

    # Ranking metrics: precision@K
    if len(proba):
        order = np.argsort(-proba)
        y_sorted = y_test.to_numpy()[order]
        for k in cfg.topk_list:
            kk = min(int(k), len(y_sorted))
            if kk <= 0:
                continue
            prec_k = float(y_sorted[:kk].mean())
            print(f"Precision@Top{kk}: {prec_k:.3f} (positives in top{kk}: {int(y_sorted[:kk].sum())}/{kk})")

    os.makedirs(os.path.dirname(cfg.model_out), exist_ok=True)
    joblib.dump(
        {
            "model": model,
            "feature_cols": cfg.feature_cols,
            "label_col": cfg.label_col,
            "timestamp_col": cfg.timestamp_col,
        },
        cfg.model_out,
    )
    return cfg.model_out


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train LightGBM scorer v1 from labeled dataset CSV")
    parser.add_argument("--dataset", default="data/processed/dataset_labeled.csv", help="Path to labeled dataset CSV")
    parser.add_argument("--model-out", default="models/tabular/scorer_lgbm.pkl", help="Output model path (.pkl)")
    parser.add_argument("--label-col", default="label", help="Label column name")
    parser.add_argument("--timestamp-col", default="timestamp_utc", help="Timestamp column name (optional)")
    parser.add_argument("--features", default="", help="Comma-separated feature columns. If empty, infer numeric columns.")
    parser.add_argument("--threshold", type=float, default=0.5, help="Decision threshold for precision/recall reporting")
    parser.add_argument("--test-size", type=float, default=0.2, help="Test split size (default 0.2)")
    args = parser.parse_args()

    feature_cols = [c.strip() for c in args.features.split(",") if c.strip()] if args.features else []
    cfg = TrainConfig(
        dataset_csv=args.dataset,
        model_out=args.model_out,
        feature_cols=feature_cols,
        label_col=args.label_col,
        timestamp_col=args.timestamp_col,
        test_size=float(args.test_size),
        decision_threshold=float(args.threshold),
    )
    out = train_lgbm_classifier(cfg)
    print(out)
