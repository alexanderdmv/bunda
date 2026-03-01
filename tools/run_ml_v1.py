from __future__ import annotations

import os
import yaml

from pipeline.labeling import build_binary_labels
from pipeline.training import TrainConfig, train_lgbm_classifier

def main():
    # Load config if present
    cfg_path = os.path.join("config", "config.yaml")
    ml_cfg = {}
    if os.path.isfile(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        ml_cfg = cfg.get("ml", {}) or {}

    decisions_csv = ml_cfg.get("decisions_csv", "data/processed/decisions.csv")
    horizon_min = int(ml_cfg.get("horizon_min", 60))
    return_threshold = float(ml_cfg.get("return_threshold", 0.20))
    labeled_out = ml_cfg.get("labeled_out", "data/processed/dataset_labeled.csv")
    model_out = ml_cfg.get("model_path", "models/tabular/scorer_lgbm.pkl")

    print("[ML] Building labels...")
    out = build_binary_labels(
        decisions_csv=decisions_csv,
        horizon_min=horizon_min,
        return_threshold=return_threshold,
        out_path=labeled_out,
    )
    print("[ML] Labeled dataset:", out)

    feature_cols = ml_cfg.get("feature_cols", []) or []
    print("[ML] Training model...")
    tcfg = TrainConfig(dataset_csv=out, model_out=model_out, feature_cols=feature_cols)
    mpath = train_lgbm_classifier(tcfg)
    print("[ML] Model saved:", mpath)

if __name__ == "__main__":
    main()
