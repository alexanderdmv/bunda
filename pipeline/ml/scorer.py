from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import os
import math

import pandas as pd


@dataclass
class MLScorerConfig:
    enabled: bool = False
    model_path: str = "models/tabular/scorer_lgbm.pkl"
    score_threshold: float = 0.75
    feature_cols: List[str] = None

    def __post_init__(self):
        if self.feature_cols is None:
            self.feature_cols = []


class MLScorer:
    """
    Lightweight inference wrapper.

    Fixes common production issues:
    - Training saved an artifact dict {"model": ..., "feature_cols": ...}
    - Runtime tokens may miss some features: we fill missing with 0.0
    - Ensures inference feature shape exactly matches what the model was trained on
    """

    def __init__(self, cfg: MLScorerConfig):
        self.cfg = cfg
        self.model = None
        self._feature_cols: List[str] = []

    def load(self) -> None:
        if not self.cfg.enabled:
            return
        if not os.path.isfile(self.cfg.model_path):
            self.model = None
            self._feature_cols = []
            return

        try:
            import joblib  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "MLScorer is enabled but 'joblib' is not installed. "
                "Install with: pip install joblib"
            ) from e

        obj = joblib.load(self.cfg.model_path)

        # Support both raw model and packed artifact dict
        model = obj
        artifact_cols: List[str] = []
        if isinstance(obj, dict):
            model = obj.get("model")
            artifact_cols = list(obj.get("feature_cols") or [])
        self.model = model

        # Determine expected feature columns
        cols: List[str] = list(self.cfg.feature_cols or [])
        if not cols:
            cols = artifact_cols

        # If still empty, try to pull from model metadata
        if (not cols) and (self.model is not None):
            # sklearn API
            if hasattr(self.model, "feature_name_"):
                try:
                    cols = list(getattr(self.model, "feature_name_"))
                except Exception:
                    cols = []
            if (not cols) and hasattr(self.model, "feature_name"):
                try:
                    cols = list(self.model.feature_name())
                except Exception:
                    cols = []

        # If cols length mismatches model expectation, prefer artifact_cols
        try:
            expected = int(getattr(self.model, "n_features_in_", 0) or 0)
        except Exception:
            expected = 0

        if expected and cols and len(cols) != expected and artifact_cols and len(artifact_cols) == expected:
            cols = artifact_cols

        self._feature_cols = list(cols)

    def score_one(self, token: Dict[str, Any]) -> float:
        """
        Return probability of positive class.

        - Uses schema from model artifact (preferred) or cfg.feature_cols.
        - Missing features are filled with 0.0.
        - Non-numeric values are coerced; NaNs are filled with 0.0.
        - Any inference error returns NaN (keeps runtime stable).
        """
        if (not self.cfg.enabled) or (self.model is None):
            return float("nan")

        cols = list(self._feature_cols or self.cfg.feature_cols or [])
        if not cols:
            return float("nan")

        # Build a single-row frame with exact column order expected by the model
        row = {c: token.get(c, 0.0) for c in cols}
        X = pd.DataFrame([row], columns=cols)

        # Coerce to numeric & fill missing
        X = X.apply(pd.to_numeric, errors="coerce").fillna(0.0)

        try:
            proba = self.model.predict_proba(X)[0][1]
            return float(proba)
        except Exception:
            return float("nan")

    @staticmethod
    def is_valid_score(score: float) -> bool:
        return isinstance(score, (int, float)) and (not math.isnan(float(score)))
