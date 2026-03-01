from __future__ import annotations

"""Project control-panel config.

We keep ONE place (config/control.yaml) for the most important knobs:
- WATCH filters
- milestone validation thresholds
- tracker DB path

All other configs (API keys etc.) may still live in config/config.yaml.
"""

from pathlib import Path
from typing import Any, Dict

import yaml


DEFAULT_CONTROL_PATH = Path("config/control.yaml")


def deep_get(d: Dict[str, Any], path: str, default=None):
    """Safe nested getter for dicts using dot-paths."""
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def load_control(path: Path = DEFAULT_CONTROL_PATH) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
