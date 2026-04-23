from __future__ import annotations

"""Secrets loader.

We keep API keys out of the control panel (config/control.yaml). This makes it
safe to commit/share the control panel while keeping credentials private.

Priority for secrets:
  1) Environment variables (preferred for deployments)
  2) config/secrets.yaml (local development)

File format example (config/secrets.example.yaml):

birdeye:
  api_key: "PASTE_BIRDEYE_KEY_HERE"

"""

from pathlib import Path
from typing import Any, Dict, Optional

import yaml


SECRETS_PATH = Path("config/secrets.yaml")


def load_secrets(optional: bool = True, path: Path = SECRETS_PATH) -> Dict[str, Any]:
    """Load secrets from YAML. If optional=True and file does not exist, returns empty dict."""
    if not path.exists():
        if optional:
            return {}
        raise FileNotFoundError(
            f"Missing secrets file: {path}. Create it from config/secrets.example.yaml"
        )
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("secrets.yaml must be a mapping")
    return data


def get_secret(secrets: Dict[str, Any], dotted_path: str, default: Optional[str] = None) -> Optional[str]:
    """Fetch a secret value using dot notation, e.g. 'birdeye.api_key'."""
    cur: Any = secrets
    for part in dotted_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    if cur is None:
        return default
    return str(cur)

