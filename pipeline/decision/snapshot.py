import json
import os
from datetime import datetime
from typing import Any, Dict

def _to_jsonable(v: Any) -> Any:
    # Keep it simple and robust: stringify unknown complex types
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _to_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_to_jsonable(x) for x in v]
    return str(v)

def append_snapshot(row: Dict[str, Any], path: str) -> None:
    """Append one snapshot as JSONL (one JSON per line).

    JSONL is used (instead of CSV) to avoid column drift and parsing errors
    when token dicts have variable keys or values contain commas/newlines.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Ensure timestamp_utc exists and is a string
    ts = row.get("timestamp_utc")
    if ts is None:
        row["timestamp_utc"] = datetime.utcnow().isoformat()
    elif not isinstance(ts, str):
        try:
            row["timestamp_utc"] = ts.isoformat()  # datetime-like
        except Exception:
            row["timestamp_utc"] = str(ts)

    payload = {k: _to_jsonable(v) for k, v in row.items()}

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
