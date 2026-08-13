"""Recent-file history for the TUI. Paths only; this module must not import Textual."""

from __future__ import annotations

import json
from pathlib import Path

from ..common.path_utils import is_uri

MAX_RECENT_PATHS = 20


def default_history_path() -> Path:
    """Return ``~/.undatum/tui-history.json``."""
    return Path.home() / ".undatum" / "tui-history.json"


def _normalize_source(source: str) -> str:
    text = (source or "").strip()
    if not text:
        return ""
    if is_uri(text):
        return text
    return str(Path(text).expanduser().resolve())


def load_recent_paths(history_file: Path | None = None) -> list[str]:
    """Load recent dataset paths. Corrupt or missing files yield an empty list."""
    path = history_file or default_history_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    raw = payload.get("paths")
    if not isinstance(raw, list):
        return []
    paths: list[str] = []
    seen = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        paths.append(value)
        if len(paths) >= MAX_RECENT_PATHS:
            break
    return paths


def record_recent_path(source: str, history_file: Path | None = None) -> None:
    """Prepend a path to history. Failures are ignored (history is optional)."""
    normalized = _normalize_source(source)
    if not normalized:
        return
    path = history_file or default_history_path()
    existing = [item for item in load_recent_paths(path) if item != normalized]
    payload = {"paths": [normalized, *existing][:MAX_RECENT_PATHS]}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError:
        return
