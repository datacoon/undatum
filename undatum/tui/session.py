"""Session state for the undatum TUI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

DEFAULT_SAMPLE_LIMIT = 200
MAX_SAMPLE_LIMIT = 5000


@dataclass
class SessionState:
    """In-memory TUI session. The grid holds a bounded sample, never the full file."""

    source: str
    options: dict[str, Any] = field(default_factory=dict)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    sample_limit: int = DEFAULT_SAMPLE_LIMIT
    headers: list[str] = field(default_factory=list)
    field_types: dict[str, str] = field(default_factory=dict)
    filter_expr: str | None = None
    last_cli: str | None = None
    format_name: str | None = None
    encoding: str | None = None
    truncated: bool = False

    def visible_rows(self) -> list[dict[str, Any]]:
        """Sample rows after the session filter, if any."""
        if not self.filter_expr:
            return self.sample_rows
        from ..common.filter import match_filter

        return [row for row in self.sample_rows if match_filter(row, self.filter_expr)]
