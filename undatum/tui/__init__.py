"""Optional interactive TUI for dataset exploration.

Importing this package does not require Textual. Widget modules under
``undatum.tui.app`` and ``undatum.tui.screens`` do.
"""

from .services import TuiServices
from .session import DEFAULT_SAMPLE_LIMIT, MAX_SAMPLE_LIMIT, SessionState

__all__ = [
    "DEFAULT_SAMPLE_LIMIT",
    "MAX_SAMPLE_LIMIT",
    "SessionState",
    "TuiServices",
]
