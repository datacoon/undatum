"""Textual application for ``undatum tui``."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from textual.app import App

from .services import TuiServices, clamp_sample_limit
from .session import SessionState


class UndatumApp(App[None]):
    """Interactive sample explorer. Does not edit source cells."""

    TITLE = "undatum tui"
    ENABLE_COMMAND_PALETTE = False
    CSS = """
    #body {
        height: 1fr;
    }
    #fields-pane {
        width: 24;
        border: solid $primary;
        padding: 0 1;
    }
    #fields-title {
        text-style: bold;
        padding-bottom: 1;
    }
    #grid {
        width: 1fr;
        border: solid $primary;
    }
    #status, #cli {
        padding: 0 1;
        height: 1;
    }
    #browse-hint {
        padding: 0 1;
        height: auto;
    }
    #cli {
        color: $text-muted;
    }
    #help-dialog, #prompt-dialog, #result-dialog {
        width: 72;
        height: auto;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
        margin: 2 4;
    }
    #result-dialog {
        width: 90%;
        max-height: 80%;
    }
    #result-table {
        height: 16;
        margin: 1 0;
    }
    #prompt-input {
        margin: 1 0;
    }
    #sql-dialog, #palette-dialog {
        width: 90%;
        height: auto;
        max-height: 80%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
        margin: 2 4;
    }
    #sql-editor {
        height: 10;
        margin: 1 0;
    }
    #palette-list {
        height: 12;
        margin: 1 0;
    }
    #recent-files {
        height: 7;
        border: solid $primary;
    }
    #files {
        height: 1fr;
    }
    """

    def __init__(
        self,
        session: SessionState | None = None,
        start_dir: str = ".",
        options: dict[str, Any] | None = None,
        limit: int | None = None,
        history_file: str | None = None,
    ) -> None:
        super().__init__()
        self.session = session
        self.start_dir = start_dir
        self.options = dict(options or {})
        self.limit = clamp_sample_limit(limit)
        self.history_file = Path(history_file) if history_file else None

    def on_mount(self) -> None:
        from .screens.browse import BrowseScreen
        from .screens.preview import PreviewScreen

        if self.session is not None:
            self.push_screen(PreviewScreen(self.session))
        else:
            self.push_screen(BrowseScreen(self.start_dir, history_file=self.history_file))

    def open_dataset(self, path: str) -> None:
        """Load a file sample and show the preview screen."""
        from .screens.preview import PreviewScreen

        self.session = TuiServices().load_sample(path, self.options, self.limit)
        from .history import record_recent_path

        record_recent_path(path, self.history_file)
        self.push_screen(PreviewScreen(self.session))


def run_tui(
    path: str | None,
    options: dict[str, Any] | None = None,
    limit: int | None = None,
) -> None:
    """Start the Textual app (requires a TTY and the tui extra)."""
    options = dict(options or {})
    session = None
    start_dir = "."
    if path:
        if os.path.isdir(path):
            start_dir = path
        else:
            session = TuiServices().load_sample(path, options, limit)
            from .history import record_recent_path

            record_recent_path(path)
    UndatumApp(session=session, start_dir=start_dir, options=options, limit=limit).run()
