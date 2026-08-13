"""SQL query modal. Runs against the source file via SqlExecutor."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Static, TextArea

from ..actions import DEFAULT_SQL_LIMIT

DEFAULT_SQL = f"SELECT * FROM data LIMIT {DEFAULT_SQL_LIMIT}"


class SqlScreen(ModalScreen[str | None]):
    """Edit a DuckDB query. Ctrl+Enter or F5 runs it."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+enter", "run", "Run", show=True),
        Binding("f5", "run", "Run", show=False),
    ]

    def __init__(self, value: str = DEFAULT_SQL) -> None:
        super().__init__()
        self._value = value or DEFAULT_SQL

    def compose(self) -> ComposeResult:
        with Vertical(id="sql-dialog"):
            yield Static("SQL against the source file (view: data)", id="sql-title")
            yield TextArea(self._value, id="sql-editor")
            yield Static("Ctrl+Enter or F5 to run · Esc to cancel", id="sql-hint")

    def on_mount(self) -> None:
        self.query_one("#sql-editor", TextArea).focus()

    def action_run(self) -> None:
        text = self.query_one("#sql-editor", TextArea).text
        self.dismiss(text)

    def action_cancel(self) -> None:
        self.dismiss(None)
