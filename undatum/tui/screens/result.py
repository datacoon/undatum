"""Modal table for profile and frequency results."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import DataTable, Static


class ResultScreen(ModalScreen[None]):
    """Show a read-only result table and the equivalent CLI command."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
    ]

    def __init__(self, title: str, headers: list[str], rows: list[list[str]], cli: str) -> None:
        super().__init__()
        self._title = title
        self._headers = headers
        self._rows = rows
        self._cli = cli

    def compose(self) -> ComposeResult:
        with Vertical(id="result-dialog"):
            yield Static(self._title, id="result-title")
            yield DataTable(id="result-table", cursor_type="row")
            yield Static(f"$ {self._cli}", id="result-cli")
            yield Static("q / Esc to close", id="result-hint")

    def on_mount(self) -> None:
        table = self.query_one("#result-table", DataTable)
        table.cursor_type = "row"
        if self._headers:
            table.add_columns(*self._headers)
            for row in self._rows:
                padded = list(row) + [""] * (len(self._headers) - len(row))
                table.add_row(*[str(cell) for cell in padded[: len(self._headers)]])

    def action_close(self) -> None:
        self.dismiss()
