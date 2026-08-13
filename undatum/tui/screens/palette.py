"""Command palette: actions with equivalent CLI templates."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Input, OptionList, Static
from textual.widgets.option_list import Option

from ..actions import TuiAction, filter_actions, render_cli
from ..session import SessionState


class PaletteScreen(ModalScreen[str | None]):
    """Search TUI actions. Enter runs the highlighted row."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(self, session: SessionState | None, field: str = "FIELD") -> None:
        super().__init__()
        self._session = session
        self._field = field or "FIELD"
        self._visible: list[TuiAction] = []

    def compose(self) -> ComposeResult:
        with Vertical(id="palette-dialog"):
            yield Static("Command palette", id="palette-title")
            yield Input(placeholder="Filter actions…", id="palette-search")
            yield OptionList(id="palette-list")
            yield Static("Enter to run · Esc to cancel", id="palette-hint")

    def on_mount(self) -> None:
        self._rebuild("")
        self.query_one("#palette-search", Input).focus()

    def _rebuild(self, query: str) -> None:
        self._visible = filter_actions(query)
        option_list = self.query_one("#palette-list", OptionList)
        option_list.clear_options()
        options = [
            Option(
                f"{action.title}  ·  {render_cli(action, self._session, self._field)}", id=action.id
            )
            for action in self._visible
        ]
        if options:
            option_list.add_options(options)
            option_list.highlighted = 0

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "palette-search":
            self._rebuild(event.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "palette-search":
            self._select_highlighted()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        option_id = event.option.id
        self.dismiss(str(option_id) if option_id else None)

    def _select_highlighted(self) -> None:
        option_list = self.query_one("#palette-list", OptionList)
        if not self._visible:
            return
        index = option_list.highlighted
        if index is None:
            index = 0
        if index < 0 or index >= len(self._visible):
            return
        self.dismiss(self._visible[index].id)

    def action_cancel(self) -> None:
        self.dismiss(None)
