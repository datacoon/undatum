"""File picker when ``undatum tui`` is started without a file."""

from __future__ import annotations

from pathlib import Path

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import DirectoryTree, Footer, Header, OptionList, Static
from textual.widgets.option_list import Option

from ..history import load_recent_paths


class BrowseScreen(Screen):
    """Pick a local file to preview."""

    BINDINGS = [
        Binding("q", "app.quit", "Quit"),
        Binding("question_mark", "help", "Help"),
        Binding("colon", "palette", "Palette"),
        Binding("u", "open_uri", "URI"),
    ]

    def __init__(self, start_dir: str = ".", history_file: Path | None = None) -> None:
        super().__init__()
        self.start_dir = start_dir
        self.history_file = history_file
        self._recent = load_recent_paths(history_file)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(
            "Select a data file, pick Recent, or press u for a path / s3:// URI.",
            id="browse-hint",
        )
        if self._recent:
            yield Static("Recent", id="recent-title")
            yield OptionList(
                *[Option(path, id=f"recent-{index}") for index, path in enumerate(self._recent)],
                id="recent-files",
            )
        yield DirectoryTree(self.start_dir, id="files")
        yield Footer()

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        self._open_path(str(Path(event.path)))

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id != "recent-files":
            return
        index = event.option_index
        if index < 0 or index >= len(self._recent):
            return
        self._open_path(self._recent[index])

    def _open_path(self, path: str) -> None:
        try:
            self.app.open_dataset(path)  # type: ignore[attr-defined]
        except Exception as exc:
            self.notify(str(exc), severity="error")

    def action_help(self) -> None:
        from .help import HelpScreen

        self.app.push_screen(HelpScreen())

    def action_open_uri(self) -> None:
        from .prompt import PromptScreen

        def _open(value: str | None) -> None:
            if value is None:
                return
            path = value.strip()
            if not path:
                return
            self._open_path(path)

        self.app.push_screen(
            PromptScreen("Path or s3:// URI", placeholder="s3://bucket/data.csv"),
            _open,
        )

    def action_palette(self) -> None:
        from .palette import PaletteScreen

        def _run(action_id: str | None) -> None:
            if action_id == "help":
                self.action_help()
            elif action_id == "open":
                return

        self.app.push_screen(PaletteScreen(None), _run)
