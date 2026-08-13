"""CLI and optional Textual tests for undatum tui."""

import sys

import pytest
from typer.testing import CliRunner

from undatum.__main__ import main
from undatum.core import app


def test_tui_help_lists_command():
    result = CliRunner().invoke(app, ["tui", "--help"])
    assert result.exit_code == 0, result.stdout
    assert "sample" in result.stdout.lower()
    assert "Textual" in result.stdout
    assert "--flatten-nested" in result.stdout
    assert "keep-nested" in result.stdout


def test_tui_missing_extra_exits_2(monkeypatch, sample_csv_file):
    from undatum.common.errors import DependencyError

    def boom():
        raise DependencyError(
            "textual",
            feature="TUI",
            install_command='pip install "undatum[tui]"',
        )

    monkeypatch.setattr("undatum.cli.tui_cli.require_tui_dependencies", boom)
    monkeypatch.setattr(sys, "argv", ["undatum", "tui", sample_csv_file])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2


def test_tui_non_tty_exits_1(monkeypatch, sample_csv_file):
    monkeypatch.setattr("undatum.cli.tui_cli.require_tui_dependencies", lambda: None)
    monkeypatch.setattr("undatum.cli.tui_cli._is_tty", lambda: False)
    monkeypatch.setattr(sys, "argv", ["undatum", "tui", sample_csv_file])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1


def test_tui_not_a_pipeline_command():
    from undatum.common.pipeline_parser import PipelineSpec, validate_pipeline

    spec = PipelineSpec([{"name": "explore", "command": "tui", "args": {}}])
    errors = validate_pipeline(spec)
    assert any("unknown command" in error.lower() for error in errors)


def test_preview_pilot_shows_sample(sample_csv_file):
    pytest.importorskip("textual")
    import asyncio

    from textual.widgets import DataTable, Static

    from undatum.tui.app import UndatumApp
    from undatum.tui.services import TuiServices

    session = TuiServices().load_sample(sample_csv_file, {}, 200)
    app = UndatumApp(session=session)

    async def _run():
        async with app.run_test() as pilot:
            await pilot.pause()
            table = app.screen.query_one("#grid", DataTable)
            assert table.row_count == 2
            status = str(app.screen.query_one("#status", Static).render())
            assert "sample" in status.lower()

    asyncio.run(_run())


def test_preview_pilot_filters_sample(sample_csv_file):
    pytest.importorskip("textual")
    import asyncio

    from textual.widgets import DataTable, Input

    from undatum.tui.app import UndatumApp
    from undatum.tui.services import TuiServices

    session = TuiServices().load_sample(sample_csv_file, {}, 200)
    app = UndatumApp(session=session)

    async def _run():
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.press("slash")
            await pilot.pause()
            prompt = app.screen.query_one("#prompt-input", Input)
            prompt.value = 'name == "Alice"'
            await prompt.action_submit()
            await pilot.pause()
            table = app.screen.query_one("#grid", DataTable)
            assert table.row_count == 1

    asyncio.run(_run())


def test_preview_pilot_opens_palette(sample_csv_file):
    pytest.importorskip("textual")
    import asyncio

    from textual.widgets import OptionList

    from undatum.tui.app import UndatumApp
    from undatum.tui.services import TuiServices

    session = TuiServices().load_sample(sample_csv_file, {}, 200)
    app = UndatumApp(session=session)

    async def _run():
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.press("colon")
            await pilot.pause()
            palette = app.screen.query_one("#palette-list", OptionList)
            assert palette.option_count >= 1

    asyncio.run(_run())


def test_preview_pilot_validate_sample(sample_csv_file):
    pytest.importorskip("textual")
    import asyncio

    from textual.widgets import DataTable, Input

    from undatum.tui.app import UndatumApp
    from undatum.tui.services import TuiServices

    session = TuiServices().load_sample(sample_csv_file, {}, 200)
    app = UndatumApp(session=session)

    async def _run():
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.pause()
            prompt = app.screen.query_one("#prompt-input", Input)
            prompt.value = ""
            await prompt.action_submit()
            await pilot.pause()
            table = app.screen.query_one("#result-table", DataTable)
            assert table.row_count >= 1

    asyncio.run(_run())
