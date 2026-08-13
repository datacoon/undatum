"""The experimental MistQL ``query`` command has been removed."""

from typer.testing import CliRunner

from undatum.core import app


def test_query_command_removed():
    result = CliRunner().invoke(app, ["query", "--help"])
    assert result.exit_code != 0
    combined = (result.stdout or "") + (result.stderr or "")
    assert "no such command" in combined.lower() or "no such command" in str(result).lower()
    assert "MistQL" not in combined
