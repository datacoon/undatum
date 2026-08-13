"""Tests for CLI defaults from config files and environment."""

import pytest
import yaml

from undatum.cmds.sniffer import Sniffer
from undatum.common.app_config import (
    clear_cli_defaults_cache,
    describe_cli_config,
    get_cli_defaults,
)
from undatum.common.command_utils import get_iterable_options
from undatum.utils import get_option


@pytest.fixture
def config_env(tmp_path, monkeypatch):
    """Isolate cwd/home and reset the defaults cache."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(
        "undatum.common.app_config.Path.home",
        staticmethod(lambda: home),
    )
    monkeypatch.chdir(tmp_path)
    for key in (
        "UNDATUM_ENGINE",
        "UNDATUM_THREADS",
        "UNDATUM_PROGRESS",
        "UNDATUM_ENCODING",
        "UNDATUM_DELIMITER",
        "UNDATUM_QUOTECHAR",
        "UNDATUM_FORMAT_OUT",
    ):
        monkeypatch.delenv(key, raising=False)
    clear_cli_defaults_cache()
    yield tmp_path, home
    clear_cli_defaults_cache()


def _write_yaml(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


@pytest.mark.use_cli_config
def test_project_yaml_defaults(config_env):
    tmp_path, _home = config_env
    _write_yaml(
        tmp_path / "undatum.yaml",
        {"defaults": {"engine": "python", "threads": 4, "delimiter": ";"}},
    )
    clear_cli_defaults_cache()
    defaults = get_cli_defaults()
    assert defaults["engine"] == "python"
    assert defaults["threads"] == 4
    assert defaults["delimiter"] == ";"


@pytest.mark.use_cli_config
def test_project_overrides_home(config_env):
    tmp_path, home = config_env
    _write_yaml(home / ".undatum" / "config.yaml", {"defaults": {"engine": "duckdb"}})
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"engine": "python"}})
    clear_cli_defaults_cache()
    assert get_cli_defaults()["engine"] == "python"


@pytest.mark.use_cli_config
def test_file_overrides_environment(config_env, monkeypatch):
    tmp_path, _home = config_env
    monkeypatch.setenv("UNDATUM_ENGINE", "duckdb")
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"engine": "python"}})
    clear_cli_defaults_cache()
    assert get_cli_defaults()["engine"] == "python"


@pytest.mark.use_cli_config
def test_cli_value_overrides_config(config_env):
    tmp_path, _home = config_env
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"engine": "python"}})
    clear_cli_defaults_cache()
    assert get_option({"engine": "duckdb"}, "engine") == "duckdb"
    assert get_option({"engine": None}, "engine") == "python"


@pytest.mark.use_cli_config
def test_explicit_none_keeps_delimiter_autodetect_without_config(config_env):
    assert get_option({"delimiter": None}, "delimiter") is None
    assert get_option({}, "delimiter") == ","


@pytest.mark.use_cli_config
def test_iterable_options_use_config_delimiter(config_env):
    tmp_path, _home = config_env
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"delimiter": "|"}})
    clear_cli_defaults_cache()
    assert get_iterable_options({"delimiter": None})["delimiter"] == "|"
    assert get_iterable_options({"delimiter": ","})["delimiter"] == ","


@pytest.mark.use_cli_config
def test_iterable_options_use_config_quotechar(config_env):
    tmp_path, _home = config_env
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"quotechar": "'"}})
    clear_cli_defaults_cache()
    assert get_iterable_options({"quotechar": None})["quotechar"] == "'"
    assert get_iterable_options({"quotechar": '"'})["quotechar"] == '"'


@pytest.mark.use_cli_config
def test_env_quotechar(config_env, monkeypatch):
    monkeypatch.setenv("UNDATUM_QUOTECHAR", "'")
    clear_cli_defaults_cache()
    assert get_cli_defaults()["quotechar"] == "'"


@pytest.mark.use_cli_config
def test_env_progress_and_threads(config_env, monkeypatch):
    monkeypatch.setenv("UNDATUM_THREADS", "8")
    monkeypatch.setenv("UNDATUM_PROGRESS", "true")
    clear_cli_defaults_cache()
    defaults = get_cli_defaults()
    assert defaults["threads"] == 8
    assert defaults["progress"] is True


@pytest.mark.use_cli_config
def test_config_show_lists_project_file(config_env):
    tmp_path, _home = config_env
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"format_out": "json"}})
    clear_cli_defaults_cache()
    described = describe_cli_config()
    assert described["files"]["project"].endswith("undatum.yaml")
    assert described["defaults"]["format_out"] == "json"


@pytest.mark.use_cli_config
def test_sniff_json_from_output_extension(config_env, sample_csv_file, capsys):
    output = config_env[0] / "sniff.json"
    Sniffer().sniff(sample_csv_file, {"output": str(output)})
    payload = output.read_text(encoding="utf-8")
    assert '"filetype"' in payload
    assert '"fields"' in payload
    capsys.readouterr()


@pytest.mark.use_cli_config
def test_config_show_cli(config_env):
    tmp_path, _home = config_env
    _write_yaml(tmp_path / "undatum.yaml", {"defaults": {"engine": "python"}})
    clear_cli_defaults_cache()
    from typer.testing import CliRunner

    from undatum.core import app

    result = CliRunner().invoke(app, ["config", "show"])
    assert result.exit_code == 0
    assert "python" in result.stdout
