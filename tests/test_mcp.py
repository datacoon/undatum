"""Tests for undatum's MCP server command and factory."""

import json

import pytest
from typer.testing import CliRunner

from undatum.core import app

runner = CliRunner()


class TestMcpCli:
    def test_tools_listing(self):
        result = runner.invoke(app, ["mcp", "tools"])
        assert result.exit_code == 0
        assert "query_sql" in result.stdout

    def test_tools_listing_json(self):
        result = runner.invoke(app, ["mcp", "tools", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        names = {d["name"] for d in data}
        assert "detect_format" in names
        assert "query_sql" in names

    def test_help_exposes_serve(self):
        result = runner.invoke(app, ["mcp", "--help"])
        assert result.exit_code == 0
        assert "serve" in result.stdout


class TestMcpServerFactory:
    def test_create_server_or_clean_import_error(self):
        from undatum.mcp.server import create_mcp_server

        try:
            server = create_mcp_server()
        except ImportError as e:
            # mcp package not installed; message should guide installation
            assert "mcp" in str(e).lower()
            pytest.skip("mcp package not installed")
        else:
            assert server is not None
