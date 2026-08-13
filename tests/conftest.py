"""Pytest configuration and fixtures."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "benchmark: marks tests as benchmarks")
    config.addinivalue_line(
        "markers", "use_cli_config: allow reading undatum.yaml / env CLI defaults"
    )


@pytest.fixture(autouse=True)
def _isolate_cli_defaults(request, monkeypatch):
    """Keep developer/user config files from leaking into tests."""
    if request.node.get_closest_marker("use_cli_config"):
        return
    monkeypatch.setattr("undatum.common.app_config.get_cli_defaults", lambda: {})


try:
    import pytest_benchmark  # noqa: F401
except ImportError:

    @pytest.fixture
    def benchmark():
        """Skip benchmarks when pytest-benchmark is unavailable."""
        pytest.skip("pytest-benchmark is not installed")


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("name,age,city\nAlice,30,New York\nBob,25,London\n")
    return str(csv_file)


@pytest.fixture
def sample_jsonl_file(tmp_path):
    """Create a sample JSONL file for testing."""
    jsonl_file = tmp_path / "sample.jsonl"
    content = '{"name": "Alice", "age": 30, "city": "New York"}\n{"name": "Bob", "age": 25, "city": "London"}\n'
    jsonl_file.write_text(content)
    return str(jsonl_file)
