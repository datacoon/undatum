"""Tests for sorter command."""

import json

import pytest

from undatum.cmds.sorter import Sorter, _get_sort_key, _normalize_for_json


class TestNormalizeForJson:
    """Test _normalize_for_json function."""

    def test_normalize_for_json_uuid(self):
        """Test normalizing UUID."""
        import uuid

        test_uuid = uuid.uuid4()
        result = _normalize_for_json(test_uuid)
        assert isinstance(result, str)
        assert result == str(test_uuid)

    def test_normalize_for_json_dict(self):
        """Test normalizing dictionary."""
        import uuid

        d = {"key": "value", "uuid": uuid.uuid4()}
        result = _normalize_for_json(d)
        assert isinstance(result["uuid"], str)

    def test_normalize_for_json_list(self):
        """Test normalizing list."""
        try:
            import uuid

            lst = [uuid.uuid4(), uuid.uuid4()]
            result = _normalize_for_json(lst)
            assert all(isinstance(item, str) for item in result)
        except ImportError:
            pytest.skip("uuid module not available")


class TestGetSortKey:
    """Test _get_sort_key function."""

    def test_get_sort_key_simple(self):
        """Test getting sort key for simple item."""
        item = {"name": "Alice", "age": 30}
        result = _get_sort_key(item, ["name"])
        assert result < _get_sort_key({"name": "Bob"}, ["name"])

    def test_get_sort_key_multiple_fields(self):
        """Test getting sort key for multiple fields."""
        item = {"name": "Alice", "age": 30}
        result = _get_sort_key(item, ["name", "age"])
        assert result > _get_sort_key({"name": "Alice", "age": 25}, ["name", "age"])

    def test_get_sort_key_numeric(self):
        """Test getting sort key with numeric field."""
        item = {"age": "30"}
        result = _get_sort_key(item, ["age"], numeric_fields=["age"])
        assert result < _get_sort_key({"age": "100"}, ["age"], numeric_fields=["age"])

    def test_get_sort_key_not_dict(self):
        """Test getting sort key for non-dict item."""
        item = "not a dict"
        result = _get_sort_key(item, ["name"])
        assert result > _get_sort_key({"name": "Alice"}, ["name"])


class TestSorter:
    """Test Sorter class."""

    def test_init(self):
        """Test sorter initialization."""
        sorter = Sorter()
        assert sorter is not None


@pytest.mark.parametrize("low_memory", [False, True])
@pytest.mark.parametrize("numeric", [False, True])
@pytest.mark.parametrize("desc", [False, True])
def test_cli_and_sdk_sort_agree_on_null_and_mixed_values(
    tmp_path, monkeypatch, low_memory, numeric, desc
):
    from typer.testing import CliRunner

    from undatum import Dataset
    from undatum.core import app

    rows = [{"id": i, "n": n} for i, n in enumerate([2, None, 1, "bad", "10", ""])]
    rows.append({"id": 6})
    src = tmp_path / "input.jsonl"
    src.write_text("".join(json.dumps(row) + "\n" for row in rows))
    args = ["sort", str(src), "--by", "n", "-e", "python"]
    if low_memory:
        from undatum.cmds import sorter

        monkeypatch.setattr(sorter, "DEFAULT_RUN_SIZE", 2)
        args += ["--low-memory"]
    if numeric:
        args += ["--numeric", "n"]
    if desc:
        args += ["--desc"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    expected = Dataset.read(str(src)).sort("n", numeric=numeric, desc=desc).collect()
    assert actual == expected
    assert sorted(r["id"] for r in actual) == list(range(7))
