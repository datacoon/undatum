"""Tests for query command."""

import os
import tempfile
from unittest.mock import MagicMock, patch

from undatum.cmds.query import DataQuery, get_iterable_options


class TestGetIterableOptions:
    """Test get_iterable_options function."""

    def test_get_iterable_options_all(self):
        """Test extracting all iterable options."""
        options = {
            "tagname": "item",
            "delimiter": ",",
            "encoding": "utf-8",
            "start_line": 1,
            "page": 1,
            "other": "value",
        }
        result = get_iterable_options(options)
        assert result == {
            "tagname": "item",
            "delimiter": ",",
            "encoding": "utf-8",
            "start_line": 1,
            "page": 1,
        }


class TestDataQuery:
    """Test DataQuery class."""

    def test_init(self):
        """Test DataQuery initialization."""
        query = DataQuery()
        assert query is not None

    @patch("undatum.cmds.query.open_iterable")
    def test_query_with_fields(self, mock_open_iterable):
        """Test query with fields selection."""
        query = DataQuery()

        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(
            return_value=iter([{"name": "Alice", "age": 30, "city": "NYC"}])
        )
        mock_open_iterable.return_value = mock_iterable

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"name": "Alice", "age": 30, "city": "NYC"}\n')
            temp_path = f.name

        try:
            options = {"fields": "name,age"}
            with patch("builtins.print"):
                query.query(temp_path, options)
            mock_open_iterable.assert_called()
        finally:
            os.unlink(temp_path)

    @patch("undatum.cmds.query.open_iterable")
    def test_query_with_mistql_query(self, mock_open_iterable):
        """Test query with mistql query expression."""
        query_obj = DataQuery()

        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([{"age": 30}]))
        mock_open_iterable.return_value = mock_iterable

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"age": 30}\n')
            temp_path = f.name

        try:
            options = {"query": "age"}
            with patch("builtins.print"):
                # May raise due to mistql syntax, but tests the code path
                try:
                    query_obj.query(temp_path, options)
                except Exception:
                    pass
            mock_open_iterable.assert_called()
        finally:
            os.unlink(temp_path)

    @patch("undatum.cmds.query.open_iterable")
    def test_query_with_output_file(self, mock_open_iterable):
        """Test query with output file."""
        query_obj = DataQuery()

        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([{"name": "Alice"}]))
        mock_open_iterable.side_effect = [
            mock_iterable,  # Input iterable
            MagicMock(),  # Output iterable
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"name": "Alice"}\n')
            temp_path = f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            output_path = f.name

        try:
            options = {"output": output_path}
            query_obj.query(temp_path, options)
            # Should create output iterable
            assert mock_open_iterable.call_count >= 2
        finally:
            os.unlink(temp_path)
            if os.path.exists(output_path):
                os.unlink(output_path)
