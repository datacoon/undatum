"""Tests for head, tail, table, and cat commands."""
import tempfile
import os
from unittest.mock import patch, MagicMock

import pytest

from undatum.cmds.head import Head, get_iterable_options as head_get_iterable_options
from undatum.cmds.tail import Tail, get_iterable_options as tail_get_iterable_options
from undatum.cmds.table import TableFormatter, get_iterable_options as table_get_iterable_options
from undatum.cmds.cat import Cat, get_iterable_options as cat_get_iterable_options


class TestHead:
    """Test Head class."""

    def test_init(self):
        """Test Head initialization."""
        head = Head()
        assert head is not None

    @patch('undatum.cmds.head.open_iterable')
    @patch('undatum.cmds.head.DataWriter')
    def test_head_basic(self, mock_writer_class, mock_open_iterable):
        """Test basic head operation."""
        head = Head()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}, {'id': 5}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": 1}\n{"id": 2}\n{"id": 3}\n{"id": 4}\n{"id": 5}\n')
            temp_path = f.name

        try:
            options = {'n': 3}
            head.head(temp_path, options)
            mock_writer.write_items.assert_called_once()
            items = mock_writer.write_items.call_args[0][0]
            assert len(items) == 3
        finally:
            os.unlink(temp_path)


class TestTail:
    """Test Tail class."""

    def test_init(self):
        """Test Tail initialization."""
        tail = Tail()
        assert tail is not None

    @patch('undatum.cmds.tail.open_iterable')
    @patch('undatum.cmds.tail.DataWriter')
    def test_tail_basic(self, mock_writer_class, mock_open_iterable):
        """Test basic tail operation."""
        tail = Tail()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}, {'id': 5}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": 1}\n{"id": 2}\n{"id": 3}\n{"id": 4}\n{"id": 5}\n')
            temp_path = f.name

        try:
            options = {'n': 3}
            tail.tail(temp_path, options)
            mock_writer.write_items.assert_called_once()
            items = mock_writer.write_items.call_args[0][0]
            assert len(items) == 3
            assert items[0]['id'] == 3  # Last 3 items: 3, 4, 5
        finally:
            os.unlink(temp_path)


class TestTableFormatter:
    """Test TableFormatter class."""

    def test_init(self):
        """Test TableFormatter initialization."""
        formatter = TableFormatter()
        assert formatter.console is not None

    @patch('undatum.cmds.table.open_iterable')
    def test_table_basic(self, mock_open_iterable):
        """Test basic table formatting."""
        formatter = TableFormatter()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}\n')
            temp_path = f.name

        try:
            options = {'limit': 10}
            # Table formatter prints to console, so we just verify it doesn't raise
            formatter.table(temp_path, options)
            mock_open_iterable.assert_called()
        finally:
            os.unlink(temp_path)

    @patch('undatum.cmds.table.open_iterable')
    def test_table_with_fields(self, mock_open_iterable):
        """Test table with field selection."""
        formatter = TableFormatter()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'name': 'Alice', 'age': 30, 'city': 'NYC'}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"name": "Alice", "age": 30, "city": "NYC"}\n')
            temp_path = f.name

        try:
            options = {'fields': 'name,age', 'limit': 10}
            with patch('builtins.print'):
                formatter.table(temp_path, options)
        finally:
            os.unlink(temp_path)


class TestCat:
    """Test Cat class."""

    def test_init(self):
        """Test Cat initialization."""
        cat = Cat()
        assert cat is not None

    @patch('undatum.cmds.cat.open_iterable')
    @patch('undatum.cmds.cat.DataWriter')
    def test_cat_rows_mode(self, mock_writer_class, mock_open_iterable):
        """Test cat in rows mode."""
        cat = Cat()
        
        # Mock iterable to return different items for each file
        def mock_iterable_side_effect(*args, **kwargs):
            mock_iter = MagicMock()
            # First call returns items for first file, second call for second file
            if not hasattr(mock_iterable_side_effect, 'call_count'):
                mock_iterable_side_effect.call_count = 0
            if mock_iterable_side_effect.call_count == 0:
                mock_iter.__iter__ = MagicMock(return_value=iter([{'id': 1}, {'id': 2}]))
            else:
                mock_iter.__iter__ = MagicMock(return_value=iter([{'id': 3}, {'id': 4}]))
            mock_iterable_side_effect.call_count += 1
            return mock_iter
        
        mock_open_iterable.side_effect = mock_iterable_side_effect
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f1:
            f1.write('{"id": 1}\n{"id": 2}\n')
            temp_path1 = f1.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f2:
            f2.write('{"id": 3}\n{"id": 4}\n')
            temp_path2 = f2.name

        try:
            options = {'mode': 'rows'}
            cat.cat([temp_path1, temp_path2], options)
            mock_writer.write_items.assert_called_once()
            items = mock_writer.write_items.call_args[0][0]
            assert len(items) == 4  # 2 from each file
        finally:
            os.unlink(temp_path1)
            os.unlink(temp_path2)

    @patch('undatum.cmds.cat.open_iterable')
    @patch('undatum.cmds.cat.DataWriter')
    def test_cat_columns_mode(self, mock_writer_class, mock_open_iterable):
        """Test cat in columns mode."""
        cat = Cat()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'name': 'Alice'}, {'name': 'Bob'}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f1:
            f1.write('{"name": "Alice"}\n{"name": "Bob"}\n')
            temp_path1 = f1.name

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f2:
            f2.write('{"age": 30}\n{"age": 25}\n')
            temp_path2 = f2.name

        try:
            options = {'mode': 'columns'}
            cat.cat([temp_path1, temp_path2], options)
            mock_writer.write_items.assert_called_once()
        finally:
            os.unlink(temp_path1)
            os.unlink(temp_path2)

    def test_cat_no_files(self):
        """Test cat with no files."""
        cat = Cat()
        cat.cat([], {})
        # Should handle gracefully
