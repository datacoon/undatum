"""Tests for sorter command."""
import tempfile
import os
from unittest.mock import patch, MagicMock

import pytest

from undatum.cmds.sorter import Sorter, _normalize_for_json, _get_sort_key


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
        d = {'key': 'value', 'uuid': uuid.uuid4()}
        result = _normalize_for_json(d)
        assert isinstance(result['uuid'], str)

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
        item = {'name': 'Alice', 'age': 30}
        result = _get_sort_key(item, ['name'])
        assert result == ('Alice',)

    def test_get_sort_key_multiple_fields(self):
        """Test getting sort key for multiple fields."""
        item = {'name': 'Alice', 'age': 30}
        result = _get_sort_key(item, ['name', 'age'])
        assert result == ('Alice', 30)

    def test_get_sort_key_numeric(self):
        """Test getting sort key with numeric field."""
        item = {'age': '30'}
        result = _get_sort_key(item, ['age'], numeric_fields=['age'])
        assert isinstance(result[0], float)

    def test_get_sort_key_not_dict(self):
        """Test getting sort key for non-dict item."""
        item = "not a dict"
        result = _get_sort_key(item, ['name'])
        assert result == item


class TestSorter:
    """Test Sorter class."""

    def test_init(self):
        """Test sorter initialization."""
        sorter = Sorter()
        assert sorter is not None
