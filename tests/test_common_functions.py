"""Tests for common utility functions."""

import pytest

from undatum.common.functions import (
    get_dict_value,
    get_dict_value_deep,
)


class TestGetDictValue:
    """Test get_dict_value function."""

    def test_get_dict_value_simple(self):
        """Test getting value from simple dictionary."""
        d = {"key": "value"}
        assert get_dict_value(d, "key") == "value"

    def test_get_dict_value_nested(self):
        """Test getting value from nested dictionary."""
        d = {"level1": {"level2": {"level3": "value"}}}
        assert get_dict_value(d, "level1.level2.level3") == "value"

    def test_get_dict_value_missing_key(self):
        """Test getting value with missing key."""
        d = {"key": "value"}
        with pytest.raises(KeyError):
            get_dict_value(d, "missing")

    def test_get_dict_value_nested_missing(self):
        """Test getting value with missing nested key."""
        d = {"level1": {"level2": "value"}}
        # When level2 is a string, trying to access level3 will raise TypeError
        with pytest.raises((KeyError, TypeError)):
            get_dict_value(d, "level1.level2.level3")


class TestGetDictValueDeep:
    """Test get_dict_value_deep function."""

    def test_get_dict_value_deep_simple(self):
        """Test getting value from simple dictionary."""
        d = {"key": "value"}
        assert get_dict_value_deep(d, "key") == "value"

    def test_get_dict_value_deep_nested(self):
        """Test getting value from nested dictionary."""
        d = {"level1": {"level2": {"level3": "value"}}}
        assert get_dict_value_deep(d, "level1.level2.level3") == "value"

    def test_get_dict_value_deep_missing_key(self):
        """Test getting value with missing key."""
        d = {"key": "value"}
        assert get_dict_value_deep(d, "missing") is None

    def test_get_dict_value_deep_nested_missing(self):
        """Test getting value with missing nested key."""
        d = {"level1": {"level2": "value"}}
        assert get_dict_value_deep(d, "level1.level2.level3") is None

    def test_get_dict_value_deep_with_list(self):
        """Test getting value from dictionary containing list."""
        d = {"items": [{"id": 1}, {"id": 2}]}
        assert get_dict_value_deep(d, "items.id") == 1  # Returns first item's value

    def test_get_dict_value_deep_with_list_as_array(self):
        """Test getting values from list as array."""
        d = {"items": [{"id": 1}, {"id": 2}]}
        result = get_dict_value_deep(d, "items.id", as_array=True)
        assert result == [1, 2]

    def test_get_dict_value_deep_nested_list(self):
        """Test getting value from nested list."""
        d = {"level1": [{"level2": {"level3": "value1"}}, {"level2": {"level3": "value2"}}]}
        assert get_dict_value_deep(d, "level1.level2.level3") == "value1"

    def test_get_dict_value_deep_nested_list_as_array(self):
        """Test getting values from nested list as array."""
        d = {"level1": [{"level2": {"level3": "value1"}}, {"level2": {"level3": "value2"}}]}
        result = get_dict_value_deep(d, "level1.level2.level3", as_array=True)
        assert result == ["value1", "value2"]

    def test_get_dict_value_deep_custom_splitter(self):
        """Test getting value with custom splitter."""
        d = {"level1": {"level2": "value"}}
        assert get_dict_value_deep(d, "level1/level2", splitter="/") == "value"

    def test_get_dict_value_deep_empty_list(self):
        """Test getting value from empty list."""
        d = {"items": []}
        assert get_dict_value_deep(d, "items.id") is None

    def test_get_dict_value_deep_list_missing_key(self):
        """Test getting value from list when key is missing."""
        d = {"items": [{"name": "item1"}, {"name": "item2"}]}
        assert get_dict_value_deep(d, "items.id") is None
