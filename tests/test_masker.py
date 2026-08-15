"""Tests for masker command."""

from unittest.mock import patch

import pytest

from undatum.cmds.masker import Masker, get_iterable_options
from undatum.common.errors import ValidationError


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
        assert "other" not in result

    def test_get_iterable_options_partial(self):
        """Test extracting partial iterable options."""
        options = {"delimiter": ";", "other": "value"}
        result = get_iterable_options(options)
        assert result == {"delimiter": ";"}

    def test_get_iterable_options_empty(self):
        """Test extracting from empty options."""
        options = {}
        result = get_iterable_options(options)
        assert result == {}


class TestMasker:
    """Test Masker class."""

    def test_init(self):
        """Test masker initialization."""
        masker = Masker()
        assert masker is not None

    @patch("undatum.cmds.masker.open_iterable")
    def test_mask_missing_fields_option(self, mock_open_iterable, tmp_path):
        """Test masking without fields option."""
        input_file = tmp_path / "input.jsonl"
        input_file.write_text('{"email": "a@b.com"}\n')
        masker = Masker()
        with pytest.raises(ValidationError, match="--fields option is required"):
            masker.mask(str(input_file), "output.jsonl", options={})

    @patch("undatum.cmds.masker.open_iterable")
    def test_mask_empty_fields(self, mock_open_iterable, tmp_path):
        """Test masking with empty fields."""
        input_file = tmp_path / "input.jsonl"
        input_file.write_text('{"email": "a@b.com"}\n')
        masker = Masker()
        with pytest.raises(ValidationError, match="No valid fields specified"):
            masker.mask(str(input_file), "output.jsonl", options={"fields": "   ,  ,  "})

    @patch("undatum.cmds.masker.open_iterable")
    def test_mask_invalid_method(self, mock_open_iterable, tmp_path):
        """Test masking with invalid method."""
        input_file = tmp_path / "input.jsonl"
        input_file.write_text('{"email": "a@b.com"}\n')
        masker = Masker()
        with pytest.raises(ValidationError, match="Invalid masking method"):
            masker.mask(
                str(input_file), "output.jsonl", options={"fields": "email", "method": "invalid"}
            )

    def test_mask_record(self):
        """Test _mask_record method."""
        masker = Masker()
        record = {"email": "test@example.com", "name": "John"}
        fields_to_mask = ["email"]

        result = masker._mask_record(record, fields_to_mask, "redact")
        assert result["email"] == "***"
        assert result["name"] == "John"

    def test_mask_record_multiple_fields(self):
        """Test masking multiple fields."""
        masker = Masker()
        record = {"email": "test@example.com", "phone": "123-456-7890", "name": "John"}
        fields_to_mask = ["email", "phone"]

        result = masker._mask_record(record, fields_to_mask, "redact")
        assert result["email"] == "***"
        assert result["phone"] == "***"
        assert result["name"] == "John"

    def test_mask_record_field_not_present(self):
        """Test masking field that doesn't exist in record."""
        masker = Masker()
        record = {"name": "John"}
        fields_to_mask = ["email"]

        result = masker._mask_record(record, fields_to_mask, "redact")
        assert "email" not in result
        assert result["name"] == "John"

    def test_mask_record_with_salt(self):
        """Test masking with salt."""
        masker = Masker()
        record = {"email": "test@example.com"}
        fields_to_mask = ["email"]

        result = masker._mask_record(record, fields_to_mask, "hash", salt="mysalt")
        assert result["email"] != "test@example.com"
        assert isinstance(result["email"], str)
        assert len(result["email"]) == 16  # Hash length
