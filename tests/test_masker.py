"""Tests for masker command."""
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock

import pytest

from undatum.cmds.masker import Masker, get_iterable_options


class TestGetIterableOptions:
    """Test get_iterable_options function."""

    def test_get_iterable_options_all(self):
        """Test extracting all iterable options."""
        options = {
            'tagname': 'item',
            'delimiter': ',',
            'encoding': 'utf-8',
            'start_line': 1,
            'page': 1,
            'other': 'value'
        }
        result = get_iterable_options(options)
        assert result == {
            'tagname': 'item',
            'delimiter': ',',
            'encoding': 'utf-8',
            'start_line': 1,
            'page': 1
        }
        assert 'other' not in result

    def test_get_iterable_options_partial(self):
        """Test extracting partial iterable options."""
        options = {
            'delimiter': ';',
            'other': 'value'
        }
        result = get_iterable_options(options)
        assert result == {'delimiter': ';'}

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

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_missing_fields_option(self, mock_open_iterable, mock_is_s3_uri):
        """Test masking without fields option."""
        masker = Masker()
        with pytest.raises(ValueError, match="--fields option is required"):
            masker.mask('input.jsonl', 'output.jsonl', options={})

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_empty_fields(self, mock_open_iterable, mock_is_s3_uri):
        """Test masking with empty fields."""
        masker = Masker()
        with pytest.raises(ValueError, match="No valid fields specified"):
            masker.mask('input.jsonl', 'output.jsonl', options={'fields': '   ,  ,  '})

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_invalid_method(self, mock_open_iterable, mock_is_s3_uri):
        """Test masking with invalid method."""
        masker = Masker()
        with pytest.raises(ValueError, match="Invalid masking method"):
            masker.mask('input.jsonl', 'output.jsonl', options={
                'fields': 'email',
                'method': 'invalid'
            })

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_record(self, mock_open_iterable, mock_is_s3_uri):
        """Test _mask_record method."""
        masker = Masker()
        record = {'email': 'test@example.com', 'name': 'John'}
        fields_to_mask = ['email']
        
        result = masker._mask_record(record, fields_to_mask, 'redact')
        assert result['email'] == '***'
        assert result['name'] == 'John'

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_record_multiple_fields(self, mock_open_iterable, mock_is_s3_uri):
        """Test masking multiple fields."""
        masker = Masker()
        record = {'email': 'test@example.com', 'phone': '123-456-7890', 'name': 'John'}
        fields_to_mask = ['email', 'phone']
        
        result = masker._mask_record(record, fields_to_mask, 'redact')
        assert result['email'] == '***'
        assert result['phone'] == '***'
        assert result['name'] == 'John'

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_record_field_not_present(self, mock_open_iterable, mock_is_s3_uri):
        """Test masking field that doesn't exist in record."""
        masker = Masker()
        record = {'name': 'John'}
        fields_to_mask = ['email']
        
        result = masker._mask_record(record, fields_to_mask, 'redact')
        assert 'email' not in result
        assert result['name'] == 'John'

    @patch('undatum.cmds.masker.is_s3_uri', return_value=False)
    @patch('undatum.cmds.masker.open_iterable')
    def test_mask_record_with_salt(self, mock_open_iterable, mock_is_s3_uri):
        """Test masking with salt."""
        masker = Masker()
        record = {'email': 'test@example.com'}
        fields_to_mask = ['email']
        
        result = masker._mask_record(record, fields_to_mask, 'hash', salt='mysalt')
        assert result['email'] != 'test@example.com'
        assert isinstance(result['email'], str)
        assert len(result['email']) == 16  # Hash length
