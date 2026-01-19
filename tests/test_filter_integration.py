"""Integration tests for filter functionality in validator and selector commands."""
import os
import tempfile
import pytest

from undatum.cmds.validator import Validator
from undatum.cmds.selector import Selector


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(
        "name,age,city,active\n"
        "Alice,30,New York,true\n"
        "Bob,25,London,false\n"
        "Charlie,35,Paris,true\n"
        "Diana,28,Berlin,false\n"
    )
    return str(csv_file)


@pytest.fixture
def sample_jsonl_file(tmp_path):
    """Create a sample JSONL file for testing."""
    jsonl_file = tmp_path / "sample.jsonl"
    content = (
        '{"name": "Alice", "age": 30, "city": "New York", "active": true}\n'
        '{"name": "Bob", "age": 25, "city": "London", "active": false}\n'
        '{"name": "Charlie", "age": 35, "city": "Paris", "active": true}\n'
        '{"name": "Diana", "age": 28, "city": "Berlin", "active": false}\n'
    )
    jsonl_file.write_text(content)
    return str(jsonl_file)


class TestValidatorFilter:
    """Test validator command with filter expressions."""

    def test_validator_with_filter_csv(self, sample_csv_file, tmp_path):
        """Test validator with filter on CSV file."""
        validator = Validator()
        output_file = str(tmp_path / "output.csv")

        options = {
            'format_in': 'csv',
            'fields': 'age',
            'rule': 'integer',
            'filter': 'age >= 30',  # Filter for age >= 30
            'output': output_file,
            'mode': 'all',
            'zipfile': False,
        }

        # Should not raise exception
        validator.validate(sample_csv_file, options)

        # Check that output file was created
        assert os.path.exists(output_file)

    def test_validator_with_filter_jsonl(self, sample_jsonl_file, tmp_path):
        """Test validator with filter on JSONL file."""
        validator = Validator()
        output_file = str(tmp_path / "output.csv")

        options = {
            'format_in': 'jsonl',
            'fields': 'age',
            'rule': 'integer',
            'filter': 'age >= 30',  # Filter for age >= 30
            'output': output_file,
            'mode': 'all',
            'zipfile': False,
        }

        # Should not raise exception
        validator.validate(sample_jsonl_file, options)

        # Check that output file was created
        assert os.path.exists(output_file)

    def test_validator_with_filter_none(self, sample_csv_file, tmp_path):
        """Test validator with no filter (None)."""
        validator = Validator()
        output_file = str(tmp_path / "output.csv")

        options = {
            'format_in': 'csv',
            'fields': 'age',
            'rule': 'integer',
            'filter': None,  # No filter
            'output': output_file,
            'mode': 'all',
            'zipfile': False,
        }

        # Should not raise exception
        validator.validate(sample_csv_file, options)

        # Check that output file was created
        assert os.path.exists(output_file)


class TestSelectorFilter:
    """Test selector command with filter expressions."""

    def test_selector_select_with_filter(self, sample_jsonl_file, tmp_path):
        """Test selector select method with filter."""
        selector = Selector()
        output_file = str(tmp_path / "output.jsonl")

        options = {
            'format_in': 'jsonl',
            'fields': 'name,age',
            'filter': 'age >= 30',  # Filter for age >= 30
            'output': output_file,
        }

        # Should not raise exception
        selector.select(sample_jsonl_file, options)

        # Check that output file was created (if output specified)
        if output_file:
            assert os.path.exists(output_file)

    def test_selector_frequency_with_filter(self, sample_jsonl_file, tmp_path):
        """Test selector frequency method with filter."""
        selector = Selector()
        output_file = str(tmp_path / "output.csv")

        options = {
            'format_in': 'jsonl',
            'fields': 'city',
            'filter': 'age >= 30',  # Filter for age >= 30
            'output': output_file,
            'engine': 'iterable',
        }

        # Should not raise exception
        selector.frequency(sample_jsonl_file, options)

        # Check that output file was created
        assert os.path.exists(output_file)

    def test_selector_split_with_filter_csv(self, sample_csv_file, tmp_path):
        """Test selector split method with filter on CSV."""
        selector = Selector()

        options = {
            'format_in': 'csv',
            'fields': None,
            'chunksize': 2,
            'filter': 'age >= 30',  # Filter for age >= 30
            'zipfile': False,
            'gzipfile': False,
            'dirname': str(tmp_path),
        }

        # Should not raise exception
        # Note: This will create output files in tmp_path
        selector.split(sample_csv_file, options)

    def test_selector_split_with_filter_jsonl(self, sample_jsonl_file, tmp_path):
        """Test selector split method with filter on JSONL."""
        selector = Selector()

        options = {
            'format_in': 'jsonl',
            'fields': None,
            'chunksize': 2,
            'filter': 'age >= 30',  # Filter for age >= 30
            'zipfile': False,
            'gzipfile': False,
            'dirname': str(tmp_path),
        }

        # Should not raise exception
        selector.split(sample_jsonl_file, options)

    def test_selector_with_filter_none(self, sample_jsonl_file, tmp_path):
        """Test selector with no filter (None)."""
        selector = Selector()
        output_file = str(tmp_path / "output.jsonl")

        options = {
            'format_in': 'jsonl',
            'fields': 'name,age',
            'filter': None,  # No filter
            'output': output_file,
        }

        # Should not raise exception
        selector.select(sample_jsonl_file, options)
