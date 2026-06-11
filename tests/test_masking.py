"""Tests for data masking utilities."""

import pytest

from undatum.common.masking import (
    hash_value,
    mask_value,
    randomize_string,
    redact,
)


class TestRedact:
    """Test redact function."""

    def test_redact_string(self):
        """Test redacting a string value."""
        assert redact("sensitive data") == "***"
        assert redact("email@example.com") == "***"

    def test_redact_none(self):
        """Test redacting None value."""
        assert redact(None) == "***"

    def test_redact_custom_token(self):
        """Test redacting with custom token."""
        assert redact("data", token="[REDACTED]") == "[REDACTED]"
        assert redact(None, token="[REDACTED]") == "[REDACTED]"


class TestHashValue:
    """Test hash_value function."""

    def test_hash_value_string(self):
        """Test hashing a string value."""
        result = hash_value("test value")
        assert isinstance(result, str)
        assert len(result) == 16  # First 16 characters of hex digest

    def test_hash_value_none(self):
        """Test hashing None value."""
        assert hash_value(None) == ""

    def test_hash_value_deterministic(self):
        """Test that hashing is deterministic."""
        value = "test value"
        result1 = hash_value(value)
        result2 = hash_value(value)
        assert result1 == result2

    def test_hash_value_with_salt(self):
        """Test hashing with salt."""
        value = "test value"
        result1 = hash_value(value, salt="salt1")
        result2 = hash_value(value, salt="salt2")
        assert result1 != result2

    def test_hash_value_different_values(self):
        """Test that different values produce different hashes."""
        result1 = hash_value("value1")
        result2 = hash_value("value2")
        assert result1 != result2

    def test_hash_value_numeric(self):
        """Test hashing numeric value."""
        result = hash_value(12345)
        assert isinstance(result, str)
        assert len(result) == 16


class TestRandomizeString:
    """Test randomize_string function."""

    def test_randomize_string_basic(self):
        """Test randomizing a string."""
        original = "test string"
        result = randomize_string(original)
        assert isinstance(result, str)
        assert result != original
        # Should preserve approximate length
        assert abs(len(result) - len(original)) <= 2

    def test_randomize_string_none(self):
        """Test randomizing None value."""
        result = randomize_string(None)
        assert isinstance(result, str)
        # None returns empty string
        assert result == ""

    def test_randomize_string_custom_length(self):
        """Test randomizing with custom length."""
        original = "test"
        result = randomize_string(original, length=10)
        assert len(result) == 10

    def test_randomize_string_different_results(self):
        """Test that randomization produces different results."""
        original = "test string"
        result1 = randomize_string(original)
        result2 = randomize_string(original)
        # Results should be different (very high probability)
        assert result1 != result2

    def test_randomize_string_numeric(self):
        """Test randomizing numeric value."""
        result = randomize_string(12345)
        assert isinstance(result, str)


class TestMaskValue:
    """Test mask_value function."""

    def test_mask_value_redact(self):
        """Test masking with redact method."""
        result = mask_value("sensitive", method="redact")
        assert result == "***"

    def test_mask_value_hash(self):
        """Test masking with hash method."""
        result = mask_value("sensitive", method="hash")
        assert isinstance(result, str)
        assert len(result) == 16

    def test_mask_value_hash_with_salt(self):
        """Test masking with hash method and salt."""
        result = mask_value("sensitive", method="hash", salt="mysalt")
        assert isinstance(result, str)
        assert len(result) == 16

    def test_mask_value_randomize(self):
        """Test masking with randomize method."""
        result = mask_value("sensitive", method="randomize")
        assert isinstance(result, str)
        assert result != "sensitive"

    def test_mask_value_default_method(self):
        """Test masking with default method."""
        # mask_value requires method parameter, no default
        # Test with explicit redact
        result = mask_value("sensitive", method="redact")
        assert result == "***"

    def test_mask_value_none(self):
        """Test masking None value."""
        result = mask_value(None, method="redact")
        assert result == "***"

    def test_mask_value_invalid_method(self):
        """Test masking with invalid method."""
        # Should raise ValueError
        with pytest.raises(ValueError, match="Unknown masking method"):
            mask_value("sensitive", method="invalid")
