"""Tests for main entry point."""

import sys
from unittest.mock import patch

import pytest

from undatum.__main__ import main
from undatum.common.errors import UndatumError, ValidationError


class TestMain:
    """Test main entry point."""

    @patch("undatum.__main__.app")
    def test_main_success(self, mock_app):
        """Test main function with successful execution."""
        mock_app.return_value = None
        main()
        mock_app.assert_called_once()

    @patch("undatum.__main__.app")
    def test_main_keyboard_interrupt(self, mock_app):
        """Test main function with keyboard interrupt exits 0."""
        mock_app.side_effect = KeyboardInterrupt()
        with patch("builtins.print") as mock_print:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
            mock_print.assert_called_once_with("Ctrl-C pressed. Aborting", file=sys.stderr)

    @patch("undatum.__main__.app")
    def test_main_undatum_error(self, mock_app):
        """Test main function with UndatumError uses its exit code."""
        mock_app.side_effect = UndatumError("Test error", exit_code=2)
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 2

    @patch("undatum.__main__.app")
    def test_main_validation_error(self, mock_app):
        """Test main function with ValidationError exits 1."""
        mock_app.side_effect = ValidationError("Bad input")
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("undatum.__main__.app")
    def test_main_other_exception(self, mock_app):
        """Test main function handles unexpected exceptions gracefully."""
        mock_app.side_effect = ValueError("Test error")
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0
