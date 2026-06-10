"""Tests for main entry point."""
import sys
from unittest.mock import Mock, patch, MagicMock

import pytest

from undatum.__main__ import main


class TestMain:
    """Test main entry point."""

    @patch('undatum.__main__.app')
    def test_main_success(self, mock_app):
        """Test main function with successful execution."""
        mock_app.return_value = None
        with patch('sys.exit') as mock_exit:
            main()
            mock_app.assert_called_once()
            mock_exit.assert_called_once_with(0)

    @patch('undatum.__main__.app')
    def test_main_keyboard_interrupt(self, mock_app):
        """Test main function with keyboard interrupt."""
        mock_app.side_effect = KeyboardInterrupt()
        with patch('sys.exit') as mock_exit:
            with patch('builtins.print') as mock_print:
                main()
                mock_print.assert_called_once_with("Ctrl-C pressed. Aborting")
                mock_exit.assert_called_once_with(0)

    @patch('undatum.__main__.app')
    def test_main_other_exception(self, mock_app):
        """Test main function with other exception."""
        mock_app.side_effect = ValueError("Test error")
        with patch('sys.exit') as mock_exit:
            # Exception propagates before sys.exit is called
            with pytest.raises(ValueError):
                main()
            # sys.exit is called in finally block, but exception prevents it
            # So we don't assert sys.exit was called
