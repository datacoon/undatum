"""Tests for progress indication utilities."""
import sys
from unittest.mock import Mock, patch, MagicMock

import pytest

from undatum.common.progress import (
    is_tty,
    progress_bar,
    update_progress,
    set_progress_description,
    set_progress_postfix,
    wrap_iterable,
)


class TestIsTty:
    """Test is_tty function."""

    def test_is_tty_with_real_stdout(self):
        """Test is_tty with real stdout."""
        # This will depend on the actual environment
        result = is_tty()
        assert isinstance(result, bool)

    @patch('sys.stdout')
    def test_is_tty_with_mock_stdout(self, mock_stdout):
        """Test is_tty with mocked stdout."""
        mock_stdout.isatty.return_value = True
        assert is_tty() is True

        mock_stdout.isatty.return_value = False
        assert is_tty() is False

    @patch('sys.stdout')
    def test_is_tty_no_isatty_method(self, mock_stdout):
        """Test is_tty when stdout doesn't have isatty method."""
        del mock_stdout.isatty
        assert is_tty() is False


class TestProgressBar:
    """Test progress_bar context manager."""

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=True)
    @patch('undatum.common.progress.tqdm')
    def test_progress_bar_enabled(self, mock_tqdm, mock_is_tty):
        """Test progress bar when enabled."""
        mock_pbar = MagicMock()
        mock_tqdm.return_value = mock_pbar

        with progress_bar(total=100, desc="Test", unit="items") as pbar:
            assert pbar == mock_pbar

        mock_tqdm.assert_called_once_with(
            total=100, desc="Test", unit="items", file=sys.stdout
        )
        mock_pbar.close.assert_called_once()

    @patch('undatum.common.progress.TQDM_AVAILABLE', False)
    def test_progress_bar_tqdm_unavailable(self):
        """Test progress bar when tqdm is unavailable."""
        with progress_bar(total=100, desc="Test") as pbar:
            assert pbar is None

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=False)
    def test_progress_bar_not_tty(self, mock_is_tty):
        """Test progress bar when not a TTY."""
        with progress_bar(total=100, desc="Test") as pbar:
            assert pbar is None

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=True)
    def test_progress_bar_disabled(self, mock_is_tty):
        """Test progress bar when disabled."""
        with progress_bar(total=100, desc="Test", disable=True) as pbar:
            assert pbar is None

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=True)
    def test_progress_bar_show_progress_false(self, mock_is_tty):
        """Test progress bar when show_progress is False."""
        with progress_bar(total=100, desc="Test", show_progress=False) as pbar:
            assert pbar is None


class TestUpdateProgress:
    """Test update_progress function."""

    def test_update_progress_with_pbar(self):
        """Test update_progress with a progress bar."""
        mock_pbar = MagicMock()
        update_progress(mock_pbar, n=5)
        mock_pbar.update.assert_called_once_with(5)

    def test_update_progress_with_none(self):
        """Test update_progress with None (no progress bar)."""
        update_progress(None, n=5)  # Should not raise

    def test_update_progress_with_exception(self):
        """Test update_progress when update raises exception."""
        mock_pbar = MagicMock()
        mock_pbar.update.side_effect = Exception("Test error")
        update_progress(mock_pbar, n=5)  # Should not raise


class TestSetProgressDescription:
    """Test set_progress_description function."""

    def test_set_progress_description_with_pbar(self):
        """Test set_progress_description with a progress bar."""
        mock_pbar = MagicMock()
        set_progress_description(mock_pbar, "New description")
        mock_pbar.set_description.assert_called_once_with("New description")

    def test_set_progress_description_with_none(self):
        """Test set_progress_description with None."""
        set_progress_description(None, "New description")  # Should not raise

    def test_set_progress_description_with_exception(self):
        """Test set_progress_description when set_description raises exception."""
        mock_pbar = MagicMock()
        mock_pbar.set_description.side_effect = Exception("Test error")
        set_progress_description(mock_pbar, "New description")  # Should not raise


class TestSetProgressPostfix:
    """Test set_progress_postfix function."""

    def test_set_progress_postfix_with_pbar(self):
        """Test set_progress_postfix with a progress bar."""
        mock_pbar = MagicMock()
        postfix = {"speed": "100 items/s"}
        set_progress_postfix(mock_pbar, postfix)
        mock_pbar.set_postfix.assert_called_once_with(postfix)

    def test_set_progress_postfix_with_none(self):
        """Test set_progress_postfix with None."""
        set_progress_postfix(None, {"speed": "100 items/s"})  # Should not raise

    def test_set_progress_postfix_with_exception(self):
        """Test set_progress_postfix when set_postfix raises exception."""
        mock_pbar = MagicMock()
        mock_pbar.set_postfix.side_effect = Exception("Test error")
        set_progress_postfix(mock_pbar, {"speed": "100 items/s"})  # Should not raise


class TestWrapIterable:
    """Test wrap_iterable function."""

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=True)
    @patch('undatum.common.progress.tqdm')
    def test_wrap_iterable_enabled(self, mock_tqdm, mock_is_tty):
        """Test wrap_iterable when progress is enabled."""
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_tqdm.return_value.__exit__.return_value = None

        items = [1, 2, 3]
        result = list(wrap_iterable(iter(items), total=3, desc="Test"))
        assert result == [1, 2, 3]
        assert mock_pbar.update.call_count == 3

    @patch('undatum.common.progress.TQDM_AVAILABLE', False)
    def test_wrap_iterable_tqdm_unavailable(self):
        """Test wrap_iterable when tqdm is unavailable."""
        items = [1, 2, 3]
        result = list(wrap_iterable(iter(items), total=3, desc="Test"))
        assert result == [1, 2, 3]

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=False)
    def test_wrap_iterable_not_tty(self, mock_is_tty):
        """Test wrap_iterable when not a TTY."""
        items = [1, 2, 3]
        result = list(wrap_iterable(iter(items), total=3, desc="Test"))
        assert result == [1, 2, 3]

    @patch('undatum.common.progress.TQDM_AVAILABLE', True)
    @patch('undatum.common.progress.is_tty', return_value=True)
    @patch('undatum.common.progress.tqdm')
    def test_wrap_iterable_with_exception(self, mock_tqdm, mock_is_tty):
        """Test wrap_iterable when progress bar raises exception."""
        mock_pbar = MagicMock()
        mock_pbar.update.side_effect = Exception("Test error")
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_tqdm.return_value.__exit__.return_value = None

        items = [1, 2, 3]
        result = list(wrap_iterable(iter(items), total=3, desc="Test"))
        # Should still yield items even if progress bar fails
        assert result == [1, 2, 3]
