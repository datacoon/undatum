"""Progress indication utilities using tqdm."""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Optional

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None


def is_tty() -> bool:
    """Check if output is a TTY (terminal).

    Returns:
        True if stdout is a TTY, False otherwise
    """
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


@contextmanager
def progress_bar(
    total: Optional[int] = None,
    desc: Optional[str] = None,
    unit: str = "items",
    disable: bool = False,
    show_progress: bool = True,
):
    """Create a progress bar context manager.

    Args:
        total: Total number of items (None for unknown)
        desc: Description text for progress bar
        unit: Unit label (e.g., "rows", "bytes")
        disable: If True, disable progress bar
        show_progress: If False, disable progress bar (overrides disable)

    Yields:
        Progress bar object (or None if disabled)
    """
    if not show_progress or disable or not TQDM_AVAILABLE or not is_tty():
        yield None
        return

    pbar = None
    try:
        pbar = tqdm(total=total, desc=desc, unit=unit, file=sys.stdout)
        yield pbar
    finally:
        # Use identity check: tqdm raises TypeError on bool(pbar) when total is None.
        if pbar is not None:
            pbar.close()


def wrap_iterable(
    iterable: Iterator[Any],
    total: Optional[int] = None,
    desc: Optional[str] = None,
    unit: str = "items",
    disable: bool = False,
    show_progress: bool = True,
) -> Iterator[Any]:
    """Wrap an iterable with progress indication.

    Args:
        iterable: Iterator to wrap
        total: Total number of items (None for unknown)
        desc: Description text for progress bar
        unit: Unit label (e.g., "rows", "bytes")
        disable: If True, disable progress bar
        show_progress: If False, disable progress bar (overrides disable)

    Yields:
        Items from iterable with progress tracking
    """
    if not show_progress or disable or not TQDM_AVAILABLE or not is_tty():
        yield from iterable
        return

    try:
        with tqdm(total=total, desc=desc, unit=unit, file=sys.stdout) as pbar:
            for item in iterable:
                yield item
                pbar.update(1)
    except Exception as e:
        logging.warning(f"Progress bar error: {e}")
        yield from iterable


def update_progress(pbar: Optional[Any], n: int = 1) -> None:
    """Update progress bar by n items.

    Args:
        pbar: Progress bar object (from progress_bar context manager)
        n: Number of items to increment by
    """
    if pbar is not None:
        try:
            pbar.update(n)
        except Exception:
            pass  # Ignore errors in progress updates


def set_progress_description(pbar: Optional[Any], desc: str) -> None:
    """Set progress bar description.

    Args:
        pbar: Progress bar object (from progress_bar context manager)
        desc: Description text
    """
    if pbar is not None:
        try:
            pbar.set_description(desc)
        except Exception:
            pass


def set_progress_postfix(pbar: Optional[Any], postfix: dict[str, Any]) -> None:
    """Set progress bar postfix (additional info like throughput).

    Args:
        pbar: Progress bar object (from progress_bar context manager)
        postfix: Dictionary of key-value pairs to display
    """
    if pbar is not None:
        try:
            pbar.set_postfix(postfix)
        except Exception:
            pass
