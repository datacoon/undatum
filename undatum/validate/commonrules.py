"""Common validation rules for email and URL."""
from email.utils import parseaddr

import validators.url


def _validate_email(s):
    """Validate email address."""
    return '@' in parseaddr(s)[1]


def _validate_url(s):
    """Validate URL."""
    r = validators.url(s)
    return r is True


def _validate_integer(value):
    """Validate integer-like values."""
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        try:
            int(value)
            return True
        except ValueError:
            return False
    return False
