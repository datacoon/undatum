# -*- coding: utf8 -*-
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
