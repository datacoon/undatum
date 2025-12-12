# -*- coding: utf8 -*-
"""Validation rules module for undatum."""
from .ruscodes import _check_inn, _check_ogrn
from .commonrules import _validate_email, _validate_url

VALIDATION_RULEMAP = {
    'ru.org.ogrn': _check_ogrn,
    'ru.org.inn': _check_inn,
    'common.email': _validate_email,
    'common.url': _validate_url
}
