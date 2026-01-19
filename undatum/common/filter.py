"""Filter matching utility module for dictionary records.

This module provides filtering capabilities using mistql for evaluating
boolean filter expressions on dictionary records. It replaces dictquery
functionality with a mistql-based implementation.
"""
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


def match_filter(record: dict[str, Any], filter_expr: Optional[str]) -> bool:
    """Match a record against a filter expression.

    This function evaluates a boolean filter expression against a dictionary
    record using mistql. Returns True if the record matches the filter,
    False otherwise.

    Args:
        record: Dictionary record to evaluate
        filter_expr: Filter expression string (e.g., "age >= 18", "status == 'active'")
                    If None, returns True (no filter applied)

    Returns:
        True if record matches the filter, False otherwise

    Raises:
        ValueError: If filter expression is invalid
        Exception: Re-raises any mistql evaluation errors with context
    """
    if filter_expr is None or not str(filter_expr).strip():
        return True

    if not isinstance(record, dict):
        logger.warning('match_filter: record is not a dict, returning False')
        return False

    try:
        from mistql import query
        from mistql.exceptions import MistQLReferenceError

        # Allow backtick-wrapped identifiers by stripping backticks for mistql.
        normalized_expr = re.sub(r"`([^`]+)`", r"\1", filter_expr)

        # mistql evaluates expressions directly against the record context.
        try:
            result = query(normalized_expr, record)
            return bool(result)
        except ValueError as exc:
            # Attempt numeric coercion for CSV-like string values.
            if "different types" not in str(exc):
                raise

            def _coerce_value(value: Any) -> Any:
                if isinstance(value, dict):
                    return {k: _coerce_value(v) for k, v in value.items()}
                if isinstance(value, list):
                    return [_coerce_value(v) for v in value]
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered == "true":
                        return True
                    if lowered == "false":
                        return False
                    try:
                        if "." in value:
                            return float(value)
                        return int(value)
                    except ValueError:
                        return value
                return value

            coerced_record = _coerce_value(record)
            result = query(normalized_expr, coerced_record)
            return bool(result)

    except ImportError as e:
        logger.error('mistql is not available: %s', e)
        raise RuntimeError('mistql library is required for filtering') from e
    except MistQLReferenceError:
        # Missing fields should be treated as non-matches, not errors.
        return False
    except Exception as e:
        logger.debug('Filter evaluation error: %s, expression: %s, record: %s', e, filter_expr, record)
        # Re-raise with more context
        raise ValueError(f'Invalid filter expression "{filter_expr}": {e}') from e


def translate_filter_to_sql(filter_expr: Optional[str]) -> Optional[str]:
    """Translate basic filter expression to SQL WHERE clause.

    This function provides basic translation of filter expressions to SQL
    WHERE clauses for use with DuckDB. It supports common comparison and
    logical operators.

    Args:
        filter_expr: Filter expression string (e.g., "age >= 18")
                    If None, returns None

    Returns:
        SQL WHERE clause string (without WHERE keyword) or None if translation
        not possible or not needed

    Note:
        This is a basic implementation. Complex expressions may not be
        translatable and should use the iterable engine with match_filter()
        instead.
    """
    if filter_expr is None:
        return None

    # Placeholder: SQL translation is not implemented yet.
    # Keep returning None to force iterable engine fallback.
    return None
