"""Filter matching utility module for dictionary records.

This module provides filtering capabilities using mistql for evaluating
boolean filter expressions on dictionary records. It replaces dictquery
functionality with a mistql-based implementation.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


class _FilterTranslationError(Exception):
    """Raised when a filter expression cannot be translated to SQL."""


def match_filter(record: dict, filter_expr: Optional[str]) -> bool:
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
        logger.warning("match_filter: record is not a dict, returning False")
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

            def _coerce_value(value):
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
        logger.error("mistql is not available: %s", e)
        raise RuntimeError("mistql library is required for filtering") from e
    except MistQLReferenceError:
        # Missing fields should be treated as non-matches, not errors.
        return False
    except Exception as e:
        logger.debug(
            "Filter evaluation error: %s, expression: %s, record: %s", e, filter_expr, record
        )
        # Re-raise with more context
        raise ValueError(f'Invalid filter expression "{filter_expr}": {e}') from e


def translate_filter_to_sql(filter_expr: Optional[str]) -> Optional[str]:
    """Translate basic filter expression to SQL WHERE clause.

    Supports comparisons (``==``, ``!=``, ``>=``, ``<=``, ``>``, ``<``),
    ``AND`` / ``OR``, parentheses, backtick identifiers, string literals,
    numbers, and booleans. Returns ``None`` when translation is not possible.

    Args:
        filter_expr: Filter expression string (e.g., "age >= 18")

    Returns:
        SQL WHERE clause string (without WHERE keyword) or None
    """
    if filter_expr is None or not str(filter_expr).strip():
        return None

    try:
        return _translate_expr(str(filter_expr).strip())
    except _FilterTranslationError:
        return None


def _matching_outer_parens(expr: str) -> bool:
    if not (expr.startswith("(") and expr.endswith(")")):
        return False
    depth = 0
    for i, char in enumerate(expr):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0 and i != len(expr) - 1:
                return False
    return depth == 0


def _split_logical(expr: str, keyword: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_string = None
    i = 0
    n = len(expr)
    kw_pat = re.compile(rf"\s+{keyword}\s+", re.IGNORECASE)

    while i < n:
        char = expr[i]
        if in_string:
            buf.append(char)
            if char == in_string:
                in_string = None
            i += 1
            continue
        if char in ("'", '"'):
            in_string = char
            buf.append(char)
            i += 1
            continue
        if char == "(":
            depth += 1
            buf.append(char)
            i += 1
            continue
        if char == ")":
            depth -= 1
            buf.append(char)
            i += 1
            continue
        if depth == 0:
            match = kw_pat.match(expr, i)
            if match:
                part = "".join(buf).strip()
                if part:
                    parts.append(part)
                buf = []
                i = match.end()
                continue
        buf.append(char)
        i += 1

    part = "".join(buf).strip()
    if part:
        parts.append(part)
    return parts if len(parts) > 1 else [expr]


def _translate_expr(expr: str) -> str:
    expr = expr.strip()
    if expr.startswith("(") and _matching_outer_parens(expr):
        return f"({_translate_expr(expr[1:-1].strip())})"

    or_parts = _split_logical(expr, "OR")
    if len(or_parts) > 1:
        return " OR ".join(_translate_expr(part) for part in or_parts)

    and_parts = _split_logical(expr, "AND")
    if len(and_parts) > 1:
        return " AND ".join(_translate_expr(part) for part in and_parts)

    return _translate_comparison(expr)


def _find_comparison_op(expr: str) -> tuple[str, str, str]:
    in_string = None
    i = 0
    n = len(expr)
    ops = (">=", "<=", "==", "!=", ">", "<")

    while i < n:
        char = expr[i]
        if in_string:
            if char == in_string:
                in_string = None
            i += 1
            continue
        if char in ("'", '"'):
            in_string = char
            i += 1
            continue
        for op in ops:
            if expr[i : i + len(op)] == op:
                left = expr[:i]
                right = expr[i + len(op) :]
                if left.strip() and right.strip():
                    return left, op, right
                raise _FilterTranslationError("empty operand")
        i += 1

    raise _FilterTranslationError("no comparison operator")


def _translate_identifier(token: str) -> str:
    token = token.strip()
    if token.startswith("`") and token.endswith("`"):
        inner = token[1:-1].replace('"', '""')
        return f'"{inner}"'
    if token.startswith('"') and token.endswith('"'):
        return token
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", token):
        return f'"{token}"'
    raise _FilterTranslationError(f"unsupported identifier: {token}")


def _translate_value(token: str) -> str:
    token = token.strip()
    if token.startswith("'") and token.endswith("'") and len(token) >= 2:
        inner = token[1:-1].replace("'", "''")
        return f"'{inner}'"
    if re.match(r"^-?\d+(?:\.\d+)?$", token):
        return token
    if token.lower() in ("true", "false"):
        return token.upper()
    raise _FilterTranslationError(f"unsupported value: {token}")


def _translate_comparison(expr: str) -> str:
    expr = expr.strip()
    if expr.startswith("(") and _matching_outer_parens(expr):
        return f"({_translate_expr(expr[1:-1].strip())})"

    if re.search(r"\bin\b|\blike\b|\bmatch\b|\|\||&&|=>|\[|\.|;", expr, re.IGNORECASE):
        raise _FilterTranslationError("unsupported construct")

    left, op, right = _find_comparison_op(expr)
    sql_left = _translate_identifier(left)
    sql_right = _translate_value(right)
    sql_op = {"==": "=", "!=": "!=", ">=": ">=", "<=": "<=", ">": ">", "<": "<"}[op]

    if re.match(r"^-?\d", sql_right):
        sql_left = f"TRY_CAST({sql_left} AS DOUBLE)"

    return f"{sql_left} {sql_op} {sql_right}"
