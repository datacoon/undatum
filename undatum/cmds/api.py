# -*- coding: utf8 -*-
"""Data API command module."""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from typing import Any, Optional

import yaml
from iterable.helpers.detect import detect_file_type

from ..common.schema_utils import duckdb_decompose
from ..common.errors import FileNotFoundError, PermissionError, find_similar_files
from ..common.path_utils import validate_file_path
from ..constants import DUCKABLE_FILE_TYPES
from ..utils import get_option

logger = logging.getLogger(__name__)

DEFAULT_ALLOWED_OPS = ["eq", "ne", "lt", "gt", "le", "ge", "like"]
DEFAULT_ORDER_DIRS = {"asc", "desc"}
DEFAULT_PAGINATION = {"default_limit": 50, "max_limit": 1000}
OPERATOR_MAP = {
    "eq": "=",
    "ne": "!=",
    "lt": "<",
    "gt": ">",
    "le": "<=",
    "ge": ">=",
    "like": "LIKE",
}


def _normalize_resource_name(path: str, idx: int) -> str:
    stem = os.path.splitext(os.path.basename(path))[0]
    name = re.sub(r"[^A-Za-z0-9_]+", "_", stem).strip("_").lower()
    if not name:
        return f"resource_{idx}"
    return name


def _detect_format(path: str, override: Optional[str]) -> Optional[str]:
    if override:
        return override.lower()
    detected = detect_file_type(path)
    if detected.get("success"):
        return detected.get("datatype").id()
    return None


def _infer_fields(path: str, filetype: str) -> list[dict[str, Any]]:
    if filetype not in DUCKABLE_FILE_TYPES:
        raise ValueError(f"Unsupported file type for API discovery: {filetype}")
    rows = duckdb_decompose(filename=path, filetype=filetype, path="*", limit=10000)
    fields = []
    for row in rows:
        if len(row) < 2:
            continue
        field_name = row[0]
        field_type = str(row[1]).lower()
        fields.append({"name": field_name, "type": field_type})
    return fields


def _infer_primary_key_candidates(path: str, filetype: str) -> list[str]:
    if filetype not in DUCKABLE_FILE_TYPES:
        return []
    rows = duckdb_decompose(
        filename=path,
        filetype=filetype,
        path="*",
        limit=10000,
        use_summarize=True
    )
    candidates: list[str] = []
    for row in rows:
        if len(row) < 5:
            continue
        try:
            unique_count = int(row[3])
            total_count = int(row[4])
        except (TypeError, ValueError):
            continue
        if total_count > 0 and unique_count == total_count:
            candidates.append(row[0])
    return candidates


def _split_csv(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def load_api_config(path: str) -> dict[str, Any]:
    """Load API config from YAML or JSON."""
    with open(path, "r", encoding="utf8") as handle:
        raw = handle.read()
    if path.lower().endswith(".json"):
        return json.loads(raw)
    return yaml.safe_load(raw)


def dump_api_config(config: dict[str, Any], output: Optional[str] = None,
                    config_format: Optional[str] = None) -> str:
    """Serialize API config to YAML or JSON."""
    if config_format:
        config_format = config_format.lower()
    if output and not config_format:
        config_format = "json" if output.lower().endswith(".json") else "yaml"
    if config_format == "json":
        return json.dumps(config, indent=2, ensure_ascii=False)
    return yaml.safe_dump(config, sort_keys=False, allow_unicode=False)


def _build_api_app(config: dict[str, Any]):
    try:
        from fastapi import FastAPI, HTTPException, Request
    except Exception as exc:
        raise ImportError(
            "Data API requires fastapi. Install with `pip install \"undatum[api]\"`."
        ) from exc
    import duckdb

    app = FastAPI(title="undatum Data API")
    conn = duckdb.connect(database=":memory:")
    resource_index = {}

    resources = config.get("resources") or []
    for idx, resource in enumerate(resources, start=1):
        name = resource.get("name") or f"resource_{idx}"
        path = resource.get("path")
        fmt = resource.get("format")
        if not path or not fmt:
            raise ValueError(f"Resource {name} missing path or format.")

        table_name = f"resource_{idx}"
        safe_path = path.replace("'", "''")
        if fmt == "csv":
            read_expr = f"read_csv_auto('{safe_path}')"
        elif fmt in {"json", "jsonl"}:
            read_expr = f"read_json_auto('{safe_path}')"
        elif fmt == "parquet":
            read_expr = f"read_parquet('{safe_path}')"
        else:
            raise ValueError(f"Unsupported API format: {fmt}")
        conn.execute(f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM {read_expr}')

        fields = [field.get("name") for field in resource.get("fields") or [] if field.get("name")]
        allowed_ops = resource.get("query", {}).get("allowed_ops") or DEFAULT_ALLOWED_OPS
        allowed_order_by = resource.get("query", {}).get("allowed_order_by") or fields
        pagination = resource.get("pagination") or DEFAULT_PAGINATION
        primary_key = resource.get("primary_key")
        resource_index[name] = {
            "table": table_name,
            "fields": set(fields),
            "allowed_ops": set(allowed_ops),
            "allowed_order_by": set(allowed_order_by),
            "pagination": pagination,
            "primary_key": primary_key,
        }

    def _parse_query(resource_meta: dict[str, Any], params: dict[str, str]) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        for key, value in params.items():
            if key in {"limit", "offset", "order_by", "order_dir"}:
                continue
            if "__" in key:
                field, op = key.split("__", 1)
            else:
                field, op = key, "eq"
            if field not in resource_meta["fields"]:
                raise HTTPException(status_code=400, detail=f"Unknown field: {field}")
            if op not in resource_meta["allowed_ops"]:
                raise HTTPException(status_code=400, detail=f"Unsupported operator: {op}")
            clauses.append(f'"{field}" {OPERATOR_MAP[op]} ?')
            values.append(value)
        return " AND ".join(clauses), values

    def _apply_order(sql: str, order_by: Optional[str], order_dir: str,
                     resource_meta: dict[str, Any]) -> str:
        if not order_by:
            return sql
        fields = [part.strip() for part in order_by.split(",") if part.strip()]
        if not fields:
            return sql
        for field in fields:
            if field not in resource_meta["allowed_order_by"]:
                raise HTTPException(status_code=400, detail=f"Order by not allowed: {field}")
        dir_lower = order_dir.lower()
        if dir_lower not in DEFAULT_ORDER_DIRS:
            raise HTTPException(status_code=400, detail=f"Invalid order_dir: {order_dir}")
        order_clause = ", ".join(f'"{field}" {dir_lower.upper()}' for field in fields)
        return f"{sql} ORDER BY {order_clause}"

    def _handle_list(resource_meta: dict[str, Any], params: dict[str, str]) -> list[dict[str, Any]]:
        pagination = resource_meta["pagination"]
        default_limit = int(pagination.get("default_limit", DEFAULT_PAGINATION["default_limit"]))
        max_limit = int(pagination.get("max_limit", DEFAULT_PAGINATION["max_limit"]))

        try:
            limit = int(params.get("limit", default_limit))
            offset = int(params.get("offset", 0))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="limit/offset must be integers") from exc

        if limit > max_limit:
            limit = max_limit
        if limit < 0 or offset < 0:
            raise HTTPException(status_code=400, detail="limit/offset must be >= 0")

        sql = f'SELECT * FROM "{resource_meta["table"]}"'
        where_clause, values = _parse_query(resource_meta, params)
        if where_clause:
            sql = f"{sql} WHERE {where_clause}"
        sql = _apply_order(sql, params.get("order_by"), params.get("order_dir", "asc"), resource_meta)
        sql = f"{sql} LIMIT ? OFFSET ?"
        values.extend([limit, offset])

        cursor = conn.execute(sql, values)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def _handle_detail(resource_meta: dict[str, Any], pk_value: str) -> dict[str, Any]:
        primary_key = resource_meta["primary_key"]
        if isinstance(primary_key, list) and len(primary_key) == 1:
            field = primary_key[0]
        elif isinstance(primary_key, str):
            field = primary_key
        else:
            raise HTTPException(status_code=404, detail="Primary key endpoint not available")
        if field not in resource_meta["fields"]:
            raise HTTPException(status_code=404, detail="Primary key field not available")

        sql = f'SELECT * FROM "{resource_meta["table"]}" WHERE "{field}" = ? LIMIT 1'
        cursor = conn.execute(sql, [pk_value])
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Not found")
        columns = [col[0] for col in cursor.description]
        return dict(zip(columns, row))

    for resource_name, meta in resource_index.items():
        path = f"/{resource_name}"

        def list_handler(request: Request, resource_meta=meta):
            params = dict(request.query_params)
            return _handle_list(resource_meta, params)

        app.get(path)(list_handler)

        def detail_handler(pk: str, resource_meta=meta):
            return _handle_detail(resource_meta, pk)

        if meta.get("primary_key"):
            app.get(f"{path}" + "/{pk}")(detail_handler)

    return app


class DataApi:
    """Data API command handler."""

    def discover(self, input_files: list[str], options: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        if options is None:
            options = {}
        if not input_files:
            raise ValueError("No input files provided.")

        output = get_option(options, "output")
        emit = get_option(options, "emit")
        if emit is None:
            emit = True
        format_in = get_option(options, "format_in")
        config_format = get_option(options, "config_format")
        default_limit = get_option(options, "default_limit") or DEFAULT_PAGINATION["default_limit"]
        max_limit = get_option(options, "max_limit") or DEFAULT_PAGINATION["max_limit"]
        allowed_ops = _split_csv(get_option(options, "allowed_ops")) or DEFAULT_ALLOWED_OPS

        resources = []
        for idx, path in enumerate(input_files, start=1):
            # Validate file exists and is readable
            try:
                validate_file_path(path, check_read=True)
            except FileNotFoundError as e:
                suggestions = find_similar_files(path)
                raise FileNotFoundError(path, suggestions) from e
            except PermissionError as e:
                raise PermissionError(path, operation="read") from e
            
            filetype = _detect_format(path, format_in)
            if not filetype:
                from ..common.errors import FormatError
                supported = ['csv', 'json', 'jsonl', 'parquet']
                raise FormatError(path, 'unknown', supported)
            fields = _infer_fields(path, filetype)
            primary_candidates = _infer_primary_key_candidates(path, filetype)
            resource = {
                "name": _normalize_resource_name(path, idx),
                "path": path,
                "format": filetype,
                "read_only": True,
                "fields": fields,
                "pagination": {
                    "default_limit": int(default_limit),
                    "max_limit": int(max_limit),
                },
                "query": {
                    "allowed_ops": allowed_ops,
                    "allowed_order_by": [field["name"] for field in fields],
                },
            }
            if primary_candidates:
                resource["primary_key"] = primary_candidates[0]
            resources.append(resource)

        config = {"resources": resources}
        payload = dump_api_config(config, output=output, config_format=config_format)
        if output:
            with open(output, "w", encoding="utf8") as handle:
                handle.write(payload)
                handle.write("\n")
        elif emit:
            sys.stdout.write(payload)
            sys.stdout.write("\n")
        return config

    def serve(self, config_path: Optional[str], options: Optional[dict[str, Any]] = None,
              config: Optional[dict[str, Any]] = None) -> None:
        if options is None:
            options = {}
        try:
            import uvicorn  # type: ignore
        except Exception as exc:
            raise ImportError(
                "Data API requires uvicorn. Install with `pip install \"undatum[api]\"`."
            ) from exc

        if config is None:
            if not config_path:
                raise ValueError("Config path is required.")
            config = load_api_config(config_path)

        host = get_option(options, "host") or "127.0.0.1"
        port = int(get_option(options, "port") or 8000)

        app = _build_api_app(config)
        uvicorn.run(app, host=host, port=port)

    def run(self, input_files: list[str], options: Optional[dict[str, Any]] = None) -> None:
        if options is None:
            options = {}
        options = dict(options)
        options["emit"] = False
        config = self.discover(input_files, options)
        self.serve(None, options, config=config)
