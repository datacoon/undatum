"""Data API command module."""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import os
import re
import sys
import tempfile
import uuid
from typing import Any, Optional

import yaml
from iterable.helpers.detect import detect_file_type
from pydantic import BaseModel, Field, create_model
from starlette.requests import Request

from .. import __version__
from ..common.errors import FileNotFoundError, PermissionError, find_similar_files
from ..common.path_utils import (
    cloud_object_suffix,
    is_cloud_uri,
    is_s3_uri,
    is_uri,
    looks_like_missing_cloud_dep,
    missing_cloud_extra_error,
    validate_file_path,
)
from ..common.schema_utils import duckdb_decompose
from ..constants import DUCKABLE_FILE_TYPES
from ..utils import get_option

logger = logging.getLogger(__name__)

DEFAULT_ALLOWED_OPS = ["eq", "ne", "lt", "gt", "le", "ge", "like"]
DEFAULT_ORDER_DIRS = {"asc", "desc"}
DEFAULT_PAGINATION = {"default_limit": 50, "max_limit": 1000}
RESERVED_QUERY_PARAMS = {
    "limit",
    "offset",
    "order_by",
    "order_dir",
    "sort",
    "include_total",
    "api_key",
}
OPERATOR_MAP = {
    "eq": "=",
    "ne": "!=",
    "lt": "<",
    "gt": ">",
    "le": "<=",
    "ge": ">=",
    "like": "LIKE",
}


class PaginationMeta(BaseModel):
    """Pagination metadata for list responses."""

    limit: int
    offset: int
    count: int
    total: Optional[int] = None


def require_api_dependencies() -> None:
    """Raise DependencyError when the optional Data API extra is not installed."""
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError as exc:
        from ..common.errors import DependencyError

        raise DependencyError(
            "fastapi",
            feature="Data API",
            install_command='pip install "undatum[api]"',
        ) from exc


def _normalize_resource_name(path: str, idx: int) -> str:
    stem = os.path.splitext(os.path.basename(path))[0]
    name = re.sub(r"[^A-Za-z0-9_]+", "_", stem).strip("_").lower()
    if not name:
        return f"resource_{idx}"
    return name


def _unique_resource_name(base: str, used: set[str]) -> str:
    if base not in used:
        used.add(base)
        return base
    counter = 2
    while f"{base}_{counter}" in used:
        counter += 1
    name = f"{base}_{counter}"
    used.add(name)
    return name


def _detect_format(path: str, override: str | None) -> str | None:
    if override:
        return override.lower()
    if is_cloud_uri(path):
        ext = os.path.splitext(path.split("?")[0])[1].lstrip(".").lower()
        if ext == "ndjson":
            return "jsonl"
        if ext in {"csv", "json", "jsonl", "parquet"}:
            return ext
        return None
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
        filename=path, filetype=filetype, path="*", limit=10000, use_summarize=True
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


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(val) for key, val in row.items()}


def _duckdb_type_to_python(dtype: str) -> type:
    dtype = dtype.lower()
    if any(token in dtype for token in ("int", "bigint", "smallint", "tinyint", "hugeint")):
        return int
    if any(token in dtype for token in ("double", "float", "real", "decimal", "numeric")):
        return float
    if "bool" in dtype:
        return bool
    return str


def _model_name(resource_name: str, suffix: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", resource_name).strip("_")
    if not safe:
        safe = "Resource"
    if safe[0].isdigit():
        safe = f"R_{safe}"
    return f"{safe}{suffix}"


def _create_row_model(resource_name: str, fields: list[dict[str, Any]]) -> type[BaseModel]:
    field_defs: dict[str, Any] = {}
    for field in fields:
        name = field.get("name")
        if not name:
            continue
        py_type = _duckdb_type_to_python(str(field.get("type", "varchar")))
        field_defs[name] = (Optional[py_type], Field(default=None, description=field.get("type")))
    if not field_defs:
        field_defs["value"] = (Optional[Any], Field(default=None))
    return create_model(_model_name(resource_name, "Row"), **field_defs)  # type: ignore[call-overload]


def _create_list_response_model(resource_name: str, row_model: type[BaseModel]) -> type[BaseModel]:
    return create_model(
        _model_name(resource_name, "ListResponse"),
        data=(list[row_model], Field(description="Matching records")),  # type: ignore[valid-type]
        pagination=(PaginationMeta, Field(description="Pagination metadata")),
    )


def _single_primary_key_field(primary_key: Any) -> str | None:
    if isinstance(primary_key, str):
        return primary_key
    if isinstance(primary_key, list) and len(primary_key) == 1:
        return primary_key[0]
    return None


def _apply_sort_alias(params: dict[str, str]) -> dict[str, str]:
    merged = dict(params)
    if "sort" in merged and "order_by" not in merged:
        sort_val = merged.pop("sort")
        if sort_val.startswith("-"):
            merged["order_by"] = sort_val[1:]
            merged["order_dir"] = "desc"
        else:
            merged["order_by"] = sort_val
            merged.setdefault("order_dir", "asc")
    return merged


def _build_filter_openapi_extra(
    fields: list[str], allowed_ops: list[str]
) -> dict[str, list[dict[str, Any]]]:
    parameters: list[dict[str, Any]] = []
    for field in sorted(fields):
        for op in sorted(allowed_ops):
            parameters.append(
                {
                    "name": f"{field}__{op}",
                    "in": "query",
                    "required": False,
                    "schema": {"type": "string"},
                    "description": f"Filter where {field} {op} the given value.",
                }
            )
        parameters.append(
            {
                "name": field,
                "in": "query",
                "required": False,
                "schema": {"type": "string"},
                "description": f"Shorthand for {field}__eq.",
            }
        )
    return {"parameters": parameters}


API_CONFIG_JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "undatum Data API config",
    "type": "object",
    "required": ["resources"],
    "properties": {
        "resources": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["name", "path", "format"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "path": {"type": "string", "minLength": 1},
                    "format": {
                        "type": "string",
                        "enum": ["csv", "json", "jsonl", "parquet"],
                    },
                    "read_only": {"type": "boolean"},
                    "primary_key": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ]
                    },
                    "fields": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name"],
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string"},
                            },
                        },
                    },
                    "pagination": {
                        "type": "object",
                        "properties": {
                            "default_limit": {"type": "integer", "minimum": 1},
                            "max_limit": {"type": "integer", "minimum": 1},
                        },
                    },
                    "query": {
                        "type": "object",
                        "properties": {
                            "allowed_ops": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "allowed_order_by": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                    },
                },
            },
        }
    },
}


def validate_api_config_schema(config: Any) -> None:
    """Validate an API config against the embedded JSON Schema subset.

    Raises:
        ValueError: If the config does not match the required shape.
    """
    if not isinstance(config, dict):
        raise ValueError("API config must be a JSON/YAML object.")
    resources = config.get("resources")
    if not isinstance(resources, list) or not resources:
        raise ValueError("API config must define a non-empty 'resources' array.")
    allowed_formats = {"csv", "json", "jsonl", "parquet"}
    for idx, resource in enumerate(resources, start=1):
        label = f"Resource {idx}"
        if not isinstance(resource, dict):
            raise ValueError(f"{label} must be an object.")
        name = resource.get("name") or f"resource_{idx}"
        label = f"Resource '{name}'"
        path = resource.get("path")
        fmt = resource.get("format")
        if not isinstance(path, str) or not path.strip():
            raise ValueError(f"{label} missing path.")
        if not isinstance(fmt, str) or fmt.lower() not in allowed_formats:
            raise ValueError(
                f"{label} has unsupported format '{fmt}'. Use csv, json, jsonl, or parquet."
            )
        fields = resource.get("fields")
        if fields is not None:
            if not isinstance(fields, list):
                raise ValueError(f"{label} fields must be an array.")
            for field in fields:
                if not isinstance(field, dict) or not field.get("name"):
                    raise ValueError(f"{label} field entries must be objects with a name.")
        pagination = resource.get("pagination")
        if pagination is not None and not isinstance(pagination, dict):
            raise ValueError(f"{label} pagination must be an object.")
        query = resource.get("query")
        if query is not None and not isinstance(query, dict):
            raise ValueError(f"{label} query must be an object.")


def _download_fsspec_uri(path: str, dest: str) -> None:
    """Copy a GCS/Azure/s3a object to a local file via fsspec."""
    try:
        import fsspec
    except ImportError as exc:
        raise missing_cloud_extra_error(path, exc) from exc
    try:
        fs, _, paths = fsspec.get_fs_token_paths(path)
        src_path = paths[0] if paths else path
        with fs.open(src_path, "rb") as src, open(dest, "wb") as out:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    except Exception as exc:
        if looks_like_missing_cloud_dep(path, exc):
            raise missing_cloud_extra_error(path, exc) from exc
        raise


def _materialize_resource_path(path: str, temp_files: list[str]) -> str:
    """Return a local path DuckDB can read, downloading cloud URIs as needed."""
    if not is_cloud_uri(path):
        return path

    suffix = cloud_object_suffix(path)
    temp_fd, temp_path = tempfile.mkstemp(suffix=suffix)
    os.close(temp_fd)
    try:
        logger.info("Downloading %s for Data API", path)
        if is_s3_uri(path):
            from ..formats.s3 import get_s3_client, parse_s3_uri

            bucket, key = parse_s3_uri(path)
            client = get_s3_client()
            client.download_file(bucket, key, temp_path)
        else:
            _download_fsspec_uri(path, temp_path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise
    temp_files.append(temp_path)
    return temp_path


def load_api_config(path: str) -> dict[str, Any]:
    """Load API config from YAML or JSON."""
    with open(path, encoding="utf8") as handle:
        raw = handle.read()
    if path.lower().endswith(".json"):
        return json.loads(raw)
    return yaml.safe_load(raw)


def dump_api_config(
    config: dict[str, Any], output: str | None = None, config_format: str | None = None
) -> str:
    """Serialize API config to YAML or JSON."""
    if config_format:
        config_format = config_format.lower()
    if output and not config_format:
        config_format = "json" if output.lower().endswith(".json") else "yaml"
    if config_format == "json":
        return json.dumps(config, indent=2, ensure_ascii=False)
    return yaml.safe_dump(config, sort_keys=False, allow_unicode=False)


def _validate_resources_config(config: dict[str, Any], temp_files: list[str] | None = None) -> None:
    validate_api_config_schema(config)
    resources = config.get("resources") or []
    if temp_files is None:
        temp_files = []
    for idx, resource in enumerate(resources, start=1):
        name = resource.get("name") or f"resource_{idx}"
        path = resource.get("path")
        fmt = resource.get("format")
        if not path or not fmt:
            raise ValueError(f"Resource {name} missing path or format.")
        if is_uri(path) and not is_cloud_uri(path):
            raise ValueError(
                f"Resource {name} path '{path}' is not a local file or cloud URI "
                f"(s3://, gs://, gcs://, az://, abfs://, abfss://)."
            )
        try:
            validate_file_path(path, check_read=True)
        except FileNotFoundError as exc:
            suggestions = find_similar_files(path)
            raise FileNotFoundError(path, suggestions) from exc
        except PermissionError as exc:
            raise PermissionError(path, operation="read") from exc
        resource["path"] = _materialize_resource_path(path, temp_files)


def _build_api_app(
    config: dict[str, Any],
    *,
    api_key: str | None = None,
    cors_origins: list[str] | None = None,
):
    try:
        from fastapi import FastAPI, HTTPException, Query
        from fastapi.middleware.cors import CORSMiddleware
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.responses import JSONResponse
    except Exception as exc:
        raise ImportError(
            'Data API requires fastapi. Install with `pip install "undatum[api]"`.'
        ) from exc
    import duckdb

    temp_files: list[str] = []
    _validate_resources_config(config, temp_files)

    app = FastAPI(
        title="undatum Data API",
        description="Read-only HTTP API over file-backed datasets (CSV, JSON/JSONL, Parquet).",
        version=__version__,
    )
    app.state.temp_files = temp_files

    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "OPTIONS"],
            allow_headers=["*"],
        )

    if api_key:
        expected = api_key

        class APIKeyMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                path = request.url.path
                if path in {"/docs", "/redoc", "/openapi.json"} or path.startswith("/docs"):
                    return await call_next(request)
                provided = request.headers.get("x-api-key") or request.query_params.get("api_key")
                if provided != expected:
                    return JSONResponse({"detail": "Unauthorized"}, status_code=401)
                return await call_next(request)

        app.add_middleware(APIKeyMiddleware)
    conn = duckdb.connect(database=":memory:")
    resource_index: dict[str, dict[str, Any]] = {}
    resource_summaries: list[dict[str, Any]] = []

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

        field_defs = resource.get("fields") or []
        fields = [field.get("name") for field in field_defs if field.get("name")]
        allowed_ops = resource.get("query", {}).get("allowed_ops") or DEFAULT_ALLOWED_OPS
        allowed_order_by = resource.get("query", {}).get("allowed_order_by") or fields
        pagination = resource.get("pagination") or DEFAULT_PAGINATION
        primary_key = resource.get("primary_key")
        meta = {
            "name": name,
            "table": table_name,
            "fields": set(fields),
            "allowed_ops": set(allowed_ops),
            "allowed_order_by": set(allowed_order_by),
            "pagination": pagination,
            "primary_key": primary_key,
            "row_model": _create_row_model(name, field_defs),
            "list_model": None,
        }
        meta["list_model"] = _create_list_response_model(name, meta["row_model"])
        resource_index[name] = meta
        pk_field = _single_primary_key_field(primary_key)
        resource_summaries.append(
            {
                "name": name,
                "list": f"/{name}",
                "detail": f"/{name}/{{pk}}" if pk_field else None,
                "primary_key": pk_field,
            }
        )

    def _parse_query(
        resource_meta: dict[str, Any], params: dict[str, str]
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        for key, value in params.items():
            if key in RESERVED_QUERY_PARAMS:
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

    def _apply_order(
        sql: str, order_by: str | None, order_dir: str, resource_meta: dict[str, Any]
    ) -> str:
        if not order_by:
            return sql
        order_fields = [part.strip() for part in order_by.split(",") if part.strip()]
        if not order_fields:
            return sql
        for field in order_fields:
            if field not in resource_meta["allowed_order_by"]:
                raise HTTPException(status_code=400, detail=f"Order by not allowed: {field}")
        dir_lower = order_dir.lower()
        if dir_lower not in DEFAULT_ORDER_DIRS:
            raise HTTPException(status_code=400, detail=f"Invalid order_dir: {order_dir}")
        order_clause = ", ".join(f'"{field}" {dir_lower.upper()}' for field in order_fields)
        return f"{sql} ORDER BY {order_clause}"

    def _count_rows(resource_meta: dict[str, Any], params: dict[str, str]) -> int:
        where_clause, values = _parse_query(resource_meta, params)
        sql = f'SELECT COUNT(*) FROM "{resource_meta["table"]}"'
        if where_clause:
            sql = f"{sql} WHERE {where_clause}"
        result = conn.execute(sql, values).fetchone()
        return int(result[0]) if result else 0

    def _handle_list(resource_meta: dict[str, Any], params: dict[str, str]) -> dict[str, Any]:
        params = _apply_sort_alias(params)
        pagination = resource_meta["pagination"]
        default_limit = int(pagination.get("default_limit", DEFAULT_PAGINATION["default_limit"]))
        max_limit = int(pagination.get("max_limit", DEFAULT_PAGINATION["max_limit"]))

        try:
            limit = int(params.get("limit", default_limit))
            offset = int(params.get("offset", 0))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="limit/offset must be integers") from exc

        include_total = params.get("include_total", "").lower() in {"1", "true", "yes"}

        if limit > max_limit:
            limit = max_limit
        if limit < 0 or offset < 0:
            raise HTTPException(status_code=400, detail="limit/offset must be >= 0")

        sql = f'SELECT * FROM "{resource_meta["table"]}"'
        where_clause, values = _parse_query(resource_meta, params)
        if where_clause:
            sql = f"{sql} WHERE {where_clause}"
        sql = _apply_order(
            sql, params.get("order_by"), params.get("order_dir", "asc"), resource_meta
        )
        sql = f"{sql} LIMIT ? OFFSET ?"
        values.extend([limit, offset])

        cursor = conn.execute(sql, values)
        columns = [col[0] for col in cursor.description]
        rows = [_json_safe_row(dict(zip(columns, row))) for row in cursor.fetchall()]

        pagination_meta: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "count": len(rows),
        }
        if include_total:
            pagination_meta["total"] = _count_rows(resource_meta, params)

        return {"data": rows, "pagination": pagination_meta}

    def _handle_detail(resource_meta: dict[str, Any], pk_value: str) -> dict[str, Any]:
        field = _single_primary_key_field(resource_meta["primary_key"])
        if not field:
            raise HTTPException(status_code=404, detail="Primary key endpoint not available")
        if field not in resource_meta["fields"]:
            raise HTTPException(status_code=404, detail="Primary key field not available")

        sql = f'SELECT * FROM "{resource_meta["table"]}" WHERE "{field}" = ? LIMIT 1'
        cursor = conn.execute(sql, [pk_value])
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Not found")
        columns = [col[0] for col in cursor.description]
        return _json_safe_row(dict(zip(columns, row)))

    @app.get("/", tags=["meta"], summary="API discovery")
    def api_root() -> dict[str, Any]:
        return {
            "name": "undatum Data API",
            "version": __version__,
            "docs": "/docs",
            "openapi": "/openapi.json",
            "resources": resource_summaries,
        }

    for resource_name, meta in resource_index.items():
        route_path = f"/{resource_name}"
        row_model = meta["row_model"]
        list_model = meta["list_model"]
        field_list = sorted(meta["fields"])
        op_list = sorted(meta["allowed_ops"])
        pagination = meta["pagination"]
        default_limit = int(pagination.get("default_limit", DEFAULT_PAGINATION["default_limit"]))
        max_limit = int(pagination.get("max_limit", DEFAULT_PAGINATION["max_limit"]))
        openapi_extra = _build_filter_openapi_extra(field_list, op_list)

        def _make_list_handler(resource_meta: dict[str, Any]):
            async def list_handler(
                request: Request,
                limit: int | None = Query(
                    default=None,
                    ge=0,
                    le=max_limit,
                    description=f"Page size (default {default_limit}, max {max_limit}).",
                ),
                offset: int = Query(default=0, ge=0, description="Number of rows to skip."),
                order_by: str | None = Query(
                    default=None, description="Comma-separated fields to sort by."
                ),
                order_dir: str = Query(default="asc", description="Sort direction: asc or desc."),
                sort: str | None = Query(
                    default=None,
                    description="Sort alias: field name, or prefix with - for descending.",
                ),
                include_total: bool = Query(
                    default=False,
                    description="Include total matching row count (may be slower).",
                ),
            ):
                params = {
                    key: value
                    for key, value in request.query_params.items()
                    if key not in RESERVED_QUERY_PARAMS
                }
                params["limit"] = str(limit if limit is not None else default_limit)
                params["offset"] = str(offset)
                if order_by is not None:
                    params["order_by"] = order_by
                params["order_dir"] = order_dir
                if sort is not None:
                    params["sort"] = sort
                params["include_total"] = "true" if include_total else "false"
                return _handle_list(resource_meta, params)

            return list_handler

        app.get(
            route_path,
            response_model=list_model,
            tags=[resource_name],
            summary=f"List {resource_name} records",
            openapi_extra=openapi_extra,
        )(_make_list_handler(meta))

        pk_field = _single_primary_key_field(meta.get("primary_key"))
        if pk_field:

            def _make_detail_handler(resource_meta: dict[str, Any]):
                async def detail_handler(pk: str):
                    return _handle_detail(resource_meta, pk)

                return detail_handler

            app.get(
                f"{route_path}/{{pk}}",
                response_model=row_model,
                tags=[resource_name],
                summary=f"Get a single {resource_name} record by primary key",
            )(_make_detail_handler(meta))

    app.state.resource_summaries = resource_summaries
    return app


def _print_startup_banner(host: str, port: int, resource_summaries: list[dict[str, Any]]) -> None:
    from rich.console import Console
    from rich.panel import Panel

    base = f"http://{host}:{port}"
    lines = [f"[bold]Base URL:[/bold] {base}", "", "[bold]Resources:[/bold]"]
    for resource in resource_summaries:
        lines.append(f"  GET {base}{resource['list']}")
        if resource.get("detail"):
            lines.append(f"  GET {base}{resource['detail']}")
    lines.extend(
        [
            "",
            "[bold]Documentation:[/bold]",
            f"  Swagger UI: {base}/docs",
            f"  ReDoc:      {base}/redoc",
            f"  OpenAPI:    {base}/openapi.json",
        ]
    )
    Console().print(Panel("\n".join(lines), title="undatum Data API", border_style="green"))


class DataApi:
    """Data API command handler."""

    def discover(
        self, input_files: list[str], options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
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
        used_names: set[str] = set()
        for idx, path in enumerate(input_files, start=1):
            try:
                validate_file_path(path, check_read=True)
            except FileNotFoundError as exc:
                suggestions = find_similar_files(path)
                raise FileNotFoundError(path, suggestions) from exc
            except PermissionError as exc:
                raise PermissionError(path, operation="read") from exc

            if is_uri(path):
                abs_path = path
            else:
                abs_path = os.path.abspath(path)
            filetype = _detect_format(abs_path, format_in)
            if not filetype:
                from ..common.errors import FormatError

                supported = ["csv", "json", "jsonl", "parquet"]
                raise FormatError(abs_path, "unknown", supported)
            infer_temps: list[str] = []
            infer_path = abs_path
            if is_cloud_uri(abs_path):
                infer_path = _materialize_resource_path(abs_path, infer_temps)
            try:
                fields = _infer_fields(infer_path, filetype)
                primary_candidates = _infer_primary_key_candidates(infer_path, filetype)
            finally:
                for tmp in infer_temps:
                    if os.path.exists(tmp):
                        os.remove(tmp)
            base_name = _normalize_resource_name(abs_path, idx)
            resource_name = _unique_resource_name(base_name, used_names)
            if resource_name != base_name:
                logger.warning(
                    "Resource name collision for %s: using '%s' instead of '%s'",
                    abs_path,
                    resource_name,
                    base_name,
                )
                sys.stderr.write(
                    f"Warning: resource name collision; using '{resource_name}' for {abs_path}\n"
                )
            resource = {
                "name": resource_name,
                "path": abs_path,
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
        validate_api_config_schema(config)
        payload = dump_api_config(config, output=output, config_format=config_format)
        if output:
            with open(output, "w", encoding="utf8") as handle:
                handle.write(payload)
                handle.write("\n")
        elif emit:
            sys.stdout.write(payload)
            sys.stdout.write("\n")
        return config

    def serve(
        self,
        config_path: str | None,
        options: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        if options is None:
            options = {}
        require_api_dependencies()
        import uvicorn  # type: ignore

        if config is None:
            if not config_path:
                raise ValueError("Config path is required.")
            config = load_api_config(config_path)

        host = get_option(options, "host") or "127.0.0.1"
        port = int(get_option(options, "port") or 8000)
        api_key = get_option(options, "api_key") or os.environ.get("UNDATUM_API_KEY")
        cors_raw = get_option(options, "cors_origins")
        cors_origins = _split_csv(cors_raw) if cors_raw else []

        app = _build_api_app(config, api_key=api_key, cors_origins=cors_origins or None)
        resource_summaries = getattr(app.state, "resource_summaries", [])
        _print_startup_banner(host, port, resource_summaries)
        uvicorn.run(app, host=host, port=port, log_level="info", access_log=True)

    def run(self, input_files: list[str], options: dict[str, Any] | None = None) -> None:
        if options is None:
            options = {}
        options = dict(options)
        options["emit"] = False
        config = self.discover(input_files, options)
        self.serve(None, options, config=config)

    def export_openapi(
        self, config_path: str, options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if options is None:
            options = {}
        require_api_dependencies()

        config = load_api_config(config_path)
        app = _build_api_app(config)
        schema = app.openapi()

        output = get_option(options, "output")
        schema_format = get_option(options, "format")
        if output and not schema_format:
            schema_format = "yaml" if output.lower().endswith((".yml", ".yaml")) else "json"
        schema_format = (schema_format or "json").lower()

        if schema_format == "yaml":
            payload = yaml.safe_dump(schema, sort_keys=False, allow_unicode=False)
        else:
            payload = json.dumps(schema, indent=2, ensure_ascii=False)

        if output:
            with open(output, "w", encoding="utf8") as handle:
                handle.write(payload)
                if not payload.endswith("\n"):
                    handle.write("\n")
        else:
            sys.stdout.write(payload)
            if not payload.endswith("\n"):
                sys.stdout.write("\n")
        return schema
