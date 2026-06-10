"""Frictionless Data Package generation command module."""
import json
import logging
import os
import re
import shutil
import tempfile
import urllib.parse
import urllib.request
from typing import Any, Optional

from .analyzer import OBJECTS_ANALYZE_LIMIT, analyze
from .doc import (
    _build_sample_csv,
    _build_samples,
    _build_title,
    _detect_languages,
    _extract_geographic_coverage,
    _extract_keywords,
    _extract_temporal_coverage,
    _get_primary_fields,
    _guess_data_theme,
    _merge_ai_metadata,
)
from ..ai import get_ai_service, get_structured_metadata
from ..constants import EU_DATA_THEMES
from ..utils import get_option

logger = logging.getLogger(__name__)

DATA_THEME_URI_BY_LABEL = {theme["label"]: theme["uri"] for theme in EU_DATA_THEMES}


def _is_url(path: str) -> bool:
    parsed = urllib.parse.urlparse(path)
    return parsed.scheme in {"http", "https"}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "dataset"


def _parse_kv_entries(value: Optional[str], default_key: str) -> list[dict[str, str]]:
    if not value:
        return []
    entries = []
    for raw_item in value.split(";"):
        raw_item = raw_item.strip()
        if not raw_item:
            continue
        if "=" not in raw_item:
            entries.append({default_key: raw_item})
            continue
        entry: dict[str, str] = {}
        for part in raw_item.split(","):
            part = part.strip()
            if not part:
                continue
            if "=" not in part:
                continue
            key, val = part.split("=", 1)
            key = key.strip()
            val = val.strip()
            if key and val:
                entry[key] = val
        if not entry:
            entry[default_key] = raw_item
        entries.append(entry)
    return entries


def _normalize_keywords(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    keywords = [kw.strip() for kw in value.split(",") if kw.strip()]
    return keywords or None


def _download_to_temp(url: str) -> str:
    suffix = os.path.splitext(urllib.parse.urlparse(url).path)[-1]
    handle, temp_path = tempfile.mkstemp(suffix=suffix)
    os.close(handle)
    urllib.request.urlretrieve(url, temp_path)
    return temp_path


def _map_field_type(field_type: str, is_array: bool) -> dict[str, Any]:
    type_map = {
        "VARCHAR": "string",
        "BIGINT": "integer",
        "INTEGER": "integer",
        "DOUBLE": "number",
        "FLOAT": "number",
        "BOOLEAN": "boolean",
        "DATE": "date",
        "TIMESTAMP": "datetime",
        "STRUCT": "object",
        "JSON": "object",
    }
    base_type = type_map.get(field_type, "string")
    if is_array:
        return {"type": "array", "items": {"type": base_type}}
    return {"type": base_type}


def _build_resource_schema(report) -> dict[str, Any]:
    fields = []
    table = report.tables[0] if report.tables else None
    if not table:
        return {"fields": fields}
    for field in table.fields or []:
        field_schema = {
            "name": field.name,
        }
        field_schema.update(_map_field_type(field.ftype, field.is_array))
        if field.description:
            field_schema["description"] = field.description
        fields.append(field_schema)
    return {"fields": fields}


def _build_package_metadata(report, samples: list[Any], options: dict[str, Any]) -> dict[str, Any]:
    metadata = report.metadata or {}
    field_names = _get_primary_fields(report)
    metadata.setdefault("title", _build_title(report.filename))
    keywords = _extract_keywords(field_names)
    metadata.setdefault("keywords", keywords)
    metadata.setdefault("geographic_coverage", _extract_geographic_coverage(samples, field_names))
    metadata.setdefault("temporal_coverage", _extract_temporal_coverage(samples, field_names))
    metadata.setdefault("languages", _detect_languages(samples, field_names))
    metadata.setdefault("data_theme", _guess_data_theme(field_names, keywords))
    if not metadata.get("description"):
        table = report.tables[0] if report.tables else None
        if table and table.description:
            metadata["description"] = table.description
    if options.get("autodoc"):
        try:
            ai_config = options.get("ai_config") or {}
            ai_service = get_ai_service(provider=options.get("ai_provider"), config=ai_config)
            sample_csv = _build_sample_csv(samples, field_names)
            if sample_csv:
                ai_metadata = get_structured_metadata(
                    sample_csv,
                    field_names,
                    language=options.get("lang", "English"),
                    ai_service=ai_service,
                )
                if ai_metadata and isinstance(ai_metadata.get("data_theme"), dict):
                    label = ai_metadata["data_theme"].get("label")
                    if label in DATA_THEME_URI_BY_LABEL and not ai_metadata["data_theme"].get("uri"):
                        ai_metadata["data_theme"]["uri"] = DATA_THEME_URI_BY_LABEL.get(label)
                    if label not in DATA_THEME_URI_BY_LABEL:
                        ai_metadata["data_theme"] = None
                _merge_ai_metadata(metadata, ai_metadata)
            else:
                logger.debug("package: AI metadata skipped (no sample CSV)")
        except Exception as exc:
            logging.warning("package: failed to generate AI metadata: %s", exc)
    return metadata


class Packager:
    """Frictionless Data Package command handler."""

    def __init__(self) -> None:
        pass

    def create(self, input_files: list[str], options: Optional[dict[str, Any]] = None) -> None:
        if options is None:
            options = {}
        if not input_files:
            raise ValueError("No input files provided.")

        package_dir = get_option(options, "package_dir")
        output_file = get_option(options, "output")
        if package_dir:
            os.makedirs(package_dir, exist_ok=True)
            if output_file:
                output_file = os.path.join(package_dir, os.path.basename(output_file))
            else:
                output_file = os.path.join(package_dir, "datapackage.json")
        else:
            if output_file is None:
                output_file = os.path.join(os.getcwd(), "datapackage.json")

        ai_config = options.get("ai_config") or {}
        engine = get_option(options, "engine") or "auto"
        objects_limit = get_option(options, "objects_limit") or OBJECTS_ANALYZE_LIMIT

        resources = []
        primary_report = None
        primary_input = None
        temp_files = []

        try:
            for idx, input_path in enumerate(input_files):
                working_path = input_path
                if _is_url(input_path):
                    working_path = _download_to_temp(input_path)
                    temp_files.append(working_path)

                report = analyze(
                    working_path,
                    filetype=get_option(options, "format_in"),
                    objects_limit=objects_limit,
                    encoding=get_option(options, "encoding"),
                    engine=engine,
                    autodoc=options.get("autodoc", False),
                    lang=options.get("lang", "English"),
                    ai_provider=options.get("ai_provider"),
                    ai_config=ai_config if ai_config else None,
                )
                if primary_report is None:
                    primary_report = report
                    primary_input = input_path

                resource_name = os.path.splitext(os.path.basename(input_path))[0] or f"resource_{idx + 1}"
                resource_path = input_path
                if package_dir and os.path.exists(input_path) and not _is_url(input_path):
                    dest_path = os.path.join(package_dir, os.path.basename(input_path))
                    if os.path.abspath(input_path) != os.path.abspath(dest_path):
                        shutil.copy2(input_path, dest_path)
                    resource_path = os.path.basename(dest_path)

                resource = {
                    "name": resource_name,
                    "path": resource_path,
                    "schema": _build_resource_schema(report),
                }
                resources.append(resource)
        finally:
            for temp_path in temp_files:
                try:
                    os.remove(temp_path)
                except OSError:
                    continue

        if primary_report is None:
            raise ValueError("Failed to generate package resources.")

        if primary_report and primary_input:
            primary_report.filename = primary_input
        samples = _build_samples(primary_report.filename, options)
        metadata = _build_package_metadata(primary_report, samples, options)

        name = get_option(options, "name") or _slugify(metadata.get("title") or primary_report.filename)
        title = get_option(options, "title") or metadata.get("title")
        description = get_option(options, "description") or metadata.get("description")
        keywords = _normalize_keywords(get_option(options, "keywords")) or metadata.get("keywords")

        package = {
            "name": name,
            "resources": resources,
        }
        if title:
            package["title"] = title
        if description:
            package["description"] = description
        if keywords:
            package["keywords"] = keywords

        licenses = _parse_kv_entries(get_option(options, "licenses"), "name")
        if licenses:
            package["licenses"] = licenses

        sources = _parse_kv_entries(get_option(options, "sources"), "title")
        if sources:
            package["sources"] = sources

        contributors = _parse_kv_entries(get_option(options, "contributors"), "title")
        if contributors:
            package["contributors"] = contributors

        version = get_option(options, "version")
        if version:
            package["version"] = version

        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(output_file, "w", encoding="utf8") as output_stream:
            output_stream.write(json.dumps(package, indent=2, ensure_ascii=False))
            output_stream.write("\n")
