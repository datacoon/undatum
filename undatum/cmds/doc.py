"""Dataset documentation command module."""
import csv
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from collections import Counter
from typing import Any, Optional

import pandas as pd
import yaml
from iterable.helpers.detect import detect_file_type, open_iterable
from tabulate import tabulate

from ..ai import get_ai_service, get_structured_metadata
from .analyzer import OBJECTS_ANALYZE_LIMIT, analyze
from ..common.schema_utils import duckdb_decompose
from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES, EU_DATA_THEMES
from ..utils import get_option, normalize_for_json

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'start_page']

GEO_FIELD_HINTS = {
    'country': {'country', 'country_code', 'countrycode', 'nation'},
    'region': {'region', 'state', 'province', 'county', 'district', 'city', 'municipality'},
    'coordinates': {'lat', 'latitude', 'lon', 'lng', 'longitude', 'x', 'y'}
}

DATE_FIELD_HINTS = {'date', 'time', 'timestamp', 'datetime', 'year', 'month', 'day'}

DATA_THEME_KEYWORDS = {
    "AGRI": {"agri", "agriculture", "farm", "crop", "soil", "livestock"},
    "ECON": {"economy", "economic", "finance", "trade", "gdp", "inflation"},
    "EDUC": {"education", "school", "student", "university", "training"},
    "ENVI": {"environment", "climate", "pollution", "emission", "biodiversity"},
    "ENER": {"energy", "power", "electric", "fuel", "gas", "oil"},
    "GOVE": {"government", "public", "administration", "policy", "budget"},
    "HEAL": {"health", "medical", "hospital", "disease", "patient"},
    "INTR": {"international", "foreign", "trade", "diplomacy"},
    "JUST": {"justice", "crime", "law", "court", "police"},
    "REGI": {"region", "regional", "urban", "rural", "territory"},
    "SOCI": {"social", "population", "demography", "welfare", "community"},
    "TECH": {"technology", "innovation", "digital", "software", "it"},
    "TRAN": {"transport", "traffic", "mobility", "road", "rail", "aviation"},
}

DATA_THEME_URI_BY_LABEL = {theme["label"]: theme["uri"] for theme in EU_DATA_THEMES}


def get_iterable_options(options: dict[str, Any]) -> dict[str, Any]:
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options and options[k] is not None:
            out[k] = options[k]
    if 'start_page' in out:
        out['page'] = out.pop('start_page')
    return out


def _format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def _normalize_outtype(outtype: str) -> str:
    """Normalize output type alias to canonical name."""
    if not outtype:
        return 'markdown'
    outtype = outtype.lower()
    if outtype in ['md', 'markdown']:
        return 'markdown'
    if outtype in ['txt', 'text']:
        return 'text'
    return outtype


def _build_title(filename: str) -> str:
    base = os.path.basename(filename)
    stem = os.path.splitext(base)[0]
    stem = re.sub(r'[_\-]+', ' ', stem).strip()
    if not stem:
        return base
    return stem.title()


def _get_primary_fields(report) -> list[str]:
    if report.tables:
        return [field.name for field in report.tables[0].fields or []]
    return []


def _iter_sample_values(samples: list[Any], field_names: list[str]):
    for sample in samples:
        if isinstance(sample, dict):
            for name in field_names:
                yield name, sample.get(name)
        elif isinstance(sample, list):
            for idx, name in enumerate(field_names):
                if idx < len(sample):
                    yield name, sample[idx]
        else:
            for name in field_names:
                yield name, None


def _extract_keywords(field_names: list[str], max_keywords: int = 15) -> list[str]:
    tokens = []
    for name in field_names:
        parts = re.split(r'[^A-Za-z0-9]+', name)
        tokens.extend([part.lower() for part in parts if len(part) > 2])
    stopwords = {"and", "or", "the", "for", "with", "from", "data", "info"}
    keywords = [token for token in tokens if token not in stopwords]
    if not keywords:
        return []
    counts = Counter(keywords)
    return [word for word, _ in counts.most_common(max_keywords)]


def _extract_geographic_coverage(samples: list[Any], field_names: list[str]) -> dict[str, Any]:
    field_map = {name: name.lower() for name in field_names}
    coverage = {
        "countries": [],
        "regions": [],
        "coordinates_present": False
    }
    coord_fields = {name for name, lname in field_map.items()
                    if any(hint in lname for hint in GEO_FIELD_HINTS["coordinates"])}
    if coord_fields:
        coverage["coordinates_present"] = True

    country_fields = {name for name, lname in field_map.items()
                      if any(hint in lname for hint in GEO_FIELD_HINTS["country"])}
    region_fields = {name for name, lname in field_map.items()
                     if any(hint in lname for hint in GEO_FIELD_HINTS["region"])}

    countries = []
    regions = []
    for name, value in _iter_sample_values(samples, field_names):
        if value is None:
            continue
        if name in country_fields and isinstance(value, str):
            val = value.strip()
            if 2 <= len(val) <= 64:
                countries.append(val)
        if name in region_fields and isinstance(value, str):
            val = value.strip()
            if 2 <= len(val) <= 64:
                regions.append(val)
    if countries:
        coverage["countries"] = sorted(set(countries))[:10]
    if regions:
        coverage["regions"] = sorted(set(regions))[:10]
    return coverage


def _extract_temporal_coverage(samples: list[Any], field_names: list[str]) -> Optional[dict[str, Any]]:
    candidate_fields = [
        name for name in field_names
        if any(hint in name.lower() for hint in DATE_FIELD_HINTS)
    ]
    if not candidate_fields:
        return None

    values = []
    for name, value in _iter_sample_values(samples, field_names):
        if name not in candidate_fields:
            continue
        if value is None:
            continue
        values.append(value)
    if not values:
        return None

    try:
        series = pd.to_datetime(values, errors='coerce')
    except Exception:
        return None
    series = series.dropna()
    if series.empty:
        return None

    start = series.min()
    end = series.max()
    has_time = any(getattr(dt, "hour", 0) or getattr(dt, "minute", 0) or getattr(dt, "second", 0)
                   for dt in series)
    granularity = "datetime" if has_time else "date"
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": granularity
    }


def _detect_languages(samples: list[Any], field_names: list[str]) -> list[dict[str, Any]]:
    try:
        from langdetect import detect  # type: ignore
    except Exception:
        return []

    texts = []
    for _name, value in _iter_sample_values(samples, field_names):
        if isinstance(value, str) and len(value.strip()) >= 20:
            texts.append(value.strip())
        if len(texts) >= 50:
            break

    if not texts:
        return []

    counts = Counter()
    total = 0
    for text in texts:
        try:
            lang = detect(text)
            counts[lang] += 1
            total += 1
        except Exception:
            continue
    if not total:
        return []
    return [
        {"code": code, "confidence": round(count / total, 2)}
        for code, count in counts.most_common(3)
    ]


def _guess_data_theme(field_names: list[str], keywords: list[str]) -> Optional[dict[str, str]]:
    tokens = set(keywords)
    for name in field_names:
        tokens.update([part.lower() for part in re.split(r'[^A-Za-z0-9]+', name) if part])

    best_label = None
    best_score = 0
    for label, theme_keywords in DATA_THEME_KEYWORDS.items():
        score = len(tokens.intersection(theme_keywords))
        if score > best_score:
            best_score = score
            best_label = label
    if not best_label or best_score == 0:
        return None
    return {
        "label": best_label,
        "uri": DATA_THEME_URI_BY_LABEL.get(best_label)
    }


def _build_sample_csv(samples: list[Any], field_names: list[str]) -> str:
    if not samples:
        return ""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    if field_names:
        writer.writerow(field_names)
    for sample in samples[:50]:
        if isinstance(sample, dict):
            row = [sample.get(name) for name in field_names]
        elif isinstance(sample, list):
            row = sample
        else:
            row = [sample]
        writer.writerow(row)
    return buffer.getvalue()


def _merge_ai_metadata(metadata: dict[str, Any], ai_metadata: dict[str, Any]) -> None:
    if not ai_metadata:
        return
    if isinstance(ai_metadata.get("keywords"), str):
        ai_metadata["keywords"] = [kw.strip() for kw in ai_metadata["keywords"].split(",") if kw.strip()]
    if isinstance(ai_metadata.get("languages"), dict):
        ai_metadata["languages"] = [ai_metadata["languages"]]
    for key in ("title", "keywords", "geographic_coverage", "temporal_coverage",
                "languages", "data_theme"):
        if ai_metadata.get(key) is not None:
            metadata[key] = ai_metadata[key]
    if ai_metadata.get("confidence") is not None:
        metadata["metadata_confidence"] = ai_metadata["confidence"]
    if ai_metadata.get("evidence") is not None:
        metadata["metadata_evidence"] = ai_metadata["evidence"]


def _parse_metacrafter_matches(entry: dict[str, Any]) -> list[dict[str, Any]]:
    matches = entry.get("matches") or entry.get("datatypes") or entry.get("types")
    if matches is None:
        matches = []
    if isinstance(matches, str):
        matches = [m.strip() for m in matches.split(",") if m.strip()]
    results = []
    for match in matches:
        if isinstance(match, dict):
            match_type = match.get("type") or match.get("name") or match.get("label")
            confidence = match.get("confidence")
        else:
            match_type = str(match).split()[0]
            confidence = None
            match_parts = str(match).split()
            if len(match_parts) > 1:
                try:
                    confidence = float(match_parts[-1])
                except ValueError:
                    confidence = None
        if match_type:
            results.append({
                "type": match_type,
                "url": entry.get("datatype_url"),
                "confidence": confidence
            })
    return results


def _run_metacrafter_scan(filename: str) -> Optional[list[dict[str, Any]]]:
    if shutil.which("metacrafter") is None:
        return None

    commands = [
        ["metacrafter", "scan-file", "--format", "json", filename],
        ["metacrafter", "scan-file", filename, "--format", "json"],
    ]
    for cmd in commands:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                continue
            output = result.stdout.strip()
            if not output:
                continue
            data = json.loads(output)
            if isinstance(data, dict):
                if "fields" in data:
                    return data["fields"]
                if "data" in data:
                    return data["data"]
            if isinstance(data, list):
                return data
        except (OSError, json.JSONDecodeError):
            continue
    return None


def _apply_semantic_types(report, metacrafter_entries: list[dict[str, Any]]) -> dict[str, Any]:
    pii_fields = []
    if not metacrafter_entries:
        return {"pii_fields": pii_fields}

    entry_map = {}
    for entry in metacrafter_entries:
        key = entry.get("key") or entry.get("name")
        if key:
            entry_map[key] = entry

    for table in report.tables or []:
        for field in table.fields or []:
            entry = entry_map.get(field.name)
            if not entry:
                field.semantic_types = []
                continue
            matches = _parse_metacrafter_matches(entry)
            field.semantic_types = matches
            if matches:
                field.sem_type = matches[0].get("type")
                field.sem_url = matches[0].get("url")
            tags = entry.get("tags") or []
            if isinstance(tags, str):
                tags = [tags]
            is_pii = any("pii" in str(tag).lower() for tag in tags)
            if is_pii:
                field.pii = True
            if is_pii or any("pii" in str(m.get("type", "")).lower() for m in matches):
                top_match = matches[0] if matches else {}
                pii_fields.append({
                    "field": field.name,
                    "type": top_match.get("type"),
                    "confidence": top_match.get("confidence")
                })
    return {"pii_fields": pii_fields}


def _mask_samples(samples: list[Any], field_names: list[str], pii_fields: list[dict[str, Any]]) -> list[Any]:
    if not pii_fields:
        return samples
    pii_set = {item.get("field") for item in pii_fields if item.get("field")}
    masked = []
    for sample in samples:
        if isinstance(sample, dict):
            new_sample = dict(sample)
            for key in pii_set:
                if key in new_sample:
                    new_sample[key] = "***"
            masked.append(new_sample)
        elif isinstance(sample, list):
            new_sample = list(sample)
            for idx, name in enumerate(field_names):
                if name in pii_set and idx < len(new_sample):
                    new_sample[idx] = "***"
            masked.append(new_sample)
        else:
            masked.append(sample)
    return masked


def _build_stats(fromfile: str, filetype: str, compression: str,
                 options: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Build statistics summary using DuckDB when available."""
    engine = get_option(options, 'engine') or 'auto'
    if engine not in ['auto', 'duckdb']:
        return None
    if filetype not in DUCKABLE_FILE_TYPES or compression not in DUCKABLE_CODECS:
        return None
    objects_limit = get_option(options, 'objects_limit') or OBJECTS_ANALYZE_LIMIT
    columns_raw = duckdb_decompose(
        filename=fromfile,
        filetype=filetype,
        path='*',
        limit=objects_limit,
        use_summarize=True
    )
    fields = []
    for column in columns_raw:
        if len(column) < 6:
            continue
        try:
            unique_count = int(column[3])
        except (ValueError, TypeError):
            unique_count = 0
        try:
            total_count = int(column[4])
        except (ValueError, TypeError):
            total_count = 0
        try:
            uniq_share = float(column[5])
        except (ValueError, TypeError):
            uniq_share = 0.0
        fields.append({
            'name': column[0],
            'type': column[1],
            'is_array': column[2] == 'True' if isinstance(column[2], str) else bool(column[2]),
            'unique_count': unique_count,
            'total_count': total_count,
            'uniqueness_percent': uniq_share
        })
    return {
        'engine': 'duckdb',
        'fields': fields
    }


def _build_samples(fromfile: str, options: dict[str, Any]) -> list[Any]:
    """Collect a bounded sample of records from the dataset."""
    sample_size = get_option(options, 'sample_size')
    if sample_size is None:
        sample_size = 10
    if sample_size <= 0:
        return []
    iterableargs = get_iterable_options(options)
    samples = []
    try:
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        try:
            for item in iterable:
                samples.append(normalize_for_json(item))
                if len(samples) >= sample_size:
                    break
        finally:
            iterable.close()
    except Exception as exc:
        logging.warning('doc: failed to sample records: %s', exc)
    return samples


def _build_doc_report(report, stats: Optional[dict[str, Any]], samples: list[Any],
                      pii_fields: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    """Assemble a documentation report from analysis results."""
    metadata = {
        'filename': report.filename,
        'file_size': report.file_size,
        'file_size_human': _format_file_size(report.file_size),
        'file_type': report.file_type,
        'compression': report.compression
    }
    metadata.update(report.metadata or {})

    tables = []
    for table in report.tables or []:
        fields = []
        for field in table.fields or []:
            semantic_types = field.semantic_types or []
            if not semantic_types and field.sem_type:
                semantic_types = [{
                    "type": field.sem_type,
                    "url": field.sem_url,
                    "confidence": None
                }]
            fields.append({
                'name': field.name,
                'type': field.ftype,
                'is_array': field.is_array,
                'description': field.description,
                'semantic_types': semantic_types,
                'pii': field.pii
            })
        tables.append({
            'id': table.id,
            'num_records': table.num_records,
            'num_cols': table.num_cols,
            'is_flat': table.is_flat,
            'description': table.description,
            'fields': fields
        })

    return {
        'metadata': metadata,
        'summary': {
            'total_tables': report.total_tables,
            'total_records': report.total_records
        },
        'schema': {
            'tables': tables
        },
        'statistics': stats,
        'samples': samples,
        'pii_fields': pii_fields or []
    }


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if value == "":
        return True
    if isinstance(value, (list, tuple, dict, set)) and not value:
        return True
    return False


def _format_markdown_inline(value: Any) -> str:
    if _is_empty_value(value):
        return "-"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            parts.append(f"{key}: {_format_markdown_inline(item)}")
        return "; ".join(parts) if parts else "-"
    if isinstance(value, (list, tuple, set)):
        items = [_format_markdown_inline(item) for item in value]
        items = [item for item in items if item]
        return ", ".join(items) if items else "-"
    return str(value)


def _format_markdown_value(value: Any) -> str:
    if _is_empty_value(value):
        return "-"
    if isinstance(value, dict):
        lines = []
        for key, item in value.items():
            lines.append(f"  - {key}: {_format_markdown_inline(item)}")
        return "\n".join(lines) if lines else "-"
    return _format_markdown_inline(value)


def _render_markdown(doc: dict[str, Any]) -> str:
    """Render documentation in Markdown format."""
    lines = ["# Dataset Documentation", ""]

    lines.append("## Metadata")
    for key, value in doc['metadata'].items():
        if isinstance(value, dict) and not _is_empty_value(value):
            lines.append(f"- **{key}**:")
            lines.append(_format_markdown_value(value))
        else:
            lines.append(f"- **{key}**: {_format_markdown_value(value)}")
    lines.append("")

    lines.append("## Summary")
    summary_rows = [[key, value] for key, value in doc['summary'].items()]
    lines.append(tabulate(summary_rows, headers=['Metric', 'Value'], tablefmt='github'))
    lines.append("")

    lines.append("## Schema")
    for table in doc['schema']['tables']:
        table_id = table['id'] or 'table'
        lines.append(f"### Table: {table_id}")
        lines.append(f"- Records: {table['num_records']}")
        lines.append(f"- Columns: {table['num_cols']}")
        lines.append(f"- Flat: {'Yes' if table['is_flat'] else 'No'}")
        if table.get('description'):
            lines.append("")
            lines.append("Summary:")
            lines.append(table['description'])
        field_rows = []
        has_semantic = any(field.get('semantic_types') for field in table['fields'])
        has_pii = any(field.get('pii') for field in table['fields'])
        headers = ['Field', 'Type', 'Array', 'Description']
        if has_semantic:
            headers.append('Semantic Types')
        if has_pii:
            headers.append('PII')
        for field in table['fields']:
            row = [
                field['name'],
                field['type'],
                'Yes' if field['is_array'] else 'No',
                field.get('description') or '-'
            ]
            if has_semantic:
                semantic_types = [item.get('type') for item in field.get('semantic_types', []) if item.get('type')]
                row.append(', '.join(semantic_types) if semantic_types else '-')
            if has_pii:
                row.append('Yes' if field.get('pii') else 'No')
            field_rows.append(row)
        if field_rows:
            lines.append("")
            lines.append(tabulate(
                field_rows,
                headers=headers,
                tablefmt='github'
            ))
        lines.append("")

    if doc.get('statistics'):
        lines.append("## Statistics")
        stat_rows = []
        for field in doc['statistics'].get('fields', []):
            stat_rows.append([
                field['name'],
                field['unique_count'],
                field['total_count'],
                f"{field['uniqueness_percent']:.2f}"
            ])
        if stat_rows:
            lines.append(tabulate(
                stat_rows,
                headers=['Field', 'Unique', 'Total', 'Unique %'],
                tablefmt='github'
            ))
        else:
            lines.append("No statistics available.")
        lines.append("")

    if doc.get('pii_fields'):
        lines.append("## PII Summary")
        pii_rows = []
        for item in doc.get('pii_fields', []):
            pii_rows.append([
                item.get('field'),
                item.get('type') or '-',
                item.get('confidence') if item.get('confidence') is not None else '-'
            ])
        if pii_rows:
            lines.append(tabulate(
                pii_rows,
                headers=['Field', 'Type', 'Confidence'],
                tablefmt='github'
            ))
        else:
            lines.append("No PII fields detected.")
        lines.append("")

    if doc['samples']:
        lines.append("## Samples")
        lines.append("```json")
        lines.append(json.dumps(doc['samples'], indent=2, ensure_ascii=False))
        lines.append("```")
        lines.append("")

    return "\n".join(lines)


def _render_text(doc: dict[str, Any]) -> str:
    """Render documentation in plain text format."""
    lines = []
    lines.append("DATASET DOCUMENTATION")
    lines.append("=" * 70)
    lines.append("")

    lines.append("Metadata")
    lines.append("-" * 70)
    meta_rows = [[key, value] for key, value in doc['metadata'].items()]
    lines.append(tabulate(meta_rows, headers=['Attribute', 'Value'], tablefmt='grid'))
    lines.append("")

    lines.append("Summary")
    lines.append("-" * 70)
    summary_rows = [[key, value] for key, value in doc['summary'].items()]
    lines.append(tabulate(summary_rows, headers=['Metric', 'Value'], tablefmt='grid'))
    lines.append("")

    lines.append("Schema")
    lines.append("-" * 70)
    for table in doc['schema']['tables']:
        table_id = table['id'] or 'table'
        lines.append(f"Table: {table_id}")
        lines.append(f"  Records: {table['num_records']}")
        lines.append(f"  Columns: {table['num_cols']}")
        lines.append(f"  Flat: {'Yes' if table['is_flat'] else 'No'}")
        if table.get('description'):
            lines.append("  Summary:")
            lines.append(f"  {table['description']}")
        field_rows = []
        has_semantic = any(field.get('semantic_types') for field in table['fields'])
        has_pii = any(field.get('pii') for field in table['fields'])
        headers = ['Field', 'Type', 'Array', 'Description']
        if has_semantic:
            headers.append('Semantic Types')
        if has_pii:
            headers.append('PII')
        for field in table['fields']:
            row = [
                field['name'],
                field['type'],
                'Yes' if field['is_array'] else 'No',
                field.get('description') or '-'
            ]
            if has_semantic:
                semantic_types = [item.get('type') for item in field.get('semantic_types', []) if item.get('type')]
                row.append(', '.join(semantic_types) if semantic_types else '-')
            if has_pii:
                row.append('Yes' if field.get('pii') else 'No')
            field_rows.append(row)
        if field_rows:
            lines.append(tabulate(
                field_rows,
                headers=headers,
                tablefmt='grid'
            ))
        lines.append("")

    if doc.get('statistics'):
        lines.append("Statistics")
        lines.append("-" * 70)
        stat_rows = []
        for field in doc['statistics'].get('fields', []):
            stat_rows.append([
                field['name'],
                field['unique_count'],
                field['total_count'],
                f"{field['uniqueness_percent']:.2f}"
            ])
        if stat_rows:
            lines.append(tabulate(
                stat_rows,
                headers=['Field', 'Unique', 'Total', 'Unique %'],
                tablefmt='grid'
            ))
        else:
            lines.append("No statistics available.")
        lines.append("")

    if doc.get('pii_fields'):
        lines.append("PII Summary")
        lines.append("-" * 70)
        pii_rows = []
        for item in doc.get('pii_fields', []):
            pii_rows.append([
                item.get('field'),
                item.get('type') or '-',
                item.get('confidence') if item.get('confidence') is not None else '-'
            ])
        if pii_rows:
            lines.append(tabulate(
                pii_rows,
                headers=['Field', 'Type', 'Confidence'],
                tablefmt='grid'
            ))
        else:
            lines.append("No PII fields detected.")
        lines.append("")

    if doc['samples']:
        lines.append("Samples")
        lines.append("-" * 70)
        lines.append(json.dumps(doc['samples'], indent=2, ensure_ascii=False))
        lines.append("")

    return "\n".join(lines)


def _write_doc_output(doc: dict[str, Any], outtype: str, output_stream) -> None:
    """Write documentation to output stream in the specified format."""
    if outtype == 'json':
        output_stream.write(json.dumps(doc, indent=2, ensure_ascii=False))
        output_stream.write('\n')
        return
    if outtype == 'yaml':
        output_stream.write(yaml.dump(doc, Dumper=yaml.Dumper))
        return
    if outtype == 'text':
        output_stream.write(_render_text(doc))
        return
    output_stream.write(_render_markdown(doc))


class Documenter:
    """Dataset documentation command handler."""
    def __init__(self):
        pass

    def document(self, fromfile: str, options: Optional[dict[str, Any]] = None) -> None:
        """Generate dataset documentation in multiple formats."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)

        outtype = _normalize_outtype(get_option(options, 'format') or 'markdown')
        output_file = get_option(options, 'output')

        format_in = get_option(options, 'format_in')
        filetype = format_in
        compression = 'raw'
        if filetype is None:
            ftype = detect_file_type(fromfile)
            if ftype['success']:
                filetype = ftype['datatype'].id()
                if ftype['codec'] is not None:
                    compression = ftype['codec'].id()

        encoding = get_option(options, 'encoding')
        objects_limit = get_option(options, 'objects_limit') or OBJECTS_ANALYZE_LIMIT

        report = analyze(
            fromfile,
            filetype=filetype,
            compression=compression,
            objects_limit=objects_limit,
            encoding=encoding,
            autodoc=options.get('autodoc', False),
            lang=options.get('lang', 'English'),
            ai_provider=options.get('ai_provider'),
            ai_config=options.get('ai_config')
        )

        stats = _build_stats(fromfile, report.file_type, report.compression, options)
        samples = _build_samples(fromfile, options)
        field_names = _get_primary_fields(report)
        metadata = report.metadata or {}
        metadata.setdefault('title', _build_title(report.filename))
        keywords = _extract_keywords(field_names)
        metadata.setdefault('keywords', keywords)
        metadata.setdefault('geographic_coverage', _extract_geographic_coverage(samples, field_names))
        metadata.setdefault('temporal_coverage', _extract_temporal_coverage(samples, field_names))
        metadata.setdefault('languages', _detect_languages(samples, field_names))
        metadata.setdefault('data_theme', _guess_data_theme(field_names, keywords))
        report.metadata = metadata

        if options.get('autodoc'):
            try:
                ai_config = options.get('ai_config') or {}
                ai_service = get_ai_service(provider=options.get('ai_provider'), config=ai_config)
                sample_csv = _build_sample_csv(samples, field_names)
                if sample_csv:
                    ai_metadata = get_structured_metadata(
                        sample_csv,
                        field_names,
                        language=options.get('lang', 'English'),
                        ai_service=ai_service
                    )
                    if ai_metadata and isinstance(ai_metadata.get("data_theme"), dict):
                        label = ai_metadata["data_theme"].get("label")
                        if label in DATA_THEME_URI_BY_LABEL and not ai_metadata["data_theme"].get("uri"):
                            ai_metadata["data_theme"]["uri"] = DATA_THEME_URI_BY_LABEL.get(label)
                        if label not in DATA_THEME_URI_BY_LABEL:
                            ai_metadata["data_theme"] = None
                    _merge_ai_metadata(metadata, ai_metadata)
            except Exception as exc:
                logging.warning('doc: failed to generate AI metadata: %s', exc)

        pii_fields = []
        if options.get('semantic_types') or options.get('pii_detect'):
            entries = _run_metacrafter_scan(fromfile)
            if entries:
                pii_fields = _apply_semantic_types(report, entries).get("pii_fields", [])
            else:
                logging.warning('doc: metacrafter not available or returned no results')
                for table in report.tables or []:
                    for field in table.fields or []:
                        field.semantic_types = []
        if options.get('pii_mask_samples') and pii_fields:
            samples = _mask_samples(samples, field_names, pii_fields)

        doc_report = _build_doc_report(report, stats, samples, pii_fields)

        if output_file:
            with open(output_file, 'w', encoding='utf8') as output_stream:
                _write_doc_output(doc_report, outtype, output_stream)
        else:
            _write_doc_output(doc_report, outtype, sys.stdout)
