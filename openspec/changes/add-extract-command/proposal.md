# Change: Add extract command for document-to-table ingestion

## Why
Undatum users often start from PDFs or Office documents and must pre-convert data before using
existing commands. A dedicated `extract` command removes that bottleneck while keeping the core
lightweight through optional dependencies.

## What Changes
- Add an `extract` command to ingest PDF/DOC/DOCX/XLS/XLSX sources into tabular outputs.
- Support outputs in CSV, JSON, NDJSON, Parquet, and optional Data Package format.
- Provide PDF-specific controls (`--method`, `--pages`, `--ocr`, `--flatten`) and multi-file
  extraction with `--output-dir`.
- Keep heavy extraction dependencies optional via extras or a plugin.

## Impact
- Affected specs: `specs/document-extraction/spec.md`
- Affected code: new command module in `undatum/cmds/`, packaging metadata for optional extras
