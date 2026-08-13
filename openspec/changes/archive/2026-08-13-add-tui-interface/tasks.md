## 0. Architecture (this change — no product TUI code)

- [x] 0.1 Record why the TUI exists and which personas it serves (`proposal.md`)
- [x] 0.2 Decide library, extras, module map, session model, and slices (`design.md`)
- [x] 0.3 Specify entry-point, sampling, reuse, and non-spreadsheet rules (`specs/tui/spec.md`)
- [x] 0.4 Mark roadmap item 4.5 as the TUI architecture proposal (not implementation)

## 1. Implementation — Slice A (blocked until this proposal is approved)

- [x] 1.1 Confirm a Textual version range that supports Python 3.9; add `tui` extra
- [x] 1.2 Add `undatum tui` Typer command: TTY check, `DependencyError`, `--limit` and I/O flags
- [x] 1.3 Add `undatum/tui/services.py` (no Textual import): open path, sample rows, headers
- [x] 1.4 Add `UndatumApp` preview screen: DataTable, field list, status bar, help, quit
- [x] 1.5 Tests: services on `sample_csv_file`; CLI missing-extra exit 2; Pilot skip-if-missing
- [x] 1.6 Document install + `undatum tui FILE` after Slice A works

## 2. Implementation — Slice B (after A)

- [x] 2.1 Profile pane via `StatProcessor` (progress disabled; worker thread)
- [x] 2.2 Frequency on selected field
- [x] 2.3 Filter expression on sample; export sample/extract
- [x] 2.4 Echo equivalent CLI in the status/command log
- [x] 2.5 Tests for filter/export services and one Pilot path

## 3. Implementation — Slice C (after B)

- [x] 3.1 SQL pane with default LIMIT via `SqlExecutor`
- [x] 3.2 Command palette from a `TuiAction` table (title + CLI template)
- [x] 3.3 Optional recent-files history (`~/.undatum/tui-history.json`, paths only)
- [x] 3.4 Tests for SQL limit default and palette CLI templates

## 4. Follow-on (separate change; do not start in the first TUI PR)

- [x] 4.1 Slice D: convert/save-as, validate summary, mask preview, pipeline YAML export
- [x] 4.2 S3 open path in the file picker (reuse existing S3 helpers)
- [x] 4.3 Web UI proposal (`add-web-ui`) — architecture in `openspec/changes/add-web-ui/` (implementation gated on approval)
