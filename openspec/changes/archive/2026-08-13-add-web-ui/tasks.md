## 0. Architecture (this change — no product web UI code)

- [x] 0.1 Record why the web UI exists and which personas it serves (`proposal.md`)
- [x] 0.2 Decide stack, extras, module map, session reuse, and slices (`design.md`)
- [x] 0.3 Specify entry-point, sampling, reuse, Data API boundary, and non-spreadsheet rules (`specs/web-ui/spec.md`)
- [x] 0.4 Mark roadmap item 4.6 as the web UI architecture proposal (not implementation)

## 1. Implementation — Slice A (blocked until this proposal is approved)

- [x] 1.1 Add `web` extra (FastAPI, uvicorn, Jinja2, python-multipart; Python 3.9)
- [x] 1.2 Add `undatum web` Typer command: `DependencyError`, default host `127.0.0.1`, `--limit`, `--open`
- [x] 1.3 FastAPI app + explore page: open path / upload, sample table, field list, CLI echo
- [x] 1.4 Call existing `TuiServices.load_sample` (do not import Textual screens)
- [x] 1.5 Tests: TestClient on `sample_csv_file`; CLI missing-extra exit 2
- [x] 1.6 Document install + `undatum web FILE` after Slice A works; exclude `web` from pipelines

## 2. Implementation — Slice B (after A)

- [x] 2.1 Profile via `TuiServices.profile` (progress disabled; background/thread)
- [x] 2.2 Frequency on selected field; filter on sample; export view
- [x] 2.3 Echo equivalent CLI on the page
- [x] 2.4 Tests for filter/export through the HTTP session

## 3. Implementation — Slice C (after B)

- [x] 3.1 SQL form with default LIMIT via `TuiServices.run_sql`
- [x] 3.2 Action list from `TuiAction` templates
- [x] 3.3 Reuse recent-file history (paths only)
- [x] 3.4 Tests for SQL limit default and action CLI templates

## 4. Implementation — Slice D (after C)

- [x] 4.1 Convert/save-as (`--low-memory`), validate sample, mask preview/write, pipeline YAML download
- [x] 4.2 CSRF on mutating POSTs; warn when `--host` is not localhost
- [x] 4.3 Tests for convert/validate/mask/pipeline through TestClient

## 5. Follow-on (separate change; not part of this archive)

Deferred after v1. Track in a future proposal, not this change:

- Visual pipeline DAG that exports the same YAML spec
- Inline `undatum plot` images (plotly/bokeh still deferred)
- Optional “open this session in Data API” convenience (read-only)
