# Change: Interactive TUI for dataset exploration

## Why

undatum is a strong non-interactive CLI, but first-look exploration still requires
remembering many commands (`table`, `headers`, `profile`, `frequency`, `sql`, `search`).
Analysts and stewards who already use the terminal want a visidata-like *session* —
preview, schema, stats, filter — without loading whole files into memory and without
forking a second processing engine. The MiroThinker roadmap listed `undatum tui` as
Phase 3; this change defines the architecture so implementation does not invent a
parallel product.

## What Changes

- Add an optional interactive TUI entered via `undatum tui [path]`.
- Treat the TUI as a **thin session layer** over existing `undatum/cmds/` processors
  and the `Dataset` SDK — not a new data engine.
- Ship behind extra `undatum[tui]` (Textual on top of existing Rich).
- Define a sliced feature set: explore first, act second; web UI and spreadsheet
  editing stay out of scope.
- **No implementation in this change until the proposal is approved.** Architecture
  lives in `design.md`; requirements in `specs/tui/spec.md`.

## Impact

- Affected specs: new `tui` capability; parent roadmap item 4.5
  (`add-undatum-improvement-roadmap`).
- Affected code (after approval): new `undatum/tui/` package, `undatum/cli/tui_cli.py`,
  `pyproject.toml` extra, tests under `tests/test_tui*.py`.
- Packaging: default `pip install undatum` stays unchanged; TUI is opt-in.
- CLI stability: existing commands are unchanged; TUI may *echo* equivalent CLI
  invocations so users can graduate to scripts and pipelines.
