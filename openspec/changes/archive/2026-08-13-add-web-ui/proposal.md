# Change: Local web UI for dataset exploration

## Why

The TUI covers analysts who already live in a terminal. Less technical users, and
anyone who wants a browser table, charts, and a visible pipeline snippet, still have
to remember CLI flags. The MiroThinker roadmap listed a simple web UI after the TUI:
open or upload data, explore fields and distributions, export pipelines as YAML.
This change defines the architecture so a future `undatum web` does not become a
second processing engine, a public SaaS, or a fork of the Data API.

## What Changes

- Add an optional local web UI entered via `undatum web [path]`.
- Treat the UI as a **thin session layer** over the same processors the TUI already
  calls (`TuiServices` / `undatum/cmds/` / `Dataset`) — not a new data engine.
- Keep the existing **Data API** (`undatum api serve`) as the read-only machine API.
  The web UI is a human session on localhost, not a multi-tenant rewrite of that API.
- Ship behind extra `undatum[web]` (FastAPI + Jinja2/HTMX; no Node build for v1).
- Define sliced features: explore first, act second; visual DAG editor and hosted
  multi-user deployments stay out of scope.
- **No product implementation in this change until the proposal is approved.**
  Architecture lives in `design.md`; requirements in `specs/web-ui/spec.md`.
  Slices A–D are implemented; visual DAG and plotly remain follow-on.

## Impact

- Affected specs: new `web-ui` capability; parent roadmap item 4.6
  (`add-undatum-improvement-roadmap`); TUI follow-on task 4.3.
- Affected code (after approval): new `undatum/web/` package, `undatum/cli/web_cli.py`,
  `pyproject.toml` extra, tests under `tests/test_web*.py`. May re-export session
  helpers so the web extra does not depend on Textual.
- Packaging: default `pip install undatum` stays unchanged; web UI is opt-in.
- CLI stability: existing commands and `undatum api` are unchanged. The UI echoes
  equivalent `undatum …` invocations, same contract as the TUI.
- Security: default bind `127.0.0.1`; mutating actions are local-session POSTs, not
  a public write API.
