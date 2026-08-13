# Contributor workflow and change process

## Canonical workflow sources
The primary workflow references in the repository are:
- `WORKFLOW_GUIDE.md`
- `openspec/AGENTS.md`
- `openspec/project.md`
- `README.md`

This page summarizes the practical workflow for future coding agents and human contributors.

## Daily development loop
1. Identify the user-facing surface that changes: CLI wrapper, command implementation, SDK, API, plugin tooling, or tests.
2. Check the matching tests first so you can preserve established behavior.
3. Make the smallest source change that satisfies the requirement.
4. Run focused tests for the touched area.
5. Expand to broader checks when behavior spans shared code paths.

## OpenSpec process
The repository uses OpenSpec for spec-driven changes. The workflow guidance in `openspec/AGENTS.md` and `WORKFLOW_GUIDE.md` is consistent on the key points:
- Use a proposal for new capabilities, breaking changes, architecture shifts, and meaningful performance/security changes.
- Skip proposals for bug fixes, typos, formatting changes, comments, or non-breaking dependency updates.
- For proposal-driven work, read the existing specs and active changes before editing.
- Validate with `openspec validate <change-id> --strict` before implementation.
- Do not implement proposal-scoped work until the proposal is approved.
- After deployment, archive completed changes and update specs as needed.

## Test and quality commands
The project’s main quality commands are defined in `pyproject.toml` and `Makefile`.

Useful commands:
- `pytest` — run the test suite
- `pytest tests/<module>.py -v` — run a focused test file
- `make lint` — Ruff + Pylint
- `make type-check` — mypy
- `make format-check` — Black check mode
- `make check-all` / `make ci` — broader pre-merge gate
- `make build` — build distributables

## What to watch out for
- `undatum/core.py` performs command registration and plugin loading during import, so import-time side effects matter.
- Many command wrappers in `undatum/cli/` are thin, so the real behavior often lives one layer deeper in `undatum/cmds/`.
- Some tests rely on optional dependencies and may skip when extras are not installed, especially API-related cases.
- The repo already has several modified files in the working tree during this run; inspect `git status` before making non-doc changes.

## For future agents
When you are asked to modify this repository:
- Read `openwiki/quickstart.md` first.
- Use the architecture page to find the correct command boundary.
- Use this page to decide whether a proposal is required and which validation commands matter.
- Prefer targeted tests over full-suite runs until behavior is stable.

## Related docs
- [Quickstart](quickstart.md)
- [Architecture and command surface](architecture.md)
