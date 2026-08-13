# Change: User-Needs Improvement Roadmap

## Why
External user signals (GitHub issues, PyPI downloads, downstream CrateDB usage) show that
undatum's loudest unmet needs are trust (broken installs), scale (OOM on multi-GB files), and
distribution (Homebrew/single-binary) — not more format breadth. Recent feature expansion
(API/AI/MCP/pipelines) widened scope while #34 and #4 stayed open. This roadmap consolidates
the core before the next feature wave.

## Implementation Reference
**Primary Reference Document:** `dev/docs/undatum-improvement-recommendations.md`

That document summarizes mined signals (22 issues, 15 PRs, ~105 downloads/day) and a
prioritized P0–P5 plan. All follow-on proposals below map to numbered recommendations in it.

## What Changes
- Define a user-needs-driven phased roadmap distinct from the MiroThinker platform roadmap
  (`add-undatum-improvement-roadmap`).
- Sequence: **P0 trust** → discovery (list submission) → **P1/P2 scale & install** → **P5 community**.
- Scaffold concrete OpenSpec change proposals for each actionable recommendation.
- Map overlapping items to existing proposals where work already exists:
  - P1.8 multiprocessing → `add-parallel-processing`
  - P1.9 DuckDB pushdown → `improve-duckdb-operations`
  - P4.18 diff/apply → archived `add-diff-command` (diff shipped; apply deferred/close)

## Follow-on Change Proposals

| Priority | Change ID | Recommendation |
|----------|-----------|----------------|
| P0.1 | `fix-gzip-duckdb-codec-routing` | Fix gzip→DuckDB codec id mismatch |
| P0.2 | `add-pyarrow-default-dependency` | Parquet works out of the box |
| P0.3 | `add-streaming-parquet-low-memory` | Fix #34 OOM; `--low-memory` mode |
| P0.4 | `add-ci-install-gate` | Clean-venv wheel smoke tests |
| P0.5 | `improve-repo-hygiene` | Drop junk artifacts; fix CHANGELOG |
| P1.6 | `add-streaming-sort-dedup` | External-merge sort; disk-backed dedup |
| P1.7 | `improve-csv-delimiter-sniffing` | csv.Sniffer + multi-line sampling |
| P1.8 | *(existing)* `add-parallel-processing` | Multiprocessing (#18) |
| P1.9 | *(existing)* `improve-duckdb-operations` | Widen DuckDB auto-routing |
| P2 | `improve-distribution-install` | Homebrew, single-binary, uv/pipx docs |
| P3 | `improve-docs-onboarding` | Format matrix, quickstarts, positioning |
| P4.16 | `add-excel-command-parity` | Excel in analyze/uniq/frequency/select (#11) |
| P4.17 | `add-db-dump-command` | `db dump --to parquet` (#13) |
| P4.18–19 | *(tracking only)* | Close/defer apply-patch & Veriform |
| P5 | `improve-community-positioning` | Discussions, list submission, Snyk, semver |

## Impact
- Affected specs: `roadmap-planning`, plus deltas in follow-on proposals
  (`data-processing`, `release-quality`, `distribution`, `documentation`,
  `database-integration`, `community`).
- Affected code: converter, engine selector, CI workflows, packaging, docs, sort/dedup,
  CSV detection, Excel command gates, db commands.
- Strategic constraint: do not expand niche formats until P0 items 1–3 land.
