# Change: Improve Documentation and Onboarding

## Why
Breadth marketing without fine print erodes trust (many format classes are read-only). The
large README is reference-grade but buries first success. Evaluators ask when to use undatum vs
miller/DuckDB/csvkit — a positioning page captures that question.

## What Changes
- Publish an honest format-support matrix (read/write, required extras, streaming support),
  preferably generated from code.
- Add task-oriented quickstarts: CSV→Parquet, validate-before-publish, query JSONL with SQL.
- Add a positioning page comparing undatum vs miller vs DuckDB vs csvkit.

## Impact
- Affected specs: `documentation`
- Affected code/docs: `README.md`, `docs/`, optional matrix generator script
- Related: pairs with P0 trust work so first impressions survive a 5-minute trial
