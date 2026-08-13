# Change: Fix Gzip → DuckDB Codec Routing

## Why
`DUCKABLE_CODECS` lists `"gzip"` but iterabledata reports gzip compression as `"gz"`. Gzipped
files silently fall back to the slow Python engine, undermining DuckDB acceleration for the
exact large-file workflows reported in #34.

## What Changes
- Align codec identifiers so gzip (`.gz`) is recognized as DuckDB-eligible.
- Accept both `"gz"` and `"gzip"` (and keep `"zst"` / `"raw"`) in `DUCKABLE_CODECS` or normalize
  codec ids at the engine-selector boundary.
- Add regression tests that assert gzip-compressed CSV/JSONL selects the DuckDB path when
  format is otherwise supported.

## Impact
- Affected specs: `data-processing`
- Affected code: `undatum/constants.py`, `undatum/common/engine_selector.py`,
  `undatum/cmds/statistics/engine.py`, related DuckDB routing call sites, `tests/test_engine_selector.py`
- Related issues: #34 (performance path for compressed large files)
