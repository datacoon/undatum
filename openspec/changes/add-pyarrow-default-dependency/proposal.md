# Change: Add PyArrow as Default Dependency for Parquet

## Why
Parquet is a flagship convert target (issues #20, #34), but a default install can fail on
`convert x.csv x.parquet` without `pyarrow`. That fails the first-five-minutes test and
undermines trust for CLI data engineers.

## What Changes
- Add `pyarrow` to default (non-extra) dependencies in `pyproject.toml`, **or** ensure every
  Parquet code path fails with a precise install hint (`pip install 'undatum[parquet]'` / similar)
  if keeping it optional is preferred after review.
- Prefer default dependency so `pip install undatum` supports Parquet read/write without extras.
- Document Parquet support as available in the default install.

## Impact
- Affected specs: `data-processing`
- Affected code: `pyproject.toml`, converter Parquet paths, README install notes
- Related issues: #20, #34
- Trade-off: larger default install wheel; acceptable for flagship format UX
