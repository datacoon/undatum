# Change: Remove MistQL support

## Why

MistQL was an experimental query language (`undatum query`) and the iterable
fallback for `--filter`. DuckDB SQL (`undatum sql`) is the supported ad-hoc
query path, and `--filter` already has a comparison/boolean subset that
translates to SQL. Keeping MistQL added a dependency and a second language.

## What Changes

- Remove the `mistql` package dependency.
- Remove the experimental `undatum query` command. Use `undatum sql` or
  `select --filter`.
- Evaluate `--filter` comparison/boolean expressions in-process (same subset
  as SQL pushdown). Unsupported syntax (`LIKE`, `IN`, `match`, nested fields)
  errors with a pointer to `undatum sql`.
- Keep `&&`/`||` as aliases for `AND`/`OR`.

## Impact

- Affected specs: `querying` (filter fallback no longer uses MistQL `match`)
- Breaking: `undatum query` is gone; MistQL-only filter forms (`match`, pipes)
  no longer work.
- `db query` is unchanged (SQL against databases).
