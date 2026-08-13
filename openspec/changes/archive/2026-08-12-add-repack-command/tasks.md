## 1. Command surface
- [x] 1.1 Add `repack` CLI in `undatum/cli/data_commands.py` with `INPUT`, optional `OUTPUT`,
      `--level`, `--progress`/`--no-progress`, optional `--compression`, `--verbose`
- [x] 1.2 Wire help text and examples (container codec + Parquet)

## 2. Implementation
- [x] 2.1 Create `undatum/cmds/repacker.py` with detection of container vs built-in compression
- [x] 2.2 Container path: recompress same codec using iterabledata `profile=max` or `--level`
- [x] 2.3 Built-in path: rewrite Parquet/ORC/AVRO with max native compression (Parquet default zstd)
- [x] 2.4 Atomic in-place when OUTPUT omitted; validate paths and refuse unsupported cases
- [x] 2.5 Progress indication via existing progress / convert `show_progress` helpers
- [x] 2.6 Actionable `ValidationError` / `FormatError` for plain uncompressed inputs

## 3. Tests & docs
- [x] 3.1 Unit/integration tests for `.csv.gz` (and one of zst/bz2) recompress with default max
- [x] 3.2 Test `--level` overrides default
- [x] 3.3 Test Parquet repack uses built-in compression (zstd by default)
- [x] 3.4 Test in-place atomic rewrite and progress flag wiring
- [x] 3.5 Brief README / FORMAT_SUPPORT note for `repack`
