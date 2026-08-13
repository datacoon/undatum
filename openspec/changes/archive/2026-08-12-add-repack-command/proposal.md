# Change: Add `repack` Command

## Why
Users often receive compressed datasets (`.gz`, `.zst`, Parquet with snappy, etc.) that were
written for speed rather than size. There is no first-class way to recompress a file at maximum
compression while preserving format—`convert` can change formats and codecs, but it is awkward
for “same format, tighter compression” workflows.

## What Changes
- Add `undatum repack INPUT [OUTPUT]` that recompresses a file at **maximum compression by default**.
- For **container codecs** (file extension / detected codec such as `gz`, `zst`, `bz2`, `xz`,
  `lz4`, `br`, `zip`, `7z`): stream-decompress and recompress with the **same codec** at max
  level (or an explicit `--level`).
- For **formats with built-in compression** (Parquet, ORC, AVRO): rewrite using the format’s
  native compression settings at max strength (default Parquet codec: `zstd`).
- Expose CLI options:
  - `--level` — explicit compression level (overrides the default max profile/level)
  - `--progress` / `--no-progress` — progress indication (default: on, matching `convert`)
  - Optional `--compression` to override the target codec for container or built-in formats
- When `OUTPUT` is omitted, perform an **atomic in-place** rewrite (temp file + replace).
- Uncompressed files without a container codec and without built-in compression (e.g. plain
  `.csv`) SHALL fail with an actionable error (suggest wrapping via `convert` or providing a
  compressed output path).

## Impact
- Affected specs: `data-processing` (new requirements)
- Affected code: new `undatum/cmds/repacker.py`, CLI wiring in `undatum/cli/data_commands.py`,
  tests under `tests/`, brief docs in README / FORMAT_SUPPORT if needed
- Dependencies: reuses `iterabledata` convert/open path and its `fast`/`balanced`/`max`
  compression profiles (`iterable.codecs.profiles`)
