## Context
`repack` is a focused recompression command. Compression I/O already lives in `iterabledata`
(codecs with `profile` / `compression_level`, and format writers for Parquet/ORC/AVRO). undatum
should orchestrate detection, option mapping, atomic write, progress, and CLI UX—not reimplement
codecs.

## Goals / Non-Goals
- Goals:
  - Recompress container-coded files at max by default, same codec
  - Rewrite built-in-compression formats (Parquet/ORC/AVRO) at max native compression
  - Options for `--level` and `--progress`
  - Streaming / low-memory where the underlying engine supports it
- Non-Goals:
  - Changing file format (that remains `convert`)
  - Archive packing of directories into zip/7z as a primary UX (optional later)
  - Cross-codec container conversion as the default (same codec unless `--compression` set)

## Decisions

### Decision: Two compression modes
1. **Container codec path** — detect codec via `detect_file_type` / extension (`gz`, `zst`, …).
   Write via iterable convert/open with `codecargs={"profile": "max"}` or
   `{"compression_level": <level>}` when `--level` is set. Keep the same data format and codec
   extension.
2. **Built-in format path** — for `parquet` / `orc` / `avro` with `raw` (or no) container codec,
   rewrite records into the same format with format-native compression:
   - Parquet: default `compression=zstd` (override via `--compression`); pass level when the
     writer supports it
   - ORC: max numeric compression strategy/level via `toiterableargs`
   - AVRO: strongest practical codec (`zstandard` / `deflate`) via `--compression` default

### Decision: Default max via iterabledata profiles
Use `profile="max"` from `iterable.codecs.profiles` when `--level` is omitted. Explicit `--level`
always wins (matches iterabledata `resolve_profile` semantics).

### Decision: Atomic in-place when OUTPUT omitted
Write to a sibling temp file, then `os.replace` onto the input path. Avoids truncated originals
on failure. Remote/S3 paths require an explicit OUTPUT (or documented limitation).

### Decision: Refuse plain uncompressed data files
A bare `.csv` / `.jsonl` has nothing to “repack”. Error with suggestions:
`undatum convert in.csv out.csv.gz` or `undatum repack in.csv out.csv.zst`.

### Alternatives considered
- Extend `convert` with `--repack` — rejected; UX and defaults differ (convert changes format;
  repack preserves format and max-compresses).
- Always require OUTPUT — safer but clumsier for archival recompression; atomic in-place is
  common for compression tools and still safe.

## Risks / Trade-offs
- Max compression is slow and CPU-heavy → document and allow `--level` for faster runs.
- Parquet writer in iterabledata may not yet pass `compression_level` through to pyarrow →
  may need a thin local write path or upstream follow-up; ship codec switch to zstd first,
  then level if available.
- ZIP/7Z multi-member archives may need special handling → start with single-stream codecs;
  document archive limitations.

## Open Questions
- Should `--compression` also allow wrapping a raw file (e.g. `repack data.csv -o data.csv.zst`)?
  Proposal leans yes when OUTPUT extension implies a codec.
- Default Parquet codec: `zstd` (proposed) vs keep existing codec and only raise level.
