---
title: "repack"
description: "Verified, atomic recompression of local data files"
---
# `repack`

Recompress a **local** file while preserving its data format. Every output is written
to a sibling candidate, independently verified, and atomically published. This applies
to omitted OUTPUT, explicitly identical paths, and distinct output files.

The default profile is **max**. Same-representation optimization keeps the original
when the candidate is not smaller. With a distinct OUTPUT, an unchanged original is
copied atomically to that path so downstream pipeline steps still have their artifact.
Use `--allow-grow` to accept a larger verified candidate.

```bash
# Safely optimize in place
undatum repack data.csv.gz

# Measure actual encoding and verification without replacing any file
undatum repack data.parquet --dry-run --json

# Faster profile; --level overrides the profile's codec-specific level
undatum repack data.jsonl.zst out.jsonl.zst --profile fast --level 3

# Positional OUTPUT and --output/-o are equivalent
undatum repack data.csv.gz -o out.csv.gz --min-savings 1

# Explicit outer-codec migration preserves the exact decoded bytes
undatum repack data.csv.gz data.csv.zst

# Wrap raw data without parsing or normalizing it
undatum repack data.csv data.csv.gz
```

## Preservation

- **Streams:** gzip, bzip2, XZ, LZMA-alone, zstd, LZ4 and Brotli. Decoded byte length and
  SHA-256 are checked through a complete independent read-back. CSV quoting, BOM,
  CRLF and empty lines are retained. Concatenated supported frames are read in full.
- **Gzip:** the first member's name, comment and mtime are retained; temporary names
  never enter the header. Format-specific extra headers such as BGZF are refused.
  Multiple members may be merged into one with the same decoded payload.
- **Parquet:** schema, field IDs, metadata, values, order and supported physical types
  are verified. Empty files remain valid. INT96 and uniform sorting declarations are
  preserved; unrepresentable physical types, mixed sorting declarations or external
  chunks fail safely. Compression, statistics and row-group layout can change.
- **ORC:** original typed schema, user metadata and nanosecond timestamps are retained.
  Union branch preservation and older ORC format versions are currently unsupported.
  Native codecs are `none`, `zlib`, `snappy`, `lz4` and `zstd`; LZO encoding is refused.
- **Avro:** original encoded blocks retain schema bytes, custom metadata, sync markers,
  union branches and record order. Supported native codecs are `null`, `deflate`,
  `bzip2`, `zstandard` and optional `snappy`. Individual encoded and decoded blocks
  are bounded to 64 MiB. Apache Avro's legacy three-byte deflate trailer is accepted
  only after checksum verification.
- **DuckDB:** offline database compaction uses `COPY FROM DATABASE` into a fresh
  database, followed by `CHECKPOINT` and independent catalog and typed-value checks.
  Supported schemas, tables, constraints, indexes, defaults, comments and tags are
  retained. Nested values, nulls, decimal precision, nanosecond timestamps and floating
  point bits are checked. Foreign keys, views, sequences, macros/functions, custom types and types
  outside the supported Arrow profile (such as intervals) are refused.
- **SQLite:** `VACUUM INTO` compacts an offline database without changing its format.
  Independent read-back checks schema, index/trigger/view definitions, typed values,
  raw TEXT/BLOB bytes, hidden rowids and composite `WITHOUT ROWID` records. Application
  metadata, sequences/statistics, page settings, encoding and persistent WAL mode
  are retained; both source and candidate pass `integrity_check`.

Parquet and ORC default to native `zstd`; Avro defaults to `zstandard`. Native Parquet
and Avro honor supported codec levels. ORC has a compression strategy: `fast` uses
speed, `balanced`/`max` use compression; explicit ORC `--level` is an error.

### DuckDB databases

```bash norun
undatum repack warehouse.duckdb
undatum repack warehouse.db compact.duckdb --compression auto
undatum repack warehouse.duckdb --dry-run --json
```

The native header identifies DuckDB even with `.db`, `.ddb` or no extension. DuckDB
chooses its internal compression automatically; all three profiles use the same copy
strategy. `--compression auto` is optional, `--level` and other codecs are errors, and
`--threads` must be 1. Results report `codec: auto`, `level: null`,
`strategy: copy_database` and the DuckDB runtime version. File layout/storage version
can change with the runtime; compatibility with every older reader is not guaranteed.

Close all database users before repacking. A source or destination `.wal` causes refusal,
including if it appears before publication. An absent WAL does not establish exclusive
ownership against other users opening the file. Only private candidate WALs are cleaned
up; source/destination WALs are never deleted.

The initial conservative profile, adapted from filerepack, supports **256 MiB physical
input**, **1,000,000 total table rows**, **256 MiB Arrow values per verification side**
and a **4 MiB serialized catalog**. A single engine thread uses a 256 MB engine memory
limit with disk spill disabled; this is not a process RSS limit. Use `--memory-limit`
for supervised RSS. `--batch-size` controls verification batches up to 1024 rows.
Logical-byte accounting includes the source scan and both verification sides;
temporary space includes the candidate and its WAL.

Foreign keys are refused before staging because DuckDB's database copy can load a
referencing table before its referenced rows. Primary keys, unique/check/not-null
constraints and defaults are supported and verified.

Arrow/Feather, HDF5, netCDF, Zarr and ZIP-based packages do not yet have native adapters here. Outer
stream recompression of a database (for example `.duckdb.gz`) preserves the decoded
database bytes and does not compact its internal tables.

### SQLite databases

```bash
# Write a new compact database; .db is identified by its native header
undatum repack db.db compact.sqlite

# Measure real savings without replacing files
undatum repack db.db --dry-run --json

# Close all database users, then explicitly permit in-place replacement
undatum repack db.db --sqlite-offline
```

Replacing **any existing destination**, including in-place and a distinct prior OUTPUT,
requires `--sqlite-offline` (Python: `sqlite_offline=True`) as an assertion that database
users are closed. A new OUTPUT and `--dry-run` do not require that replacement assertion.
Source and output must have no `-wal`, `-shm` or `-journal` sidecars. Any such sidecar,
including one detected before publication, causes refusal even with `--sqlite-offline`.
The assertion and absence of sidecars do not establish an exclusive lock against a
user opening a database during replacement. Source/output sidecars are never deleted.

SQLite compaction reclaims unused pages; it does not add a compression codec. Omitted
`--compression`, `auto` and `none` are accepted; `--level`, other codecs and
`--threads` other than 1 are errors. All profiles use the same operation. Reports show
`codec: none`, `level: null`, `strategy: vacuum_into` and the SQLite runtime version.
No fallback to ordinary `VACUUM` is used because that can renumber hidden rowids.
Some SQLite runtimes also renumber hidden rowids during `VACUUM INTO` for particular
table definitions (for example, a CHECK constraint). Verification refuses those
candidates and preserves the source and any existing destination.

Python's SQLite **3.31 or newer** is required; there is no extra package dependency.
There is no total file-size or row-count cap. The supported profile limits catalog
metadata to 4 MiB and tables to 999 columns. Virtual tables (including FTS/RTree),
unavailable table-expression functions/collations and tables shadowing every hidden
rowid alias are refused. `WITHOUT ROWID` primary keys are inspected natively. Names
such as `.sqlite`, `.sqlite3`, `.db`, `.gpkg`, `.mbtiles`, `.sqlitedb` and `.vscdb`, or
no extension, are supported when the actual schema fits that profile.
Files with extension-reserved page bytes, trailing opaque data or incomplete pages
are refused because the native rewrite cannot prove preservation of that payload.

Verification streams batches of at most 1024 rows. A single large value or native
allocation can exceed that transfer size; use `--memory-limit` for worker RSS
supervision. Logical-byte accounting measures framed typed values, including row
identities and both verification sides; `rows` includes stored sequence/statistics
table records. Temporary-space accounting includes owned SQLite WAL/SHM/journal
files. SQLite progress callbacks also check cooperative budgets during native work.
Closed WAL-mode databases retain their persistent WAL mode after compaction.

The source's mode, modification time and supported extended attributes are retained.
Detected changes to the source, candidate or an existing destination abort publication.
In-place symbolic links and multiply hard-linked files are refused; use a distinct
OUTPUT. Atomic replacement provides visibility, **not power-loss durability** or a
portable lock against other writers. Ownership, ACLs and alternate data streams are
outside this metadata contract.

## Savings and reports

`--min-savings` is a percentage of the physical source size, validated in `0–100`.
A same-representation candidate must be strictly smaller unless `--allow-grow` is set.
Explicit outer-codec changes and raw wrapping are honored even if they grow; an explicit
savings threshold can refuse publication. Gzip bytes are never copied under a zstd suffix.

`--dry-run` performs real encoding and verification, then removes the candidate. It
reports measured candidate savings, leaves the source and any prior OUTPUT intact,
and can cost as much as a normal repack.

`--json` or `-O json` prints one `undatum.repack/1` document. Key distinctions:

- `input_size` and `candidate_size`: physical sizes before and after encoding.
- `output_size`: actual destination size, or null if no destination exists.
- `candidate_savings_*`: measured potential savings, including during dry-run.
- `savings_*`: realized savings; zero when nothing was published.
- `status`: `replaced`, `unchanged`, `skipped` or `dry_run`.
- `published` and `changed`: distinguish an original-copy fallback from applying compression.
- `would_publish`: the acceptance decision before dry-run prevents publication.
- `effective_settings`: candidate codec, profile, level, threads, optional ORC strategy
  and whether those settings were applied to the destination.
- `verification`, `timings` and `resources`: preservation proof, encode/verify times,
  processed logical bytes, candidate scratch peak and worker RSS monitoring.

Existing Python `Repacker.repack(input, output, options)` calls continue returning a
`dict`, with legacy `bytes_read`/`bytes_written` aliases. For native ORC, decoded-byte
accounting measures the typed verification representation, rather than physical bytes.

## Resources and dependencies

`--max-decoded-bytes` limits **cumulative** decoded/logical work, including verification.
`--temp-limit` limits candidate and fallback file sizes, plus owned database sidecars.
`--batch-size` controls Parquet
transfer rows; `--row-group-size` controls its physical layout separately. Neither is a
hard memory cap for native dictionaries, column chunks or individual large records.

`--memory-limit` (bytes) supervises a native worker's process-tree RSS; `--timeout`
(seconds) terminates that worker when the operation deadline is exceeded. Monitoring
samples every 20 ms and checks the OS peak RSS on supported platforms before accepting
the result. Parent-side source hashing, preflight and publication are outside the worker
RSS budget; their time is included in the deadline. Interruptions discard the candidate.

```bash norun
pip install "undatum[compression]"  # optional stream codecs and Avro snappy/zstd
pip install "undatum[repack]"       # psutil for worker RSS supervision
undatum repack data.csv.zst --memory-limit 536870912 --timeout 120
```

`--threads` applies to zstd streams and native Avro zstandard. Other backends reject
unsupported thread counts or levels before writing. Profiles reuse iterabledata's
codec-specific settings; native Parquet levels use PyArrow's actual capabilities.

Local files only. Use `convert` for remote URIs or data-format changes. A changed outer
codec requires a compatible distinct output suffix; native `data.parquet` cannot be
rewritten as bare Parquet under `.parquet.gz`. ZIP/7z and other stream codecs without a
preservation adapter are refused instead of processing just one member.

### Updating existing scripts

Add `--allow-grow` if a script relies on always replacing a same-representation file.
For outer-codec changes, provide a distinct OUTPUT whose suffix matches the requested
codec. Remove ignored/unsupported `--level` arguments (ORC uses strategies), and use
`convert` for data-format changes. Upgrade pyorc to at least 0.11; older native wheels
can crash when used in the same process as PyArrow 16 on macOS. `-` is refused for
repack input/output; its JSON summary can still be piped normally.

See [JSON result schemas](/commands/json-output), [format support](/formats/) and
[repack benchmarks](/development/benchmarks).

<!-- BEGIN GENERATED: reference -->
<!-- Generated by scripts/generate_cli_reference.py from the CLI; do not edit by hand. -->

## Reference

**Reads:** local verified streams, native Parquet/ORC/Avro and offline DuckDB/SQLite · **Writes:** the same format, verified and atomically published · **Memory:** stream/batch based; optional native worker RSS limit · **Engines:** python

```text
undatum repack [OPTIONS] INPUT_FILE [OUTPUT]
```

| Argument | Description |
|----------|-------------|
| `INPUT_FILE` | Path to input file to recompress. (required) |
| `OUTPUT` | Output path (defaults to atomic in-place rewrite of INPUT). |

| Option | Description | Default |
|--------|-------------|---------|
| `-o`, `--output` TEXT | Output path (alias for positional OUTPUT). |  |
| `-l`, `--level` INTEGER | Compression level (overrides default maximum / max profile). |  |
| `--profile` TEXT | Compression profile: fast, balanced, max. | `max` |
| `--dry-run` | Encode and verify without publishing; report measured savings. |  |
| `--sqlite-offline` / `--no-sqlite-offline` | Assert SQLite users are closed before replacing an existing path. | `--no-sqlite-offline` |
| `--allow-grow` | Allow a larger verified optimization candidate. |  |
| `--min-savings` FLOAT | Minimum candidate savings percentage (0–100). |  |
| `--max-decoded-bytes` INTEGER | Cumulative decoded-byte limit, including verification. |  |
| `--temp-limit` INTEGER | Maximum candidate/fallback plus owned database sidecars, in bytes. |  |
| `--memory-limit` INTEGER | Native worker RSS limit in bytes (requires undatum[repack]). |  |
| `--timeout` FLOAT | Operation wall-time limit in seconds; terminate the native worker. |  |
| `--batch-size` INTEGER | Parquet transfer / database verification batch rows (databases max 1024). | `65536` |
| `--row-group-size` INTEGER | Target Parquet row-group rows, separate from batch size. | `65536` |
| `--threads` INTEGER | Codec threads (zstd streams/Avro only). | `1` |
| `-O`, `--format-out` TEXT | Summary format: text or json. |  |
| `--json` | Print the result as one JSON document (same as --format-out json). |  |
| `--compression` TEXT | Override compression codec. For container files: gz/zst/bz2/…. For Parquet/ORC/AVRO: format-native codec (e.g. zstd, snappy). DuckDB: auto only. SQLite compaction: none/auto. Databases reject --level. |  |
| `--progress` / `--no-progress` | Show progress bar. | `--progress` |
| `--verbose` / `--no-verbose` | Enable verbose logging output. | `--no-verbose` |

See also [shared options](/commands/shared-options).
<!-- END GENERATED: reference -->
