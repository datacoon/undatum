# Format Support Matrix

undatum supports **100+ formats** via [iterabledata](https://github.com/datenoio/iterabledata/).
Capabilities differ by format: many are **read-only**, some require extras, and streaming
support varies.

## How to inspect live capabilities

```bash
# Human-readable catalog
undatum formats list

# Capability matrix (read / write / streaming / …)
undatum formats list --capabilities

# Machine-readable
undatum formats list --capabilities --json
```

## Core formats (typical CLI workflows)

| Format | Read | Write | Streaming | Notes |
|--------|------|-------|-----------|-------|
| CSV / TSV | yes | yes | yes | Delimiter auto-detected (`,`;`\\t`\|`) |
| JSON Lines / NDJSON | yes | yes | yes | Preferred for nested records |
| JSON (array/object) | yes | yes | limited | Prefer JSONL for large files |
| Parquet | yes | yes | batched | Requires `pyarrow` (default dependency) |
| Excel XLSX / XLS | yes | limited | sheet-based | Use `--start-page` for sheet index |
| XML | yes | limited | yes | Use `--tagname` for record elements |
| BSON | yes | yes | yes | |
| AVRO / ORC | yes | yes | batched | |
| YAML | yes | yes | limited | |

Compression codecs commonly used with the above: `gz`/`gzip`, `zst`, `bz2`, `xz`, `lz4`, `zip`, `7z`.

Use `undatum repack` to recompress container-coded files at maximum compression, or to rewrite
Parquet/ORC/AVRO with format-native compression (see README `repack` section).

## Important caveats

1. **Read-only formats exist.** Do not assume every format listed by `formats list` can be a
   conversion *target*. undatum fails fast with writable-format suggestions when you pick a
   read-only output.
2. **Extras.** Some connectors need optional installs (`undatum[cloud]`, `undatum[api]`,
   `undatum[extract]`, database drivers, etc.).
3. **Large files.** Prefer `--low-memory` on `convert` / `sort` / `dedup`, and Parquet or
   JSONL over in-memory JSON arrays.

For the authoritative list, always trust `undatum formats list --capabilities` for your
installed version.
