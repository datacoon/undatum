---
title: "repack"
description: "undatum repack command reference"
---
# `repack`

Recompress a **local** file at **maximum compression by default**, without changing the data format. Use [`convert`](/commands/convert) when you need a different format or a cloud URI.

Two paths:

- **Container codecs** (`.gz`, `.zst`, `.bz2`, `.xz`, `.lz4`, …): stream-decompress and recompress. Same codec unless the output extension or `--compression` says otherwise.
- **Built-in compression** (Parquet / ORC / AVRO): rewrite the file with format-native compression. Parquet and ORC default to `zstd`; AVRO defaults to `zstandard`.

Omitting OUTPUT writes to a sibling temp file and atomically replaces the input on success.

```bash
# In-place max recompress of a gzip file
undatum repack data.csv.gz

# Explicit output and a faster level
undatum repack data.jsonl.zst out.jsonl.zst --level 3

# Parquet → zstd (format-native compression)
undatum repack data.parquet out.parquet

# Wrap an uncompressed file into a codec container
undatum repack data.csv data.csv.zst

# Change container codec (gzip → zstd)
undatum repack data.csv.gz data.csv.zst
```

**Key options:**
- `--level` / `-l` — compression level (overrides the default `max` profile). Applies to container codecs and Parquet; ORC and AVRO ignore `--level`.
- `--compression` — override codec. For containers: `gz`, `zst`, `bz2`, `xz`, `lz4`, … For Parquet/ORC/AVRO: a format-native codec (for example `zstd`, `snappy`).
- `--progress` / `--no-progress` — progress bar (default: on)
- `--verbose` — verbose logging

**Limitations:**

- **Local files only.** Remote URIs (`s3://`, `gs://`, `az://`) are rejected. Download first, or use [`convert`](/commands/convert).
- **Uncompressed input needs a codec.** `undatum repack data.csv` fails. Point OUTPUT at a compressed path (`data.csv.zst`) or pass `--compression`.
- **Format is preserved.** `repack` does not convert CSV to Parquet. Use [`convert`](/commands/convert) for that, then `repack` if you want max native compression.
- **ZIP / 7z** are available as codecs but are not the primary path. Prefer single-stream containers (`gz`, `zst`, `bz2`, `xz`, `lz4`).
- Parquet repack requires `pyarrow`.

See the [format support matrix](/formats/) for the codec list and extras (`undatum[compression]`).
