---
title: "repack"
description: "undatum repack command reference"
---
# `repack`

Recompress a file at **maximum compression by default**, preserving the data format.

- Container codecs (`.gz`, `.zst`, `.bz2`, `.xz`, `.lz4`, …): stream-recompress with the same codec at max strength (or `--level`).
- Built-in formats (Parquet / ORC / AVRO): rewrite using native compression (Parquet defaults to `zstd`).
- Omitting OUTPUT rewrites the input atomically.

```bash
# In-place max recompress of a gzip file
undatum repack data.csv.gz

# Explicit output and faster level
undatum repack data.jsonl.zst out.jsonl.zst --level 3

# Parquet → zstd (built-in compression)
undatum repack data.parquet out.parquet

# Wrap an uncompressed file into a codec container
undatum repack data.csv data.csv.zst
```

**Key options:**
- `--level` / `-l` — compression level (overrides default maximum)
- `--compression` — override codec (container or format-native)
- `--progress` / `--no-progress` — progress bar (default: on)

**Supported conversions:**

`convert` uses iterabledata's engine, so any **readable** format can be converted to any **writable** one — there is no fixed pairwise matrix. The live catalog depends on installed optional dependencies; inspect it on your machine:

```bash
# All formats with read/write flags
undatum formats list

# Formats that can be used as conversion output
undatum formats list --writable

# Read-only inputs (e.g. ARFF, Hudi, GPX, HDF5, TAR)
undatum formats list --read-only

# Capability matrix (bulk, streaming, tables, nested, maturity, native bulk)
undatum formats list --capabilities

# List sheets/tables in a workbook or SQLite/lakehouse source
undatum formats tables workbook.xlsx
undatum formats tables data.sqlite --json

# Single-format details (aliases, optional extras, limitations)
undatum formats describe parquet

# Machine-readable catalog export
undatum formats export --output formats.json
```

**Common examples:**

| Use case | Example |
|----------|---------|
| Tabular text → columnar | `undatum convert data.csv data.parquet` |
| Columnar → tabular text | `undatum convert data.parquet data.csv` |
| JSON Lines ↔ CSV | `undatum convert data.jsonl data.csv` |
| Excel → JSON Lines | `undatum convert sheet.xlsx sheet.jsonl` |
| XML → JSON Lines | `undatum convert --tagname item feed.xml feed.jsonl` |
| Geospatial | `undatum convert points.geojson points.parquet` |
| GeoJSON Text Sequence | `undatum convert features.geojsonl features.parquet` |
| Bulk directory/glob | `undatum convert ./raw ./out --recursive --to-ext parquet` |

**Format families** (non-exhaustive; run `formats list` for the full set):

| Family | Examples |
|--------|----------|
| Tabular text | `csv` (alias: `tsv`), `jsonl` (alias: `ndjson`), `annotatedcsv`, `csvw`, `fwf`, `ssv` |
| Columnar / analytics | `parquet`, `orc`, `avro`, `arrow`, `geoparquet`, `zarr`, `ddb` |
| Lakehouse | `delta`, `iceberg`, `lance`, `ducklake`, `paimon` (Hudi remains read-only) |
| Documents / config | `json`, `yml` (alias: `yaml`), `xml`, `toml` |
| Geospatial | `geojson`, `geojsonseq`, `gml`, `gpx`, `shp`, `gpkg`, `kml`, `fgdb`, `mif`, `las` |
| Scientific / statistical | `h5`, `nc`, `mat`, `segy`, `grib2`, `sas`, `sav`, `dta` (many are read-only) |
| Containers | `zip`, `tar` (read-only multi-member), WebDataset |
| Logs / feeds | `log`, `gelf`, `cef`, `rss`, `kafka` |
| Graph / RDF | `graphml`, `gexf`, `jsonld`, `nt`, `ttl`, `trig`, `hdt` |

See the [format support matrix](/formats/) for extras (`undatum[lakehouse]`, `undatum[gis]`, `undatum[scientific]`) and version notes.

**Limitations:**

- **Read-only formats** can be inputs but not outputs — check with `formats list --writable`.
- **Schema-required outputs** (`protobuf`, `capnp`, `thrift`) need an externally supplied schema or message class and cannot be used as generic conversion targets.
- Override detection when the file extension is ambiguous: `--format-in` / `--format-out` (see `undatum convert --help`).
- Lakehouse and many open-data formats need the matching **iterabledata** optional extra and Python 3.10+.
