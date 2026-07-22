# Format Support Matrix

undatum supports **140+ formats** via [iterabledata](https://github.com/datenoio/iterabledata/)
(catalog size depends on the installed iterabledata version and optional extras).
Capabilities differ by format: many are **read-only**, some require extras, and streaming
support varies.

Recommended engine: **iterabledata ≥ 1.0.18** on Python 3.10+ for the full open-data pack,
lakehouse writes, codec profiles, and Avro write support. Run `undatum formats list` to see
what your install actually exposes.

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
| Parquet | yes | yes | batched | Requires `pyarrow` (default dependency); codec profiles `fast`/`balanced`/`max` |
| Arrow / Feather | yes | yes | batched | Native batch convert path in iterabledata 1.0.17+ |
| Excel XLSX / XLS | yes | limited | sheet-based | Use `--start-page` for sheet index |
| XML | yes | limited | yes | Use `--tagname` for record elements |
| BSON | yes | yes | yes | |
| AVRO | yes | yes | batched | Writable since iterabledata 1.0.14 (lazy schema inference) |
| ORC | yes | yes | batched | Robust schema inference for unusual column names (1.0.14+) |
| YAML | yes | yes | limited | |
| DuckDB / SQLite files | yes | yes | table-based | Default table name derived from output filename when omitted (1.0.14+) |

Compression codecs commonly used with the above: `gz`/`gzip`, `zst`, `bz2`, `xz`, `lz4`, `zip`, `7z`.
Codec performance profiles (`fast`, `balanced`, `max`) are available through iterabledata;
`undatum repack` uses maximum compression by default.

Use `undatum repack` to recompress container-coded files at maximum compression, or to rewrite
Parquet/ORC/AVRO with format-native compression (see README `repack` section).

## Format families (non-exhaustive)

| Family | Examples | Notes |
|--------|----------|-------|
| Tabular text | `csv`, `tsv`, `jsonl`/`ndjson`, `annotatedcsv`, `csvw`, `fwf`, `ssv` | Core CLI path |
| Columnar / analytics | `parquet`, `orc`, `avro`, `arrow`, `geoparquet`, `zarr`, `vortex`, `ddb` | Many need optional deps |
| Lakehouse / warehouse | `delta`, `iceberg`, `lance`, `hudi` (RO), `ducklake`, `paimon`, `paimon_row`, `paimon_mosaic` | Delta/Iceberg writable in iterabledata 1.0.18+; install via `pip install "iterabledata[lakehouse]"` (and related extras) |
| Geospatial | `geojson`, `geojsonseq`, `gml`, `gpx`, `shp`, `gpkg`, `kml`, `fgdb`/`gdb`, `mif`, `asc`, `e00`, `las`, `bag`, `czml` | Open-data GIS pack largely experimental (1.0.18) |
| Scientific / geophysical | `h5`, `nc`, `mat`, `segy`, `grib2`, `mseed`, `cif`, `pdb`, `xyz`, `edi` | Often read-only; extras such as `mat`, `geophysical`, `lidar` |
| Business / legacy | `mdb`/`accdb`, `lotus123` (`123`/`wk1`), `xlsb` | Access needs `iterabledata[access]` |
| Containers | `zip`, `tar` (RO multi-member), WebDataset | TAR tags records with `_member` (1.0.16+) |
| Genomic / bio | `genomic_vcf`, `bam`, `sam`, `bed*`, `gff3`/`gtf`, `cram`, `fasta`/`fastq` | VCF vs vCard `.vcf` disambiguated by content |
| Graph / RDF | `graphml`, `gexf`, `jsonld`, `nt`, `ttl`, `trig`, `hdt` | HDT via open-data pack |
| Logs / feeds / OTLP | `log`, `gelf`, `cef`, `rss`, `kafka`, OTLP JSON/Protobuf | |
| Niche open data | `iati`, `fst`, `webdataset` | Experimental; optional extras |

## Important caveats

1. **Read-only formats exist.** Do not assume every format listed by `formats list` can be a
   conversion *target*. undatum fails fast with writable-format suggestions when you pick a
   read-only output. Hudi remains read-only pending Python SDK write support.
2. **Extras.** Some connectors need optional installs (`undatum[cloud]`, `undatum[api]`,
   `undatum[extract]`, database drivers, or iterabledata extras such as `lakehouse`,
   `geospatial`, `lidar`, `mat`, `geophysical`, `access`, `fst`, `paimon`, `ducklake`).
3. **Large files.** Prefer `--low-memory` on `convert` / `sort` / `dedup`, and Parquet or
   JSONL over in-memory JSON arrays. Parquet/Arrow writers buffer bounded batches
   (iterabledata 1.0.17+).
4. **Python version.** Recent iterabledata releases (1.0.11+) require **Python ≥ 3.10**.
   undatum still declares `requires-python >= 3.9`; use Python 3.10+ to pull the latest
   iterabledata catalog from PyPI.
5. **Security / error policy** (iterabledata 1.0.16+): XML parsers disable external entities;
   pickle reads warn unless `trust=True`; some formats raise `FormatParseError` on malformed
   input instead of returning an empty dataset.

For the authoritative list, always trust `undatum formats list --capabilities` for your
installed version. Upstream release notes:
[iterabledata CHANGELOG](https://github.com/datenoio/iterabledata/blob/main/CHANGELOG.md).
