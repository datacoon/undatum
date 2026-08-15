## Context

CLI convert/stats already open `gs://` and `az://` through iterabledata's fsspec
cloud layer (`open_iterable_with_s3` native-cloud branch). S3 reads still use
boto3 temp-file download so `region`/`profile` keep working. The Data API, URI
helpers, and several commands still treat only `s3://` as remote storage.

## Goals / Non-Goals

- Goals:
  - First-class GCS and Azure URIs next to S3 for read and write
  - Same major commands as S3 (`convert`, `stats`, `ingest`, `mask`, Data API,
    TUI/web open-by-path, SDK)
  - Clear missing-extra errors (`pip install "undatum[gcs]"` / `[azure]` / `[cloud]`)
  - Standard provider credential chains; no undatum-specific secret files
- Non-Goals:
  - Kafka / streaming connectors (roadmap 4.3)
  - DuckDB native `gs://`/`az://` httpfs (temp-file materialization is enough
    for the Data API, matching S3)
  - Changing the boto3 S3 read path
  - Listing buckets or a cloud file browser

## Decisions

- Decision: Reuse iterabledata/fsspec for GCS and Azure I/O instead of a
  boto3-style download helper per provider.
  - Alternatives considered: Custom GCS/Azure temp-file clients. Rejected:
    duplicates fsspec, more extras to maintain.
- Decision: Data API still materializes cloud objects to a local temp file
  because DuckDB's file readers expect a filesystem path. S3 keeps boto3;
  GCS/Azure use fsspec `open` + copy.
- Decision: Add `gcs` and `azure` extras that pull `fsspec` plus `gcsfs` or
  `adlfs`. `cloud` stays the union (fsspec + s3fs + gcsfs + adlfs). `s3`
  stays boto3-only.
- Decision: `is_cloud_uri()` covers `s3`, `s3a`, `gs`, `gcs`, `az`, `abfs`,
  `abfss`. `is_s3_uri()` remains `s3` only so existing AWS-specific code is
  unchanged.

## Risks / Trade-offs

- Missing gcsfs/adlfs currently surfaces as a raw ImportError from fsspec →
  wrap as `DependencyError` with an install hint.
- Azure `abfs://container@account.dfs.core.windows.net/path` netloc is not a
  simple container name → pass the original URI to fsspec; only parse
  bucket/key when needed for suffixes/temp names.
- Data API downloads the whole object (same as S3 today) → document; large
  objects should use CLI streaming commands instead of `api serve`.

## Migration Plan

No migration. New schemes start working when extras are installed. Rollback is
reverting the change; S3 and local paths are unaffected.

## Open Questions

- None for this slice.
