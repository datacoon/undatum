# Change: Add GCS and Azure Cloud Storage Connectors

## Why

S3 is a first-class path type (`s3://` helpers, Data API materialization, boto3
credentials). Google Cloud Storage and Azure Blob/ADLS already reach iterabledata
via fsspec for most CLI I/O, but they are not specified, not parsed as URIs, not
accepted by the Data API, and fail with opaque import errors when extras are
missing. Roadmap item 4.2 (`add-undatum-improvement-roadmap`) called for
`gs://` and `az://` as peer connectors to S3.

## What Changes

- **ADDED**: GCS URI support (`gs://`, `gcs://`) for command, SDK, TUI, web, and
  Data API paths
- **ADDED**: Azure Blob / ADLS URI support (`az://`, `abfs://`, `abfss://`)
- **ADDED**: Standard credential chains (ADC / `GOOGLE_APPLICATION_CREDENTIALS`;
  `AZURE_STORAGE_ACCOUNT` / `AZURE_STORAGE_KEY` / Azure identity)
- **ADDED**: `undatum[gcs]` and `undatum[azure]` extras; `undatum[cloud]` remains
  the multi-cloud umbrella
- **ADDED**: `DependencyError` install hints when gcsfs/adlfs/fsspec are missing
- **MODIFIED**: Data API resource paths accept GCS and Azure URIs (download to a
  temp file for DuckDB, same as S3)
- **MODIFIED**: Commands that special-case `s3://` treat GCS/Azure the same way

S3 behavior is unchanged. Local paths stay the default. Kafka and other streaming
connectors remain out of scope (roadmap 4.3).

## Impact

- Affected specs: `cloud-connectors`, `data-api`, `tui`, `web-ui`
- Affected code: `undatum/common/path_utils.py`, `undatum/common/s3_iterable.py`,
  `undatum/cmds/api.py`, `undatum/cmds/masker.py`, `undatum/cmds/validator.py`,
  `undatum/cli/formats_cli.py`, `pyproject.toml`
- Dependencies: optional `gcsfs` / `adlfs` (already listed under `cloud`)
- Backward compatibility: fully compatible; new URI schemes only
