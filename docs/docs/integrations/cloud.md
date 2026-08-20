---
title: "Cloud storage"
description: "Read and write s3://, gs://, and az:// URIs"
---
# Cloud storage

undatum reads and writes cloud object storage URIs natively through iterabledata. Install the appropriate extra before using cloud paths in commands like `convert`, `stats`, `count`, `mask`, and the Python SDK.

```bash
# AWS S3 only
pip install "undatum[s3]"

# Google Cloud Storage only
pip install "undatum[gcs]"

# Azure Blob / ADLS only
pip install "undatum[azure]"

# S3 + Google Cloud Storage + Azure Blob (recommended for multi-cloud)
pip install "undatum[cloud]"
```

### Supported URI schemes

| Provider | URI examples | Credential setup |
|----------|--------------|------------------|
| **AWS S3** | `s3://bucket/path`, `s3a://bucket/path` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `AWS_REGION`, or `~/.aws/credentials` |
| **Google Cloud Storage** | `gs://bucket/path`, `gcs://bucket/path` | Application Default Credentials, `GOOGLE_APPLICATION_CREDENTIALS`, or gcloud user credentials |
| **Azure Blob / ADLS** | `az://container/path`, `abfs://container/path`, `abfss://container/path` | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, or Azure identity chain via `adlfs` |

### Usage examples

```bash
# Read from cloud storage
undatum stats gs://my-bucket/data.csv
undatum count s3://my-bucket/data.jsonl

# Write to cloud storage
undatum convert local.csv s3://my-bucket/output.parquet
undatum convert data.csv gs://my-bucket/output.parquet

# Cloud-to-cloud conversion
undatum convert s3://bucket/input.jsonl gs://bucket/output.parquet
undatum mask s3://bucket/data.csv --fields email --method hash --output az://container/masked.csv
```

**Supported commands:** Any command that accepts file paths — including `convert`, `stats`, `count`, `analyze`, `select`, `validate`, `ingest`, `mask`, and SDK `Dataset.read()` / `write()`. [`repack`](/commands/repack) is **local files only**; download first or use `convert` for cloud URIs.

**Notes:**
- Cloud I/O is streaming-aware; large files do not need to be downloaded manually first.
- Local-only options such as `--atomic` apply to local output paths only.
- For S3-only workflows, `undatum[s3]` is sufficient; use `undatum[gcs]` or `undatum[azure]` for a single other cloud, or `undatum[cloud]` for all three.
