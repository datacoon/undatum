# Change: Add S3 Cloud Storage Connector

## Why

Undatum currently only supports local file paths, requiring users to manually download/upload files
from cloud storage before processing. This adds friction and extra steps to data workflows. Adding
direct S3 support would enable seamless integration with cloud data stacks.

**Current Issues:**
1. **No cloud support**: Users must download files before processing
2. **Manual steps**: Extra download/upload operations required
3. **Workflow friction**: Breaks integration with cloud-based data pipelines

**Expected Benefits:**
- **Seamless cloud integration** with AWS S3
- **Reduced workflow steps** by eliminating manual downloads
- **Better integration** with cloud-based data stacks
- **Foundation** for additional cloud connectors (GCS, Azure)

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 5.1)

## What Changes

- **ADDED**: S3 URI support (`s3://bucket/path`) for input and output paths
- **ADDED**: AWS credentials handling via standard environment variables
  - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
  - `AWS_PROFILE` for named profiles
  - `AWS_REGION` for region specification
- **ADDED**: S3 connector implementation for read/write operations
- **MODIFIED**: Command path parsing to recognize S3 URIs
- **MODIFIED**: Major commands to support S3 paths:
  - `convert`, `ingest`, `export`, `count`, `stats`, etc.

All changes maintain backward compatibility. Local file paths continue to work as before.

## Impact

- **Affected specs**: `cloud-connectors` capability
- **Affected code**:
  - New `undatum/formats/s3.py` module for S3 connector
  - `undatum/common/path_utils.py` - Add URI parsing
  - `undatum/cmds/converter.py` - Support S3 input/output
  - `undatum/cmds/ingester.py` - Support S3 input
  - `undatum/cmds/statistics.py` - Support S3 input
  - All command processors - Support S3 URIs in path arguments
- **Dependencies**: `boto3` (AWS SDK for Python) - new optional dependency
- **Backward compatibility**: Fully backward compatible - local paths continue to work
