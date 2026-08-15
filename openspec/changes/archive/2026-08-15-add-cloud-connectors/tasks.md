## 1. URI helpers
- [x] 1.1 Add `is_gcs_uri`, `is_azure_uri`, `is_cloud_uri`, and `parse_cloud_uri` in `path_utils.py`
- [x] 1.2 Point `s3_iterable` native-cloud detection at those helpers
- [x] 1.3 Raise `DependencyError` with `undatum[gcs]` / `[azure]` / `[cloud]` install hints when extras are missing

## 2. Command and API integration
- [x] 2.1 Materialize GCS/Azure Data API resource paths to temp files (fsspec)
- [x] 2.2 Accept cloud URIs in API config validation and `api discover` format detection
- [x] 2.3 Use shared cloud open path in `mask` and `validate` (not S3-only branches)
- [x] 2.4 Treat cloud URIs as remote in `formats tables` existence checks

## 3. Packaging
- [x] 3.1 Add optional extras `gcs` (`fsspec`, `gcsfs`) and `azure` (`fsspec`, `adlfs`)
- [x] 3.2 Keep `cloud` as the multi-cloud umbrella extra

## 4. Tests
- [x] 4.1 Unit tests for GCS/Azure URI detection and parsing
- [x] 4.2 Tests that `open_iterable_with_s3` / `open_path` delegate `az://` and `gcs://`
- [x] 4.3 Tests that Data API materializes `gs://` / `az://` via fsspec
- [x] 4.4 Tests that missing extras raise `DependencyError`

## 5. Documentation
- [x] 5.1 Document `gcs` / `azure` extras and Data API cloud paths
- [x] 5.2 Update SDK docstrings for GCS/Azure URIs
- [x] 5.3 CHANGELOG Unreleased entry
- [x] 5.4 Mark roadmap task 4.2 complete
