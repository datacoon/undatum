## 1. S3 Connector Implementation
- [x] 1.1 Create `undatum/formats/s3.py` module
  - Implement S3 reader (download and stream)
  - Implement S3 writer (upload with streaming)
  - Add credential handling (env vars, profiles)
- [x] 1.2 Add S3 URI parsing
  - Parse `s3://bucket/path` format
  - Extract bucket and key from URI
  - Validate URI format

## 2. Path Utilities
- [x] 2.1 Update `undatum/common/path_utils.py`
  - Add URI detection (s3://, file://, etc.)
  - Add path normalization for S3 URIs
  - Add path resolution logic

## 3. Command Integration
- [x] 3.1 Update `convert` command
  - Support S3 input paths
  - Support S3 output paths
- [x] 3.2 Update `ingest` command
  - Support S3 input paths
- [x] 3.3 Update `stats` command
  - Support S3 input paths
- [x] 3.4 Update `count` command
  - Support S3 input paths
- [ ] 3.5 Update other major commands
  - Add S3 support to commands that accept file paths

## 4. CLI Integration
- [ ] 4.1 Add AWS credential documentation
  - Document environment variables
  - Document profile usage
  - Add examples
- [ ] 4.2 Update help text
  - Document S3 URI format
  - Add S3 examples

## 5. Testing
- [ ] 5.1 Unit tests for S3 connector
  - Test URI parsing
  - Test credential handling
  - Mock S3 operations
- [ ] 5.2 Integration tests
  - Test S3 read operations
  - Test S3 write operations
  - Test with various file formats
- [ ] 5.3 Error handling tests
  - Test invalid credentials
  - Test missing buckets
  - Test network errors

## 6. Documentation
- [ ] 6.1 Update README with S3 examples
- [ ] 6.2 Document AWS credential setup
- [ ] 6.3 Add S3 usage guide
