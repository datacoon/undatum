## 1. Roadmap Documentation
- [x] 1.1 Create proposal.md with roadmap overview and reference to implementation document
- [x] 1.2 Add spec delta for roadmap-planning capability
- [x] 1.3 Validate proposal structure

## 2. Phase 1: Foundation Change Proposals
Create OpenSpec change proposals for immediate impact items:

- [x] 2.1 Create `improve-duckdb-operations` proposal
  - Push more operations through DuckDB (sort, frequency, uniq, sample, search, dedup, slice, join)
  - Add `--engine auto|duckdb|python` selector
  - Expose DuckDB tuning options (--duckdb-threads, --duckdb-memory, --duckdb-temp-dir)

- [x] 2.2 Create `add-parallel-processing` proposal
  - Implement chunked streaming I/O
  - Add `--threads N` for CPU-bound operations
  - Add `--progress` flag with progress bar/percentage

- [x] 2.3 Create `add-s3-connector` proposal
  - Add S3 URI support (s3://bucket/path)
  - Respect AWS credentials from environment
  - Support read/write operations for major commands

- [x] 2.4 Create `add-mask-command` proposal
  - Implement `undatum mask` command
  - Support methods: redact, hash, randomize
  - Handle PII protection use cases

- [x] 2.5 Create `add-python-sdk` proposal
  - Design Dataset API (read, transform, write)
  - Map CLI commands to SDK methods
  - Ensure consistent behavior between CLI and SDK

## 3. Phase 2: Workflow & Quality Change Proposals
Create OpenSpec change proposals for workflow and quality features:

- [x] 3.1 Create `add-pipeline-command` proposal
  - Implement `pipeline run` and `pipeline validate` subcommands
  - Define YAML/JSON pipeline spec format
  - Support variables and overrides

- [x] 3.2 Create `add-pipeline-templates` proposal
  - Implement reusable tasks (profile_dataset, generate_docs)
  - Add `pipeline templates list` and `pipeline templates init` commands
  - Ship templates for common workflows

- [x] 3.3 Create `enhance-stats-profiling` proposal
  - Extend stats with missing value rates, distinct counts, cardinality
  - Add type inference (categorical vs numerical)
  - Add distribution info (mean, median, percentiles)
  - Add `profile` alias command

- [x] 3.4 Create `add-rich-validation-rules` proposal
  - Support YAML/JSON rule definitions
  - Implement hard errors and soft warnings
  - Add violation summary reporting

- [x] 3.5 Create `add-db-query-load` proposal
  - Implement `db query` command
  - Implement `db load` command
  - Support PostgreSQL, MySQL/MariaDB, SQLite

- [x] 3.6 Create `add-plot-command` proposal
  - Implement `plot` command for visualizations
  - Support histogram, bar plot types
  - Add matplotlib backend (optional plotly/bokeh)

- [x] 3.7 Create `add-examples-command` proposal
  - Implement `examples list` and `examples run` commands
  - Create recipe library for common tasks
  - Make recipes copy-paste ready

## 4. Phase 3: Ecosystem & UX Change Proposals
Create OpenSpec change proposals for ecosystem expansion:

- [x] 4.1 Create `add-plugin-system` proposal
  - Design entry point system (undatum.plugins)
  - Define plugin API for commands, IO connectors, transforms
  - Support plugin discovery and registration

- [ ] 4.2 Create `add-cloud-connectors` proposal
  - Add GCS connector (gs://bucket/path)
  - Add Azure connector (az://container/path)
  - Enhance existing S3 support

- [ ] 4.3 Create `add-streaming-connectors` proposal
  - Add Kafka integration (kafka://topic/raw)
  - Implement batch semantics for streaming
  - Support log processing and event sampling

- [ ] 4.4 Create `add-synthetic-data` proposal
  - Implement `synth` or `generate` command
  - Learn schema and distributions from real data
  - Generate realistic synthetic datasets

- [ ] 4.5 Create `add-tui-interface` proposal
  - Design interactive TUI (undatum tui)
  - Implement data preview, schema inspection
  - Add interactive stats/search capabilities

- [ ] 4.6 Create `add-web-ui` proposal (longer term)
  - Design simple web UI architecture
  - Implement data upload/exploration
  - Add visual pipeline builder

- [ ] 4.7 Create `add-advanced-quality-monitoring` proposal
  - Implement data drift detection
  - Add quality trend tracking
  - Support quality dashboards

- [ ] 4.8 Create `add-pipeline-autodoc` proposal
  - Implement `pipeline doc` command
  - Generate step-by-step explanations
  - Create Mermaid diagrams for pipeline flow

- [ ] 4.9 Create `improve-cli-ergonomics` proposal
  - Add shell completions (undatum completion install)
  - Enhance --help with examples
  - Improve error messages with actionable suggestions

## 5. Coordination & Tracking
- [ ] 5.1 Track progress of Phase 1 proposals
- [ ] 5.2 Coordinate dependencies between proposals
- [ ] 5.3 Update roadmap document as proposals are completed
- [ ] 5.4 Review and prioritize Phase 2/3 proposals based on Phase 1 learnings
