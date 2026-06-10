TH```markdown
# undatum – Feature Improvement Plan

Below is a clear, actionable feature roadmap for improving **undatum** as a data processing tool. You can use this file directly as `undatum-improvements.md` in your repo.

---

## 1. Strengthen the Core: Performance & Big Data Handling

### 1.1. Push More Operations into DuckDB

**Goal:** Make operations on large CSV/JSONL/Parquet files fast and scalable.

**Actions:**

- Route more commands through DuckDB when possible:
  - Candidates: `sort`, `stats`, `frequency`, `uniq`, `sample`, `search`, `dedup`, `slice`, `join`.
  - Conditions:
    - Input format supported by DuckDB (CSV, JSON, Parquet, etc.).
    - Operation expressible as a SQL query.
- Add an engine selector:
  - `--engine auto` (default): automatically pick DuckDB or Python based on input size, format, and available dependencies.
  - `--engine duckdb`: force DuckDB usage.
  - `--engine python`: force pure‑Python pipeline.
- Expose DuckDB tuning options:
  - `--duckdb-threads N`
  - `--duckdb-memory <bytes|MB|GB>`
  - `--duckdb-temp-dir /path/to/tmp`

**Outcome:** undatum becomes an “out‑of‑the‑box big‑data‑ready” tool.

### 1.2. Parallel & Chunked Processing

**Goal:** Efficiently process multi‑GB files even when DuckDB is not used.

**Actions:**

- Implement chunked streaming I/O:
  - Read N lines/records at a time.
  - Process and write output incrementally.
  - Keep memory usage roughly constant.
- Add `--threads N` for CPU‑bound operations:
  - Apply to `convert`, `stats`, `frequency`, `dedup`, `search`, etc.
  - Use multiprocessing or multithreading depending on workload.
- Add a `--progress` flag:
  - Textual or progress‑bar style indicator for long‑running commands.
  - Show approximate percentage, estimated time, and throughput.

**Outcome:** undatum reliably handles large files on modest hardware.

---

## 2. Pipelines Instead of Single Commands Only

### 2.1. YAML/JSON Pipelines

**Goal:** Let users define repeatable workflows instead of shell scripts.

**Actions:**

- Introduce a `pipeline` command with sub‑commands like:
  - `undatum pipeline run pipeline.yml`
  - `undatum pipeline validate pipeline.yml`
- Define a simple pipeline spec, e.g.:

  ```yaml
  steps:
    - name: load_data
      command: convert
      args:
        input: s3://bucket/raw.ndjson
        input-format: jsonl
        output: /tmp/data.parquet
        output-format: parquet

    - name: clean
      command: fill
      args:
        column: age
        value: 0

    - name: deduplicate
      command: dedup
      args:
        key: user_id

    - name: stats
      command: stats
      args:
        output: /tmp/data_stats.json
  ```

- Features:
  - Validate that commands, arguments, and paths are valid before running.
  - Support variables and overrides (e.g., environment variables, CLI `--var key=value`).

**Outcome:** Users can encode ETL/data‑cleaning workflows declaratively and re‑run them reliably.

### 2.2. Reusable “Tasks” and Templates

**Actions:**

- Provide named, reusable tasks (e.g., `profile_dataset`, `generate_docs`) that compile to sets of pipeline steps.
- Ship templates:
  - `undatum pipeline templates list`
  - `undatum pipeline templates init basic-cleaning`
- Include templates for:
  - Basic CSV cleaning.
  - JSONL normalization.
  - Dataset profiling and documentation.

**Outcome:** Faster onboarding and standardized workflows across teams.

---

## 3. Data Quality, Validation, and Governance

### 3.1. Rich Validation Rules

**Goal:** Turn `validate` and `schema` into a light but practical data‑quality framework.

**Actions:**

- Support rule definitions via YAML/JSON, for example:

  ```yaml
  fields:
    user_id:
      required: true
      type: integer
      unique: true
    age:
      type: integer
      min: 0
      max: 120
    email:
      pattern: "^[^@]+@[^@]+$"
  ```

- CLI:

  ```bash
  undatum validate --rules rules.yml input.jsonl
  ```

- Behavior:
  - Hard errors: stop processing, non‑zero exit code.
  - Soft warnings: log issues but continue.
  - Summary of violations by rule and field.

**Outcome:** undatum can enforce schema and quality in CI/CD or pipelines.

### 3.2. Data Profiling & Quality Reports

**Goal:** Provide instant “health checks” of datasets.

**Actions:**

- Extend `stats` (or add `profile`) with:
  - Missing value rate per column.
  - Distinct count and cardinality.
  - Basic type inference (categorical vs numerical).
  - Simple distribution info (mean, median, percentiles for numerics).
- Output:
  - JSON and human‑readable text.
  - Optional HTML or Markdown reports for sharing.

**Outcome:** One command yields a shareable overview of dataset quality.

---

## 4. Data Security: Masking and Anonymization

### 4.1. `mask` Command

**Goal:** Make PII protection a first‑class feature.

**Actions:**

- Implement:

  ```bash
  undatum mask --fields email,phone --method hash input.jsonl > masked.jsonl
  undatum mask --fields ssn --method redact input.csv > masked.csv
  ```

- Supported methods:
  - `redact`: replace with a fixed token (e.g., `***`).
  - `hash`: deterministic one‑way hash, so joins remain possible but identities are hidden.
  - `randomize`: replace with random but type‑compatible values (e.g., age range, fake emails).

**Outcome:** undatum becomes safe to use with sensitive data in dev, demos, and data‑sharing scenarios.

### 4.2. Synthetic Data Generation

**Goal:** Provide realistic but non‑sensitive datasets.

**Actions:**

- Add a `synth` or `generate` command that:
  - Reads a real dataset.
  - Learns schema and approximate distributions.
  - Outputs synthetic data with similar characteristics.

**Outcome:** Easier testing, demos, and sharing without leaking real user data.

---

## 5. Cloud, Database, and Streaming Integration

### 5.1. Cloud Storage Connectors

**Goal:** Make undatum work directly with remote data.

**Actions:**

- Add support for URIs:
  - `s3://bucket/path`
  - `gs://bucket/path`
  - `az://container/path`
  - `https://...`
- Respect standard environment variables for credentials.
- Ensure major commands understand remote paths:
  - `convert`, `ingest`, `export`, `count`, `stats`, etc.

**Outcome:** Replace manual download/upload steps; integrate better into real data stacks.

### 5.2. Database Support

**Goal:** Treat databases as first‑class sources and sinks.

**Actions:**

- `db query`:

  ```bash
  undatum db query "SELECT * FROM users LIMIT 100" \
    --db postgresql://user:pass@host/db \
    --output sample.jsonl
  ```

- `db load`:

  ```bash
  undatum db load --table cleaned_users cleaned_users.parquet
  ```

- Start with PostgreSQL, MySQL/MariaDB, and SQLite.

**Outcome:** undatum can sit cleanly between files, warehouses, and applications.

### 5.3. Basic Streaming/Queue Integration

**Goal:** Support simple batch‑style operations on streaming systems.

**Actions:**

- Add minimal Kafka integration, e.g.:

  ```bash
  undatum convert --input kafka://topic/raw --output out.jsonl
  ```

- Start with batch semantics (“consume last N messages”) before full streaming.

**Outcome:** Entry‑level streaming support for log processing and event sampling.

---

## 6. Developer Experience: SDK and Plugin System

### 6.1. Python SDK

**Goal:** Enable scripting and notebook integration.

**Actions:**

- Provide a `Dataset` (or similar) API:

  ```python
  from undatum import Dataset

  ds = Dataset.read("data.jsonl")
  ds = ds.fill("age", value=0).dedup(keys=["user_id"])
  ds.stats().to_json("stats.json")
  ds.write("output.parquet")
  ```

- Map core CLI commands to methods on this object.
- Ensure consistent behavior between CLI and SDK.

**Outcome:** Power users can integrate undatum into Python workflows without shelling out.

### 6.2. Plugin Architecture

**Goal:** Let others extend undatum without modifying core.

**Actions:**

- Use entry points (`undatum.plugins`) to discover plugins.
- Provide a simple API:

  ```python
  def register(undatum):
      @undatum.command("my-transform")
      def my_transform(records, **kwargs):
          ...
  ```

- Support:
  - New commands.
  - New IO connectors.
  - Custom transforms.

**Outcome:** Ecosystem growth and domain‑specific extensions (finance, health, logs, etc.).

---

## 7. Usability and UX Enhancements

### 7.1. Better CLI Ergonomics

**Actions:**

- Shell completions:

  ```bash
  undatum completion install
  ```

- Improve `--help`:
  - Clear structure per sub‑command.
  - Realistic examples for each.
- Colorized, actionable error messages:
  - For example: “Column `age` appears as string; use `--cast age:int` to convert.”

**Outcome:** Lower learning curve and faster troubleshooting.

### 7.2. Interactive TUI / Simple GUI (Longer Term)

**Actions:**

- Start with a TUI:

  ```bash
  undatum tui
  ```

  Features:
  - Preview data (`head`).
  - Inspect schema and types.
  - Run `stats` or `search` interactively.

- Later, build a simple web UI:
  - Upload or reference local/cloud data.
  - Explore fields and distributions.
  - Build pipelines visually and export them as YAML.

**Outcome:** Undatum becomes usable by less technical users and for quick exploratory analysis.

---

## 8. Data Exploration and Visualization

### 8.1. `plot` Command

**Goal:** Provide quick, scriptable visualizations.

**Actions:**

- Implement:

  ```bash
  # Histogram of ages
  undatum plot --type hist --field age data.csv --output age_hist.png

  # Bar plot of frequency for a categorical field
  undatum frequency --field country data.csv | undatum plot --type bar --x country --y count
  ```

- Backends:
  - `matplotlib` for static PNG/SVG.
  - Optionally `plotly`/`bokeh` for interactive HTML.

**Outcome:** One‑stop data inspection and quick visual checks.

---

## 9. Documentation and “Smart Help”

### 9.1. Command Recipes

**Goal:** Make common tasks discoverable and copy‑paste ready.

**Actions:**

- Provide:

  ```bash
  undatum examples list
  undatum examples run clean-csv
  ```

- Example recipes:
  - “Clean broken CSV with inconsistent columns.”
  - “Join user logins with profile data and compute retention.”
  - “Generate documentation and profile for open data release.”

**Outcome:** Users learn by example instead of reading long manuals.

### 9.2. Autodoc for Pipelines

**Goal:** Explain and document pipelines automatically.

**Actions:**

- Given `pipeline.yml`, generate docs:

  ```bash
  undatum pipeline doc pipeline.yml > pipeline.md
  ```

- Content:
  - Step‑by‑step explanation (inputs, outputs, transforms).
  - Simple diagram (e.g., Mermaid) of flow.

**Outcome:** Pipelines become self‑documenting and easier to review.

---

## 10. Suggested Roadmap

### Phase 1 (Short Term)

Focus on immediate impact and low complexity:

- Push more operations through DuckDB.
- Implement `--engine auto` and `--threads`.
- Add basic S3 read/write support.
- Introduce a minimal `mask` command.
- Publish a basic Python SDK with read/transform/write.

### Phase 2 (Medium Term)

Stabilize workflows and quality features:

- Implement YAML/JSON pipelines (`pipeline run`, `pipeline validate`).
- Enhance `stats` with profiling; add a `profile` alias.
- Add `db query` and `db load`.
- Introduce `plot` for simple charts.
- Add command recipes and `examples` commands.

### Phase 3 (Long Term)

Scale ecosystem and UX:

- Full plugin system and more connectors (GCS, Azure, Kafka).
- Interactive TUI and optional web UI.
- Synthetic data generation and more advanced quality/drift monitoring.

---

By following this plan, undatum evolves from a collection of useful commands into a robust, extensible data platform: capable on big data, pipeline‑friendly, safe for sensitive data, and approachable for both CLI experts and less technical users.
```