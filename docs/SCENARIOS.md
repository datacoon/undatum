# Usage scenarios: choose your goal

undatum covers many workflows. This page is a task-oriented index: find the row
that sounds like you, then follow the linked reference sections. If you are
completely new, do the [five-minute quickstart](QUICKSTART.md) first.

| You are a… | You want to… | Start with |
|------------|--------------|------------|
| [Data analyst](#data-analyst) | Inspect unfamiliar files and answer questions without writing a program | `table`, `tui`, `web`, `profile`, `frequency`, `sql`, `plot` |
| [Data engineer](#data-engineer) | Build repeatable, streaming transformations across formats, databases, and object storage | `convert`, `dedup`, `db dump` / `db load`, `pipeline` |
| [Data steward](#data-steward) | Assess quality, encode reusable rules, and produce evidence before data is released | `analyze`, `validate`, `diff`, `schema` |
| [Open-data publisher](#open-data-publisher) | Publish documented, portable, standards-friendly datasets | `package`, `doc`, `mask`, `validate` |
| [Application developer](#application-developer) | Embed data preparation in Python or expose a dataset through a read-only API | Python `Dataset` SDK, `api` |
| [Researcher / journalist](#researcher--journalist) | Turn awkward public files and documents into analysis-ready, shareable data | `extract`, `sniff`, `convert`, `doc` |
| [Operations / security analyst](#operations--security-analyst) | Search large event exports, reduce sensitive data, and create focused incident extracts | `search`, `select`, `sample`, `mask` |
| [AI / automation builder](#ai--automation-builder) | Give agents controlled dataset tools or add AI assistance to documentation | `mcp`, `ai doc`, LangChain tools |
| [Plugin author](#plugin-author) | Add domain-specific commands, connectors, or transforms without a fork | `plugins`, entry points |

All commands below are also available via the shorter `data` alias
(`data convert …` is identical to `undatum convert …`).

---

## Data analyst

*Inspect unfamiliar files and answer questions without building a Python project.*

### Understand a newly received dataset

Preview rows, infer structure, measure missingness, and inspect distributions.

```bash
undatum table sales.csv --limit 20
undatum tui sales.csv          # optional: pip install "undatum[tui]"
undatum web sales.csv          # optional: pip install "undatum[web]"
undatum profile sales.csv
undatum frequency sales.csv --fields region,status
```

Reference: [`table`](../README.md#table) · [`tui`](../README.md#tui) · [`web`](../README.md#web) · [`stats` / `profile`](../README.md#stats--profile) · [`frequency`](../README.md#frequency)

### Run ad-hoc SQL across files

Query one or more files with DuckDB SQL and save a reusable result. A single
input file is available as the view `data`; multiple inputs are named after
their file stems.

```bash
undatum sql "SELECT region, SUM(amount) AS total FROM data GROUP BY 1" sales.parquet \
  --output totals.csv --format csv

# Join two files
undatum sql "SELECT * FROM orders JOIN users USING (user_id)" orders.csv users.parquet
```

Reference: [`sql`](../README.md#sql) · [`plot`](../README.md#plot)

---

## Data engineer

*Build repeatable, streaming transformations across formats, databases, and object storage.*

### Normalize a raw delivery for analytics

Convert compressed JSONL to Parquet, deduplicate records, and profile the output.

```bash
undatum convert raw.jsonl.zst stage.parquet --low-memory
undatum dedup stage.parquet --key-fields id --output clean.parquet
undatum profile clean.parquet
```

Reference: [`convert`](../README.md#convert) · [`dedup`](../README.md#dedup) · [Large files](LARGE_FILES.md)

### Move data between a database and files

Export a table or query to a file, or load a prepared file into a table.

```bash
undatum db dump --db postgresql://user:pass@host/db --query "SELECT * FROM events" \
  --output events.parquet --to parquet

undatum db load clean.parquet --db postgresql://user:pass@host/db \
  --table events --mode upsert --upsert-key id
```

Reference: [`db query` / `db load`](../README.md#db-query--db-load) · [`db dump`](../README.md#db-dump) · [Cloud storage](../README.md#cloud-storage-support) · [Pipelines](../README.md#pipeline-workflows)

---

## Data steward

*Assess quality, encode reusable rules, and produce evidence before data is released.*

### Gate a dataset release

Validate field and cross-field rules, emit a machine-readable report, and fail
CI on violations.

```bash
undatum validate data.csv --rules rules.yml --output-format json \
  --violation-report violations.json --fail-on-warnings
```

Example rule files live in [`examples/validation-rules/`](../examples/validation-rules/).

Reference: [`validate`](../README.md#validate) · [`analyze`](../README.md#analyze)

### Detect unintended changes between versions

Compare releases by key and enforce thresholds in CI.

```bash
undatum diff previous.parquet current.parquet --key id --ignore-order \
  --max-changed-rows 0 --summary-only
```

Reference: [`diff`](../README.md#diff) · [`schema`](../README.md#schema)

---

## Open-data publisher

*Publish documented, portable, standards-friendly datasets with clear metadata.*

### Create a publishable data package

Infer schema, add metadata, materialize resources, and validate the descriptor.

```bash
undatum package create data.csv --package-dir release --output release/datapackage.json
undatum package validate release/datapackage.json
```

Reference: [`package`](../README.md#package)

### Prepare a safe public extract

Mask sensitive fields deterministically, then generate human-readable
documentation with PII detection.

```bash
undatum mask source.csv --fields email,phone --method hash --salt "$SALT" --output public.csv
undatum doc public.csv --pii-detect --pii-mask-samples --output DATASET.md
```

Reference: [`mask`](../README.md#mask) · [`doc`](../README.md#doc)

---

## Application developer

*Embed data preparation in Python or expose a local dataset through a read-only API.*

### Add a fluent preprocessing step

Read, clean, and write data from application code without shell orchestration.

```python
from undatum.sdk import Dataset

Dataset.read("users.csv").fill("age", value=0).dedup(keys=["id"]).write("users.parquet")
```

Reference: [Python SDK](../README.md#python-sdk)

### Prototype a read-only data service

Expose CSV, JSONL, or Parquet with filtering, sorting, pagination, and OpenAPI docs.

```bash
pip install "undatum[api]"
undatum api run users.parquet
curl "http://127.0.0.1:8000/users?status=active&limit=10"
```

Reference: [`api`](../README.md#api)

---

## Researcher / journalist

*Turn awkward public files and documents into analysis-ready, shareable data.*

### Extract tables from a report

Convert PDF or office-document tables into CSV/Parquet for review and analysis.

```bash
pip install "undatum[extract]"
undatum extract report.pdf --output-format csv --output tables.csv
undatum profile tables.csv
```

Reference: [`extract`](../README.md#extract)

### Tame an unfamiliar public-data format

Detect encoding and structure, convert to an open analytical format, and
generate a data note.

```bash
undatum sniff source.csv
undatum convert source.csv source.parquet
undatum doc source.parquet --output DATA_NOTE.md
```

Reference: [`sniff`](../README.md#sniff) · [Format support](FORMAT_SUPPORT.md)

---

## Operations / security analyst

*Search large event exports, reduce sensitive data, and create focused incident extracts.*

### Triage a large log export

Find suspicious records, select useful fields, and save a compact incident
dataset.

```bash
undatum search events.jsonl --pattern "error|denied|timeout" --ignore-case --output hits.jsonl
undatum select hits.jsonl --fields ts,service,user_id,message --output incident.parquet
```

Reference: [`search`](../README.md#search) · [`select`](../README.md#select)

### Share a privacy-safe diagnostic sample

Take a representative sample and deterministically mask identifiers before
sharing.

```bash
undatum sample events.parquet --n 10000 --output sample.parquet
undatum mask sample.parquet --fields user_id,email --method hash --salt "$SALT" --output safe.parquet
```

Reference: [`sample`](../README.md#sample) · [`mask`](../README.md#mask)

---

## AI / automation builder

*Give agents controlled dataset tools or add AI assistance to discovery and documentation.*

### Connect undatum to an MCP client

Let an agent inspect, query, and perform confirm-gated writes over datasets.

```bash
pip install "undatum[mcp]"
undatum mcp tools
undatum mcp serve
```

Reference: [AI agent tools and MCP server](../README.md#ai-agent-tools-and-mcp-server)

### Generate assisted dataset documentation

Produce structured general, schema, and quality blocks with hosted or local
models.

```bash
undatum ai doc data.csv --format json --blocks general,schema,quality
```

Reference: [`ai`](../README.md#ai) · [`doc`](../README.md#doc)

---

## Plugin author

*Add domain-specific commands, connectors, or transforms without maintaining a fork.*

### Ship a custom command

Package a Typer-based command that undatum discovers automatically after
installation.

```toml
[project.entry-points."undatum.plugins"]
my-plugin = "mypackage.plugin:register"
```

```bash
undatum plugins list
undatum plugins info my-plugin
```

Reference: [`plugins`](../README.md#plugins) · [`examples/plugins/`](../examples/plugins/)

### Integrate a proprietary data source

Add a custom connector or transform while keeping core undatum unchanged.

```python
from undatum.plugins.base import ConnectorPlugin, TransformPlugin
# implement the extension, expose register(), then install the package
```

Reference: [`plugins`](../README.md#plugins)

---

## Next steps

- [Quickstarts](QUICKSTART.md) — five-minute first success
- [Format support matrix](FORMAT_SUPPORT.md)
- [When to use undatum](POSITIONING.md)
- [Large files](LARGE_FILES.md)
- [Error handling](ERROR_HANDLING.md)
