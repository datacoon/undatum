## 1. Is this a good idea for undatum?

Yes, but with **two important constraints**:

1. Make it an **optional feature / plugin**, not part of the tight core.  
   - Extra dependencies (FastAPI/Eve, uvicorn, auth, etc.) don’t need to burden users who only want CLI data processing.

2. Focus it as a **read‑mostly, data‑sharing API** for files and pipeline outputs, **not as a general OLTP database replacement**.  
   - The sweet spot is: “I have a CSV/JSONL/Parquet (or pipeline output) and want to quickly expose it over HTTP with filtering/pagination for other teams or tools.”

Within those constraints, it’s a **very strong addition** that:

- Leverages your existing DuckDB integration.
- Fits nicely with future `pipeline`, `doc`, `package` (Frictionless) features.
- Gives undatum a compelling “from raw file to documented, queryable dataset” story.

---

## 2. Conceptual design (adapting APICrafter’s idea)

APICrafter flow (simplified):

1. **discover** against MongoDB → generate `apicrafter.yml` with resource/field definitions.
2. **run** → start Eve API using that config, auto‑wire CRUD + OpenAPI docs.

For undatum, adapt that to **file‑backed resources**:

1. **`undatum api discover`**  
   - Inputs: one or more data files (CSV, JSONL, Parquet, maybe DB queries later).  
   - Output: an **API config** (YAML/JSON) describing:
     - Resources (one per file or logical dataset).
     - Schema (columns, types, constraints, primary key candidates).
     - Query options (which filters are allowed, default pagination, etc.).
   - Implementation: reuse your existing schema/stats logic.

2. **`undatum api serve`**  
   - Input: the API config file.  
   - Behavior: spin up a **FastAPI** (or Eve/Flask) application where:
     - Each resource becomes an endpoint (`/resource_name`).
     - Backend is **DuckDB** reading from the file (or view) on demand.
     - OpenAPI/Swagger docs are automatically available.

3. (Optional convenience) **`undatum api run <files>`**  
   - Do `discover` in‑memory and immediately serve without writing a config file, useful for quick ad‑hoc use.

This mirrors APICrafter’s DX, but with **files + DuckDB** instead of MongoDB + Eve.

---

## 3. Recommended technical approach

### 3.1. Keep it as a plugin / extra

Package it as something like:

- PyPI extra: `pip install "undatum[api]"`  
  or
- Separate plugin package: `pip install undatum-api` discovered via entrypoints.

Within undatum, register a top‑level command:

```bash
undatum api discover ...
undatum api serve ...
undatum api run ...
```

This keeps:

- **Core undatum**: pure data processing, profiling, conversion.  
- **API plugin**: extra server runtime and HTTP features for those who want them.

---

### 3.2. API config format (analogous to `apicrafter.yml`)

Define a compact, human‑editable YAML (or JSON). Example:

```yaml
resources:
  - name: sales
    path: data/sales.csv           # can also support s3://, https://, etc.
    format: csv                    # csv|jsonl|parquet|...
    primary_key: [transaction_id]  # optional
    read_only: true                # default: only GET
    fields:
      - name: transaction_id
        type: integer
      - name: product_name
        type: string
      - name: amount
        type: float
      - name: sold_at
        type: datetime
    pagination:
      default_limit: 50
      max_limit: 1000
    query:
      allowed_ops: ["eq", "ne", "lt", "gt", "le", "ge", "like"]
      allowed_order_by: ["sold_at", "amount"]
```

Your **`discover` command** should:

- Use undatum’s existing schema detection/stats to infer:
  - `fields.{name,type}`
  - `primary_key` candidates (e.g., columns with unique values).
- Fill sensible defaults for pagination and query options.
- Allow CLI overrides (e.g., specifying primary key manually).

Example CLI:

```bash
undatum api discover sales.csv customers.parquet --output api.yml
```

---

### 3.3. Server implementation (FastAPI + DuckDB)

**Why FastAPI instead of Eve for this case:**

- You are not tied to MongoDB here.
- FastAPI gives:
  - Very easy path operations.
  - Automatic OpenAPI/Swagger UI out of the box.
  - Good async support.
- DuckDB is an ideal query engine over CSV/JSONL/Parquet.

**Core idea:**

- For each resource in the config:
  - Create a DuckDB view or table pointing to the file using `read_csv_auto`, `read_json_auto`, or `read_parquet`.
  - Define FastAPI routes mapped to that view.

**Endpoint patterns (auto‑generated):**

- `GET /sales`  
  - Query params:
    - `limit`, `offset` (pagination).
    - `field__op=value` for filtering, where `op ∈ {eq,ne,lt,gt,le,ge,like}`.
    - `order_by`, `order_dir`.
- `GET /sales/{pk}` if `primary_key` is defined.

**Example filter semantics:**

- `GET /sales?amount__gt=100&product_name__like=Apple%25`  
  → translates to SQL `WHERE amount > 100 AND product_name LIKE 'Apple%'`.

This translation layer is simple but powerful, and can be expanded gradually.

---

### 3.4. Example end‑to‑end usage

1. **Prepare the dataset** with undatum:

   ```bash
   undatum convert raw.csv --output cleaned.parquet
   undatum stats cleaned.parquet
   ```

2. **Generate API config**:

   ```bash
   undatum api discover cleaned.parquet --output api.yml
   ```

3. **Run the server**:

   ```bash
   undatum api serve --config api.yml --port 8000 --host 0.0.0.0
   ```

4. **Query from any client**:

   ```bash
   curl "http://localhost:8000/cleaned?limit=20&offset=0"
   curl "http://localhost:8000/cleaned?country__eq=US&amount__gt=100"
   ```

5. **Explore docs**: open `http://localhost:8000/docs` (Swagger UI auto‑generated).

---

## 4. Design choices & trade‑offs

### 4.1. Read‑only vs. write access

For undatum’s file‑centric model:

- **MVP: read‑only** API.
  - Serves data from underlying files (or pipeline outputs).
  - Is safe and conceptually simple: HTTP is a *view* on a dataset, not an OLTP DB.
- Later, you could add:
  - `POST /resource` to upload a file and rebuild the view.
  - `PUT /resource/config` to update schema or filters.

But do *not* try to emulate Mongo‑style CRUD at the row level on top of static files; it is mismatched to the storage model and invites complexity.

### 4.2. Performance and big data

Because you already use **DuckDB**:

- For Parquet/CSV/JSONL, DuckDB can push down predicates and read only necessary columns.
- For large files:
  - Always use `LIMIT/OFFSET` and avoid returning huge result sets.
  - Consider a `max_limit` in config to protect the server.

This makes the API still usable even on multi‑GB datasets.

### 4.3. Security and deployment

MVP:

- Expose on `localhost` by default.
- Optionally allow `--host 0.0.0.0` with a big warning.
- Add simple **token/Bearer auth** or Basic Auth via a config section later.

In many teams, this API will sit behind an Nginx/Traefik/API‑gateway anyway, which can handle TLS and more advanced auth.

---

## 5. How it plugs into other planned features

This API capability aligns well with other improvements:

- **Pipelines**:  
  `undatum pipeline run pipeline.yml && undatum api run output.parquet`  
  → clean/process data, then immediately expose it as an API.

- **`doc` command**:  
  - You can add a section to the data documentation showing example API queries.
  - Optionally generate docs from the API config itself.

- **Frictionless `package`**:  
  - Include API URLs in the Data Package metadata as an additional access method.

- **`extract` for PDF/DOCX/etc.**:  
  - Extract tabular data → store as CSV/Parquet → serve via API without extra glue.

This strengthens undatum’s position as a **complete path from raw data → clean dataset → documented → accessible via HTTP**.

---

## 6. Practical implementation steps

If you want a concrete to‑do list:

1. **Define API config schema** (YAML/JSON) and document it.
2. Implement **`undatum api discover`**:
   - Accept multiple input files.
   - Use existing undatum schema inference to fill `fields` and `primary_key` candidates.
   - Dump to `api.yml`.
3. Implement **`undatum api serve`** as a separate module that:
   - Loads `api.yml`.
   - For each resource:
     - Creates a DuckDB view over the file.
     - Registers FastAPI routes for list, filter, detail (if primary key).
   - Exposes OpenAPI docs.
4. Package it as `undatum[api]` or `undatum-api`.
5. Add basic examples and docs:
   - “Serve a CSV as REST in 30 seconds.”
   - “Serve pipeline output as an API.”

---

## 7. Conclusion

- **Is it a good idea?**  
  Yes — as an **optional, read‑oriented REST API layer** for publishing files/pipeline outputs. It fits undatum’s goals and existing tech stack very well.

- **Best implementation approach?**  
  - Copy APICrafter’s pattern (discover → config → run), but:
    - Use **files + DuckDB** instead of MongoDB.
    - Use **FastAPI** (or Eve if you want consistency with APICrafter) for auto‑generated REST + OpenAPI.
  - Expose this via `undatum api discover` / `undatum api serve` commands in a plugin.

If you’d like, I can refine this down into a specific CLI and config spec (with argument names matching undatum’s current style) that you can drop straight into your repo as an initial design doc.