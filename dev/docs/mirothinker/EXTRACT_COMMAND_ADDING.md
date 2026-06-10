## Short Answer

Yes, adding an `extract` command is worth doing for undatum, as long as:

- It is **optional** (extra install or plugin) so the core stays lean.
- You’re clear that it’s **best‑effort**, especially for messy or scanned PDFs.
- It outputs **standard tabular formats** (CSV/NDJSON/Parquet) so it plugs straight into existing undatum commands and DuckDB.

Below is a concrete, implementation‑oriented view.

---

## Why an `extract` Command Makes Sense

### Benefits

1. **Removes a common bottleneck**  
   Many real‑world datasets start as PDFs or Word docs (reports, invoices, statements, survey exports). Right now users must pre‑convert those elsewhere before using undatum. `extract` would remove that extra step.

2. **Fits undatum’s role in the workflow**  
   undatum is already about:

   - Reading structured data.
   - Cleaning / transforming.
   - Profiling and documenting.

   `extract` becomes the **ingestion front‑end** for unstructured or semi‑structured office formats, feeding into the rest of the pipeline.

3. **Works naturally with your other features**

   - After `extract`, users can run `stats`, `validate`, `mask`, `profile`, `package` (Frictionless).
   - It’s an obvious first step in YAML pipelines:
     ```yaml
     steps:
       - name: ingest_docs
         command: extract
         args:
           input: data/reports/*.pdf
           output_format: parquet
       - name: profile
         command: stats
     ```

4. **High user value, moderate complexity**  
   Mature Python libraries already exist for:

   - PDF table/text extraction (e.g., pdfplumber, Tabula, Camelot).
   - DOC/DOCX (python‑docx).
   - Spreadsheets (pandas / openpyxl).

   So you’re mostly integrating them and standardizing their output, not inventing new extraction tech.

### Caveats

- **Extraction is imperfect by nature**  
  - Complex layouts and scanned PDFs can’t be reliably turned into perfect tables.
  - You’ll need to set expectations and provide options like `--method tables|text` and `--ocr`.

- **Heavy dependencies**  
  - PDF and especially OCR stacks (Tesseract, pdf2image, etc.) are large and sometimes tricky to install.
  - This argues strongly for **optional extras or a plugin** rather than bundling into the core.

---

## Recommended Design

### Command Shape

Introduce a dedicated sub‑command:

```bash
# Basic usage: convert a PDF into CSV
undatum extract report.pdf > report.csv

# Explicit output format
undatum extract report.pdf --output-format json  > report.json

# Word document tables to Parquet
undatum extract survey.docx --output-format parquet --output survey.parquet

# Multiple files (each becomes a resource/file)
undatum extract data/*.pdf --output-dir extracted/

# With options specific to PDFs
undatum extract report.pdf \
  --method tables \
  --pages 1-5 \
  --output-format csv \
  --output report_p1_5.csv
```

Key options:

- `--output-format {csv,json,ndjson,parquet,datapackage}`
- `--output` / `--output-dir`
- `--method {tables,text,ocr}` (default varies by format: `tables` for PDF, `text` for DOCX)
- `--pages` (for PDFs: `1-3,7,10-12`)
- `--ocr` (boolean, only needed for scanned PDFs)
- `--flatten` (combine multiple tables into one table with an extra `table_index` column)

### How It Integrates

- **Input formats**: PDF, DOC/DOCX, XLS/XLSX, possibly PPTX later.
- **Output**: always something undatum already understands (CSV/NDJSON/Parquet or a Data Package).
- **Downstream**: immediately usable by:
  - `stats` / `profile` for quick health checks.
  - `validate` with rules.
  - `mask` for PII.
  - `convert` for further format shifts.
  - `package` to produce a Frictionless `datapackage.json`.

This keeps `extract` as a **thin ingestion layer**, not another full processing engine.

---

## Implementation Strategy

### 1. Keep It Optional

Ship as:

- A plugin discovered via entry points, e.g. `undatum-extract`, or  
- An extras group: `pip install "undatum[extract]"`.

This way:

- Core users who only deal with CSV/JSON aren’t forced to pull in PDF/OCR deps.
- You can evolve the extractor independently (even version it separately if needed).

### 2. Start Narrow and Reliable

Initial scope:

- **PDF**:
  - Focus on “digital” PDFs (not scanned).
  - Primary mode: `--method tables` using a table‑extraction library.
  - Secondary: `--method text` that outputs line‑based or paragraph‑based text (usable for further parsing or search).
- **DOCX**:
  - Extract tables into tabular output.
  - Optionally extract full text into a simple text/NDJSON representation.
- **XLS/XLSX**:
  - Delegate to `pandas.read_excel` and treat like a normal structured source (this will feel very natural to users).
- Defer PPTX, ODS, complex archive formats to a later phase.

### 3. Be Transparent About Ambiguity

- For PDFs with multiple tables:
  - Default: output each table as a separate file/resource.
  - With `--flatten`: concatenate into one table and add `source_table_index`, `source_page` columns.
- For documents that don’t contain clear tables:
  - Fail gracefully with a clear message: “no tables detected; try `--method text`”.
- For scanned PDFs:
  - Error or warn if `--ocr` isn’t supplied when extraction finds no text.

### 4. Fit Into the Existing Roadmap

Given your earlier improvement plan:

- **Before**:
  - Finish DuckDB integration (`--engine auto`).
  - Add thread/chunking support.
  - Basic S3/remote I/O.
- **Alongside**:
  - Enhanced `stats/profile` and `validate` (they’ll consume the extracted data).
  - Frictionless package generation (so `extract` → `package` is a natural flow).
- **After**:
  - TUI/web UI, which can later expose “upload a PDF/Word doc and see a table”.

This avoids getting bogged down in extraction before the core engine is robust.

---

## Conclusion

Adding an `extract` command is a **good and coherent extension** of undatum:

- It makes undatum a more complete tool by covering the “document → data” gap.
- It leverages existing libraries rather than inventing new parsing logic.
- It integrates neatly with your pipeline, profiling, validation, and packaging features.

I’d recommend:

1. Implement `extract` as an **optional plugin/extra**.
2. Start with **PDF (tables + text), DOCX (tables), XLSX**.
3. Output **CSV/NDJSON/Parquet** and optionally a **Frictionless Data Package**.
4. Place it in **Phase 2** of your roadmap, after core performance and storage features.

If you’d like, I can next draft a concrete CLI spec and minimal Python skeleton that matches undatum’s existing command style.