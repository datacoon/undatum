# How undatum Could Be Improved to Fit User Needs Better

**Date:** 2026-07-21
**Basis:** (1) the comparison report *undatum vs. structured-text-tools*; (2) mined user-need signals — all 22 GitHub issues, 15 PRs, PyPI download stats (~18.9k downloads / 181 days, ~105/day), and the verified downstream user (CrateDB's `cratedb-toolkit`).

---

## 0. What real users actually asked for (signal summary)

Mining the issue tracker changes the picture vs. the feature-matrix comparison. Of 22 issues, only **5 came from external users** — and they cluster hard:

| Theme | Evidence | Status |
|---|---|---|
| **Install/packaging breakage** | #4 Homebrew formula (open **5+ years**, `help wanted`); #19 `No module named xmltodict` (broken release); #37 `ModuleNotFoundError: chardet` (broke CrateDB CI) | 1 of 3 fixed pattern repeats — missing-dep bugs shipped twice |
| **Memory/scalability walls** | #34 OOM converting 5–8 GB jsonl.zst→parquet even with 96 GB RAM (**open since 2024-11**); #18 multiprocessing (open); #20 parquet compression (fixed in 2 days) | top open bug |
| **Feature requests** | jsonl→csv (#1, shipped), parquet compression (#20, shipped) | maintainer ships user requests **fast** when engaged |
| Maintainer's own backlog | 17/22 issues: format expansion, Excel ops (#11), docx tables (#12), DB dumps (#13), diff/apply (#3) | mostly open |

**Reading:** undatum has real, persistent usage (~105 downloads/day) but a razor-thin community surface — Discussions disabled, 15/15 PRs are unmerged Snyk-bot bumps, zero external human contributions. The loudest unmet needs are not new formats; they're **trust** (don't ship broken installs), **scale** (don't OOM on multi-GB files), and **distribution** (let people install it).

Meanwhile recent development (v1.3–1.5: API serving, AI/MCP, pipelines) is maintainer-driven and widened scope while #34 and #4 stayed open. The core recommendation: **consolidate before expanding**.

---

## 1. User segments and their needs

| Segment | What they use | What they need most |
|---|---|---|
| **A. CLI data engineers** (csvkit/miller/DuckDB crowd) | `convert`, `stats`, `sql`, `validate` on GB-scale files | Memory-safe streaming, speed, single-binary install, honest format docs |
| **B. Open-data publishers** (the tool's origin niche) | `validate` (INN/OGRN), `package` (Frictionless/SDMX), Excel sources | Excel parity in analysis commands (#11), docs, packaging |
| **C. LLM/agent builders** (new MCP audience) | `mcp serve`, agent tools, `ai filter/doc` | Rock-solid core (agents amplify breakage), accurate schemas, predictable errors |
| **D. Downstream library users** (cratedb-toolkit precedent) | iterabledata via undatum | Semver discipline, no missing-dep releases, stable import surface |

---

## 2. Prioritized roadmap

### P0 — Trust & correctness (quick wins, days of effort)

1. **Fix the gzip→DuckDB routing bug.** `DUCKABLE_CODECS = ["zst", "gzip", "raw"]` but iterabledata's id is `"gz"` — gzipped files silently fall back to the slow Python engine. One-line fix, directly addresses the exact workflow in #34.
2. **Make parquet work out of the box.** Add `pyarrow` to default dependencies (or fail with a precise install hint everywhere). Parquet is the flagship output (issues #20, #34 both about it); a default install that errors on `convert x.csv x.parquet` fails the first-5-minutes test.
3. **Fix the OOM in #34.** Stream parquet writes in batches (pyarrow `ParquetWriter` per row-group, or route large conversions through the DuckDB engine which spills to disk). Add an explicit `--low-memory` mode + a docs section on large-file behavior (the issue author asked for exactly this).
4. **CI install-gate.** A matrix job that does `pip install dist/*.whl` on a clean venv and runs smoke commands (`convert`, `stats`, `validate` on each core format). Two of five external issues ever filed were missing-dependency bugs in shipped releases — this gate would have caught both (#19, #37).
5. **Repo hygiene:** drop committed `pylint_report.txt`, 1.3 MB `data.csv`, IDE dirs; fix CHANGELOG placeholder dates (`2024-XX-XX`) and the missing 1.2.0 section. Signals engineering maturity to evaluators comparing against miller/csvkit.

### P1 — Scalability & performance (weeks)

6. **Streaming `sort` and `dedup`.** Both currently load the entire dataset — a direct contradiction of the large-file positioning, and the next OOM reports after #34. External merge sort (spill to temp files) for `sort`; disk-backed set or Bloom-filter-with-exact-pass for `dedup`.
7. **Smarter delimiter/dialect sniffing.** Replace first-line character counting with `csv.Sniffer` + quote-aware sampling over N lines. Cheap, removes a whole class of silent mis-parses.
8. **Multiprocessing where safe (#18).** Recursive multi-file conversion already has `--threads`; extend parallelism to chunked single-file conversion for order-insensitive ops (convert/validate/stats via merge), keeping row-at-a-time semantics as the default.
9. **Push more work into DuckDB.** Stats/profiling and filter-heavy `select` on CSV/JSONL/Parquet can run as SQL with spill-to-disk instead of Python loops — undatum already has the engine selector; widen auto-routing coverage.

### P2 — Distribution & installation (the #1 external pain theme)

10. **Ship the Homebrew formula (#4).** Open 5+ years with `help wanted`. Modern Python options make this easy: `brew` formula wrapping a PyInstaller binary, or document `brew install pipx && pipx install undatum` as the official macOS path and close the issue honestly.
11. **Single-binary builds.** Competing tools win ops contexts with static binaries (miller, dasel, rq, csvtk, DuckDB). Provide PyInstaller/`pex`/`shiv` artifacts per release (linux/mac/win). For a tool whose selling point is "one CLI for everything," install friction is existential.
12. **Document `uv tool install undatum` / `pipx`** as first-class install methods in the README header.

### P3 — Documentation & onboarding

13. **Honest format-support matrix.** Publish a generated page listing all ~120 format classes with: read/write capability (57 are read-only!), required extras, streaming support. Current breadth marketing without the fine print erodes trust when users hit the wall.
14. **Task-oriented quickstart.** "CSV→Parquet in 30 seconds", "Validate a dataset before publishing", "Query JSONL with SQL". The 2,838-line README is reference-grade but buries the first success.
15. **Positioning page** ("When undatum vs. miller vs. DuckDB vs. csvkit") — the comparison table already exists in our report; a condensed version in the docs pre-empts the #1 evaluator question and captures search traffic.

### P4 — Feature parity users actually requested

16. **Excel everywhere (#11).** Excel is readable via iterabledata but missing from `analyze/uniq/frequency/select`. Since all commands consume the same row iterators, this is mostly removing artificial format gates — high value for the open-data segment (B), low effort.
17. **Close the loop on DB dumps (#13)** — `db query`/`db load` partially cover it; either document the recipe or ship `db dump --to parquet`.
18. **diff/apply-patch (#3)** — `diff` exists with tolerances; the 2020-era binary-diff port can be scoped down or closed.
19. **Explicitly defer niche formats** (Veriform #16 — maintainer already marked low priority; correct call — say so publicly and close as "not planned").

### P5 — Community & positioning (strategic)

20. **Submit to dbohdan/structured-text-tools.** undatum qualifies for the Multiformat section and fills a real hole (nothing listed handles BSON; only rq/Remarshal touch MsgPack). 7.1k-star list = the single highest-leverage discovery channel for exactly segment A. Do this *after* P0 items 1–3 so the first impression survives a 5-minute trial.
21. **Open the community surface.** Enable GitHub Discussions; add CONTRIBUTING.md + `good first issue` labels; respond to #34/#4 even if the answer is a workaround. Zero external PRs in 6 years is a barrier problem, not an interest problem (105 downloads/day).
22. **Handle the Snyk PR queue** (15 open bot PRs, none merged) — enable auto-merge for patch-level security bumps or close them; an unattended PR queue signals an unattended project.
23. **Nurture the downstream channel.** cratedb-toolkit's CI already depends on this ecosystem (#37) and its maintainer is engaged — showcase it, coordinate the pyiterable migration, add a downstream-compat CI check. Downstream users are the strongest retention signal a solo project can have.
24. **Release discipline.** Given v1.3–1.5 shipped API/AI/MCP/pipelines within one month: adopt semver-strict release notes, deprecate loudly, and keep a stable-command guarantee — agent builders (segment C) programmatically depend on CLI surface stability.

---

## 3. Strategic guidance (what *not* to do)

- **Don't chase jq/dasel-style document surgery.** undatum's data model is record streams; deep nested-document editing is a different market with entrenched winners.
- **Don't add more format breadth until read-only gaps and install gaps are closed** — breadth is already the differentiator; trust is the bottleneck.
- **Do double down on the agent-native niche** (MCP server, agent tools) — no tool in the structured-text-tools list occupies it, and it aligns with where CLI data tooling demand is moving. But agents amplify core breakage: a flaky `convert` inside an agent loop is worse than a flaky manual run. Agent features should ride on the P0/P1 hardened core.
- **Sequence matters:** P0 (trust) → submit to the list (discovery) → P1/P2 (scale & install) → P5 (community). Shipping AI features while #34 OOMs and brew 404s inverts user priorities as measured by actual issues.

---

## 4. Impact × effort matrix

| Recommendation | Impact | Effort | Addresses |
|---|---|---|---|
| Fix gzip codec id (#P0.1) | High | 1 line | #34-class perf complaints |
| Default-install pyarrow (#P0.2) | High | Hours | #20, #34, first-run UX |
| Streaming parquet / --low-memory (#P0.3) | **Highest** | Days | #34 (top open bug) |
| CI install-gate (#P0.4) | High | Hours | #19, #37 (2 of 5 external issues ever) |
| Homebrew/single-binary (#P2.10–12) | High | Days | #4 (open 5+ yrs), ops adoption |
| Streaming sort/dedup (#P1.6) | High | 1–2 weeks | next wave of OOMs |
| Format-support matrix (#P3.13) | Medium-High | Hours (generated) | trust, evaluators |
| Excel ops parity (#P4.16) | Medium | Days | #11, open-data segment |
| List submission (#P5.20) | High (discovery) | 1 PR | adoption |
| Discussions + CONTRIBUTING (#P5.21) | Medium | Hours | zero-contributor problem |

**Bottom line:** user needs, as voiced in undatum's own tracker, are unglamorous — *install it easily, don't break my CI, don't OOM on my 8 GB file*. Fitting users better means consolidating the core (P0–P2) before the next feature wave, then converting the tool's genuine uniqueness (BSON breadth + validation/masking + agent-native design) into discovery via the structured-text-tools list and the MCP ecosystem.

<KIMI_REF type="file" path="sandbox:///mnt/agents/output/undatum-improvement-recommendations.md" />
