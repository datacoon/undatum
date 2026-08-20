---
title: "Cookbook"
description: "Pick a role and goal, then follow verified undatum commands"
---
# Cookbook

undatum covers many workflows. This page is a task-oriented index: find the row
that sounds like you, then follow the linked reference sections. If you are
completely new, do the [five-minute quickstart](/getting-started/quick-start) first.

| You are a… | You want to… | Start with |
|------------|--------------|------------|
| [Data analyst](/use-cases/sql-and-analytics) | Inspect unfamiliar files and answer questions without writing a program | `table`, `tui`, `web`, `profile`, `frequency`, `sql`, `plot` |
| [Data engineer](/use-cases/data-pipelines) | Build repeatable, streaming transformations across formats, databases, and object storage | `convert`, `dedup`, `db dump` / `db load`, `pipeline` |
| [Data steward](/use-cases/quality-and-packaging) | Assess quality, encode reusable rules, and produce evidence before data is released | `analyze`, `validate`, `diff`, `schema` |
| [Open-data publisher](/use-cases/quality-and-packaging) | Publish documented, portable, standards-friendly datasets | `package`, `doc`, `mask`, `validate` |
| [Application developer](/integrations/sdk) | Embed data preparation in Python or expose a dataset through a read-only API | Python `Dataset` SDK, `api` |
| [Researcher / journalist](/use-cases/format-conversion) | Turn awkward public files and documents into analysis-ready, shareable data | `extract`, `sniff`, `convert`, `doc` |
| [Operations / security analyst](/commands/search) | Search large event exports, reduce sensitive data, and create focused incident extracts | `search`, `select`, `sample`, `mask` |
| [AI / automation builder](/use-cases/agents-and-mcp) | Give agents controlled dataset tools or add AI assistance to documentation | `mcp`, `ai doc`, LangChain tools |
| [Plugin author](/integrations/plugins) | Add domain-specific commands, connectors, or transforms without a fork | `plugins`, entry points |

All commands below are also available via the shorter `data` alias
(`data convert …` is identical to `undatum convert …`).


## Detailed walkthroughs

- [Format conversion](/use-cases/format-conversion)
- [Data pipelines](/use-cases/data-pipelines)
- [Quality and packaging](/use-cases/quality-and-packaging)
- [SQL and analytics](/use-cases/sql-and-analytics)
- [Agents and MCP](/use-cases/agents-and-mcp)
