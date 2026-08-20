---
title: "config"
description: "Show resolved CLI defaults from files and environment"
---
# `config`

Inspect the resolved CLI defaults from `undatum.yaml`, `~/.undatum/config.yaml`, and `UNDATUM_*` environment variables.

```bash
undatum config
undatum config show
```

Example project config:

```yaml
ai:
  provider: openai
  model: gpt-4o-mini
  timeout: 30
defaults:
  engine: duckdb
  threads: 4
  progress: true
  encoding: utf8
  format_out: json
```

Environment variables such as `UNDATUM_AI_PROVIDER` and `UNDATUM_QUOTECHAR` override file defaults. CLI flags override both.
