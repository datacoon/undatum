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

Example project `undatum.yaml`:

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
  delimiter: ","
  quotechar: '"'
  format_out: json
```

`defaults:` keys: `engine`, `threads`, `progress`, `encoding`, `delimiter`, `quotechar`, `format_out`.

### Precedence

**CLI defaults** (`defaults:` / `UNDATUM_*`): later sources win — environment, then `~/.undatum/config.yaml`, then `./undatum.yaml`. Explicit CLI flags override all of them.

| Environment | Config key |
|-------------|------------|
| `UNDATUM_ENGINE` | `defaults.engine` |
| `UNDATUM_THREADS` | `defaults.threads` |
| `UNDATUM_PROGRESS` | `defaults.progress` |
| `UNDATUM_ENCODING` | `defaults.encoding` |
| `UNDATUM_DELIMITER` | `defaults.delimiter` |
| `UNDATUM_QUOTECHAR` | `defaults.quotechar` |
| `UNDATUM_FORMAT_OUT` | `defaults.format_out` |

**AI settings** (`ai:`): environment, then the first config file found (`./undatum.yaml` preferred over `~/.undatum/config.yaml`), then CLI flags. Provider API keys (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, …) are always read from the environment. See [AI documentation](/integrations/ai).
