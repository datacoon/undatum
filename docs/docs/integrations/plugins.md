---
title: "Plugins"
description: "Extend undatum with commands, connectors, and transforms"
---
# Plugins

Add domain-specific commands, connectors, or transforms without maintaining a fork.

```toml
[project.entry-points."undatum.plugins"]
my-plugin = "mypackage.plugin:register"
```

```bash
undatum plugins list
undatum plugins info my-plugin
undatum plugins validate
```

```python
from undatum.plugins.base import ConnectorPlugin, TransformPlugin
```

Example plugins live in [examples/plugins](https://github.com/datenoio/undatum/tree/main/examples/plugins).
See the [`plugins`](/commands/plugins) command for the CLI surface.
