---
title: "frequency"
description: "undatum frequency command reference"
---
# `frequency`

Calculates frequency distribution for specified fields.

```bash
undatum frequency --fields category data.jsonl
undatum frequency --fields status,region data.csv
undatum frequency --fields city --format-out json --output freq.json data.csv
undatum frequency --fields city workbook.xlsx --table Sheet2
undatum frequency --fields capital_city.lat nested.jsonl --flatten-nested
```
