# Change: Add Plot Command for Data Visualizations

## Why

While `undatum` provides comprehensive statistics via the `stats` command, users often need visual representations to better understand data distributions, patterns, and relationships. Adding a `plot` command would enable quick data exploration through visualizations without requiring external tools or Python scripts.

**Current Issues:**
1. **No visualization support**: Users must export data and use external tools (Excel, Python scripts, etc.) to create plots
2. **Workflow interruption**: Breaking the workflow to visualize data reduces productivity
3. **Limited quick insights**: Statistical summaries don't always reveal patterns that visualizations can show
4. **Tool fragmentation**: Requires knowledge of multiple tools for complete data analysis

**Expected Benefits:**
- **Quick visual exploration** of data distributions and patterns
- **Integrated workflow** - visualize data directly from undatum
- **Multiple plot types** for different analysis needs
- **Export capabilities** for reports and presentations
- **Minimal dependencies** with optional advanced backends

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 5.3)

## What Changes

- **ADDED**: `undatum plot` command:
  - Generate histograms for numerical field distributions
  - Generate bar charts for categorical field frequencies
  - Support multiple plot types (histogram, bar, scatter, line)
  - Output to file (PNG, SVG, PDF) or display
  - Integration with `stats` command data
- **ADDED**: Plot backends:
  - Matplotlib (default, required)
  - Optional Plotly/Bokeh for interactive plots
- **ADDED**: Plot configuration:
  - Customizable titles, labels, colors
  - Multiple fields support
  - Output format options

All changes are additive. No existing functionality is modified.

## Impact

- **Affected specs**: `data-visualization` capability (new)
- **Affected code**:
  - New `undatum/cmds/plotter.py` module
  - New `undatum/core.py` - Add `plot` command
- **Dependencies**: matplotlib (required), plotly/bokeh (optional)
- **Backward compatibility**: Fully backward compatible - new command only
