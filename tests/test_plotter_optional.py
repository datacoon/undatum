"""Plotting reports missing optional dependencies before accessing data files."""

import pytest

from undatum.cmds import plotter


@pytest.mark.parametrize("operation", ["construct", "plot"])
def test_plotter_without_matplotlib_has_install_hint(monkeypatch, operation):
    monkeypatch.setattr(plotter, "MATPLOTLIB_AVAILABLE", False)
    with pytest.raises(ImportError, match="pip install matplotlib"):
        if operation == "construct":
            plotter.Plotter()
        else:
            plotter.Plotter.__new__(plotter.Plotter).plot("missing.csv", "value")
