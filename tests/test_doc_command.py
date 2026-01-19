import json
from pathlib import Path

from undatum.cmds.doc import Documenter


def test_doc_json_output(tmp_path):
    input_file = Path(__file__).parent / "fixtures" / "2cols6rows.csv"
    output_file = tmp_path / "doc.json"

    Documenter().document(str(input_file), options={
        "format": "json",
        "output": str(output_file),
        "sample_size": 2
    })

    data = json.loads(output_file.read_text(encoding="utf8"))
    assert data["metadata"]["file_type"] == "csv"
    for key in ["title", "keywords", "geographic_coverage", "temporal_coverage", "languages", "data_theme"]:
        assert key in data["metadata"]
    assert data["schema"]["tables"]
    assert isinstance(data.get("pii_fields"), list)
    field_entry = data["schema"]["tables"][0]["fields"][0]
    assert "semantic_types" in field_entry
    assert "pii" in field_entry
    assert len(data["samples"]) == 2


def test_doc_markdown_output(tmp_path):
    input_file = Path(__file__).parent / "fixtures" / "2cols6rows.csv"
    output_file = tmp_path / "doc.md"

    Documenter().document(str(input_file), options={
        "format": "markdown",
        "output": str(output_file),
        "sample_size": 1
    })

    content = output_file.read_text(encoding="utf8")
    assert "# Dataset Documentation" in content
    assert "## Metadata" in content
    assert "## Schema" in content
    assert "## Samples" in content
    assert "- **geographic_coverage**:" in content
    assert "  - coordinates_present: No" in content


def test_doc_semantic_types_options(tmp_path):
    input_file = Path(__file__).parent / "fixtures" / "2cols6rows.csv"
    output_file = tmp_path / "doc.json"

    Documenter().document(str(input_file), options={
        "format": "json",
        "output": str(output_file),
        "sample_size": 1,
        "semantic_types": True,
        "pii_detect": True,
        "pii_mask_samples": True
    })

    data = json.loads(output_file.read_text(encoding="utf8"))
    assert "pii_fields" in data
    assert data["schema"]["tables"]
