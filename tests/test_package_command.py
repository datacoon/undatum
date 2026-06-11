import json
from pathlib import Path

from undatum.cmds.packager import Packager


def test_package_create_generates_descriptor(tmp_path):
    input_file = Path(__file__).parent / "fixtures" / "2cols6rows.csv"
    output_file = tmp_path / "datapackage.json"

    Packager().create(
        [str(input_file)],
        options={
            "output": str(output_file),
            "sample_size": 2,
        },
    )

    data = json.loads(output_file.read_text(encoding="utf8"))
    assert data["name"]
    assert len(data["resources"]) == 1
    resource = data["resources"][0]
    assert resource["path"] == str(input_file)
    assert resource["schema"]["fields"]


def test_package_create_directory_output(tmp_path):
    input_file = Path(__file__).parent / "fixtures" / "2cols6rows.csv"
    package_dir = tmp_path / "package"

    Packager().create(
        [str(input_file)],
        options={
            "package_dir": str(package_dir),
            "sample_size": 1,
        },
    )

    package_file = package_dir / "datapackage.json"
    assert package_file.exists()
    assert (package_dir / input_file.name).exists()
