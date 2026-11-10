import subprocess

import pytest


@pytest.mark.docs
def test_docs_build():
    result = subprocess.run(
        ["make", "xml_coverage"],
        cwd="docs",
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Doc build failed:\n{result.stderr}\n{result.stdout}"
    )
