import pytest
import tempfile
from pathlib import Path

from autosubmit.platforms.locplatform import LocalPlatform


@pytest.fixture
def local_platform():
    with tempfile.TemporaryDirectory() as tempdir:
        config = {"LOCAL_ROOT_DIR": tempdir, "LOCAL_TMP_DIR": "tmp"}
        platform = LocalPlatform(expid="a000", name="local", config=config)
        yield platform


def test_file_read_size(local_platform: LocalPlatform):
    path = local_platform.config.get("LOCAL_ROOT_DIR")

    assert isinstance(path, str)

    random_file = Path(path) / "random_file"

    assert isinstance(random_file, Path)

    with open(random_file, "w") as f:
        f.write("a" * 100)

    # Test read_file limited to 10 bytes
    assert local_platform.read_file(random_file, 10).decode() == ("a" * 10)

    # Test get the file size
    assert local_platform.get_file_size(random_file) == 100
