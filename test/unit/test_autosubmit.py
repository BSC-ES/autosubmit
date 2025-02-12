import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig
from pathlib import Path
from test.unit.conftest import autosubmit_config
from typing import Callable, Dict


@pytest.mark.parametrize("fake_dir, real_dir", [
    pytest.param("a000", "a000", marks=pytest.mark.xfail(reason="Meant to fail since it can't create a folder if one already exists")), # test meant to FAIL
    ("","a000"), # test meant to PASS
    ("",""), ]) # test meant to PASS with a generated expid
def test_expid(autosubmit_config: Callable[[str,Dict], BasicConfig], fake_dir, real_dir) -> None:
    """
    Function to test if the autosubmit().expid generates the paths and expid properly

    ::fake_dir -> if fake dir exists test will fail since it won't be able to generate folder
    ::real_dir -> folder it'll try to create and experiment id
    """

    if fake_dir != "":
        path = Path(BasicConfig.LOCAL_ROOT_DIR) / fake_dir
        path.mkdir()

    if real_dir != "":
        autosubmit_config(real_dir, {})
    expid = Autosubmit().expid("Test", real_dir)

    if real_dir == "":
        autosubmit_config(expid, {})

    experiment = Autosubmit().describe(expid)

    path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid

    assert path.exists()
    assert experiment is not None
    assert isinstance(expid, str) and len(expid) == 4