import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig
from pathlib import Path
from test.unit.conftest import autosubmit_config
from typing import Callable, Dict


def test_expid(autosubmit_config: Callable[[str,Dict], BasicConfig]) -> None:
    """
    Function to test if the autosubmit().expid generates the paths and expid properly
    """

    autosubmit_config('a000', {})
    expid = Autosubmit().expid("Test")
    experiment = Autosubmit().describe(expid)
    path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid

    assert path.exists()
    assert experiment is not None
    assert isinstance(expid, str) and len(expid) == 4