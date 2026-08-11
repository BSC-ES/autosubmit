# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import pytest

from autosubmit.helpers.version import get_version
from autosubmit.scripts.autosubmit import _autosubmit


def test_autosubmit_script_main(mocker):
    """Test that the autosubmit script exit code.

    It must exit with the same value returned by the ``main`` function.
    """
    as_log = mocker.patch("autosubmit.scripts.autosubmit.Log")
    args = ["-v"]
    exit_code = _autosubmit(args)

    assert as_log.info.call_args[0][0] == get_version()
    assert exit_code == 0


def test_autosubmit_script_readme(mocker, autosubmit_config):
    """Test that the readme command is executed and returns 0.

    At the moment readme is still returning a boolean. Hopefully, that
    will be fixed in the near future. This test can stay just to make
    sure the command is working (it was not when this test was written).
    """
    as_conf = autosubmit_config("a000", {})
    mocker.patch("autosubmit.config.basicconfig.BasicConfig", as_conf.basic_config)
    mock_log_info = mocker.patch("autosubmit.log.log.Log.info")
    args = ["readme"]
    exit_code = _autosubmit(args)
    logged_text = " ".join(str(call) for call in mock_log_info.call_args_list)
    assert "lightweight" in logged_text
    assert exit_code == 0


def test_autosubmit_script_error_raised(mocker):
    command = "inspect"
    expid = "fail"

    args = [command, expid]
    with pytest.raises(SystemExit) as cm:
        _autosubmit(args)
    assert cm.value.code == 1
