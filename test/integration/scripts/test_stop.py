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

"""Test for the autosubmit stop command"""

from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

from autosubmit.scripts.autosubmit import main

_EXPID = "t111"


def test_autosubmit_stop_command_invocation(autosubmit_exp, mocker: MockerFixture):
    """
    Test if the stop function was called by the CLI
    """
    autosubmit_exp(_EXPID, experiment_data={})

    mocker.patch("sys.argv", ["autosubmit", "stop", "-y", _EXPID])

    with patch("autosubmit.autosubmit.Autosubmit.stop") as mock_stop:
        main()

        mock_stop.assert_called_once()

        passed_args = mock_stop.call_args[0]
        assert passed_args[7] is True


@pytest.mark.parametrize("force_yes", [True, False])
def test_stop_bypass_prompt_confirmation(
    autosubmit_exp, mocker: MockerFixture, force_yes: bool
):
    """
    Test if the -y option bypasses the prompt confirmation
    """
    autosubmit_exp(_EXPID, experiment_data={})

    # Mock command line arguments
    passed_args = ["autosubmit", "stop"] + (["-y"] if force_yes else []) + [_EXPID]
    mocker.patch("sys.argv", passed_args)

    with patch("builtins.input") as mock_input:
        mock_input.return_value = "no"

        assert main() is None

        if force_yes:
            mock_input.assert_not_called()
        else:
            mock_input.assert_called_once()
