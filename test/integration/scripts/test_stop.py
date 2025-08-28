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

import pytest
from pytest_mock import MockerFixture

from autosubmit.scripts.autosubmit import main

_EXPID = "t111"


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

    mock_input = mocker.patch("builtins.input")
    mock_input.return_value = "no"

    assert main() is None

    assert mock_input.call_count == (0 if force_yes else 1)
