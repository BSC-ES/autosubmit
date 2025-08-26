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

"""Test for the autosubmit pklfix command"""

from pathlib import Path
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

from autosubmit.scripts.autosubmit import main

_EXPID = "t111"


def test_autosubmit_pklfix_command_invocation(autosubmit_exp, mocker: MockerFixture):
    """
    Test if the pkl_fix function was called by the CLI
    """
    autosubmit_exp(_EXPID, experiment_data={})

    mocker.patch("sys.argv", ["autosubmit", "pklfix", "-f", _EXPID])

    with patch("autosubmit.autosubmit.Autosubmit.pkl_fix") as mock_pklfix:
        main()

        mock_pklfix.assert_called_once()

        passed_args = mock_pklfix.call_args[0]
        assert passed_args[1] is True


@pytest.mark.parametrize("force", [True, False])
def test_pklfix_bypass_prompt_confirmation(
    autosubmit_exp, mocker: MockerFixture, force: bool
):
    """
    Test if the --force option bypasses the prompt confirmation
    """
    exp = autosubmit_exp(_EXPID, experiment_data={})

    as_conf = exp.as_conf

    # Create empty pkl files
    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(_EXPID)
    pkl_folder_path = exp_path.joinpath("pkl")
    current_pkl_path = pkl_folder_path.joinpath(f"job_list_{_EXPID}.pkl")
    backup_pkl_path = pkl_folder_path.joinpath(f"job_list_{_EXPID}_backup.pkl")

    with open(current_pkl_path, "w") as f:
        f.write("some big content here")
    with open(backup_pkl_path, "w") as f:
        f.write("some big content here")

    # Mock command line arguments
    passed_args = ["autosubmit", "pklfix"] + (["-f"] if force else []) + [_EXPID]
    mocker.patch("sys.argv", passed_args)

    with patch(
        "autosubmit.autosubmit.Autosubmit._user_yes_no_query"
    ) as mock_user_yes_no_query:
        mock_user_yes_no_query.return_value = False

        assert main() is None

        if force:
            mock_user_yes_no_query.assert_not_called()
        else:
            mock_user_yes_no_query.assert_called_once()
