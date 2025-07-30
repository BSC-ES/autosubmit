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

import subprocess
from pathlib import Path

import pytest

from autosubmit.log.log import Log


def test_add_autosubmit_dict(autosubmit_config, mocker):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={})
    as_conf._add_autosubmit_dict()
    assert "AUTOSUBMIT" in as_conf.experiment_data
    # test log.warning has been called
    mocker.patch.object(Log, "warning")
    as_conf._add_autosubmit_dict()
    Log.warning.assert_called_once()  # type: ignore


@pytest.mark.parametrize("is_owner", [True, False])
def test_load_workflow_commit(autosubmit_config, tmpdir, mocker, is_owner):
    """Test that the workflow commit is correctly injected into the experiment_data (Autosubmit will add it to the database)."""
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={}
    )
    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig.is_current_logged_user_owner",
        new_callable=mocker.PropertyMock,  # This is needed for property mocking
        return_value=is_owner
    )
    as_conf.reload(force_load=True)
    assert "AUTOSUBMIT" in as_conf.experiment_data
    assert "WORKFLOW_COMMIT" not in as_conf.experiment_data["AUTOSUBMIT"]

    as_conf.experiment_data = {
        "AUTOSUBMIT": {},
        "ROOTDIR": tmpdir.strpath,
        "PROJECT": {
            "PROJECT_DESTINATION": 'git_project',
            'PROJECT_TYPE': 'GIT'
        }
    }
    project_dir = Path(as_conf.get_project_dir())

    project_dir.mkdir(parents=True, exist_ok=True)
    # Project root is third parent, ../../../.
    project_path = Path(__file__).parents[3]
    # git clone this project
    output = subprocess.check_output(
        f"git clone file://{project_path.resolve()} git_project",
        cwd=project_dir.parent,
        shell=True
    )
    assert output is not None
    as_conf.load_workflow_commit()
    if is_owner:
        assert "WORKFLOW_COMMIT" in as_conf.experiment_data["AUTOSUBMIT"]
        assert len(as_conf.experiment_data["AUTOSUBMIT"]["WORKFLOW_COMMIT"]) > 0
    else:
        assert "WORKFLOW_COMMIT" not in as_conf.experiment_data["AUTOSUBMIT"]
