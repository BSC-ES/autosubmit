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

"""Integration test for ``Autosubmit._copy_code``.

TODO: This test probably belongs somewhere else -- we will know once we split
      ``autosubmit/autosubmit.py`` into smaller parts.
"""

from pathlib import Path
from shutil import rmtree
from subprocess import CalledProcessError

import pytest

from autosubmit.log.log import AutosubmitCritical

_EXPID = 't000'


def test_copy_code_local_project_destination_is_file(autosubmit_exp):
    """Test that Autosubmit fails to copy the project, as the destination is an existing file."""
    existing_file = str(Path(__file__).resolve())
    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit_exp(_EXPID, experiment_data={
            'PROJECT': {
                'PROJECT_TYPE': 'LOCAL',
                'PROJECT_DESTINATION': 'local_project'
            },
            'LOCAL': {
                'PROJECT_PATH': existing_file
            }
        })

    assert 'Local project path is not a valid' in str(cm.value.message)
    assert existing_file in str(cm.value.message)


def test_copy_code_local_project_destination_is_not_specified(autosubmit_exp):
    """Test that Autosubmit fails to copy the project, as the destination is an existing file."""
    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit_exp(_EXPID, experiment_data={
            'PROJECT': {
                'PROJECT_TYPE': 'LOCAL',
                'PROJECT_DESTINATION': 'local_project'
            },
            'LOCAL': {
                'PROJECT_PATH': None
            }
        })

    assert 'LOCAL.PROJECT_PATH must exists' in str(cm.value.message)


def test_copy_code_local_project_destination_is_an_empty_string(autosubmit_exp):
    """Test that Autosubmit fails to copy the project, as the destination is an existing file."""
    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit_exp(_EXPID, experiment_data={
            'PROJECT': {
                'PROJECT_TYPE': 'LOCAL',
                'PROJECT_DESTINATION': 'local_project'
            },
            'LOCAL': {
                'PROJECT_PATH': ''
            }
        })

    assert 'Empty project path!' in str(cm.value.message)


def test_copy_code_local_project_local_destination_does_not_exist(autosubmit_exp, tmp_path):
    """Test that Autosubmit copies when the local destination does not exist."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    with (project_path / 'ROBOTS.txt') as f:
        f.write_text('test')
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'LOCAL',
            'PROJECT_DESTINATION': 'local_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        }
    })

    exp_project_path = Path(
        exp.as_conf.basic_config.LOCAL_ROOT_DIR,
        exp.expid,
        exp.as_conf.basic_config.LOCAL_PROJ_DIR)
    project_destination = exp.as_conf.get_project_destination()
    local_destination = exp_project_path / project_destination

    # ``autosubmit_exp`` will call create, so we create a new file.
    with (project_path / 'AGENTS.txt') as f:
        f.write_text('noop')

    assert Path(local_destination, 'ROBOTS.txt').exists()
    assert not Path(local_destination, 'AGENTS.txt').exists()

    rmtree(local_destination)

    exp.autosubmit.create(_EXPID, noplot=True, hide=True)

    assert Path(local_destination, 'AGENTS.txt').exists()


def test_copy_code_local_project_cp_error(autosubmit_exp, tmp_path, mocker):
    """Test that we catch errors when ``cp`` fails."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    with (project_path / 'ROBOTS.txt') as f:
        f.write_text('test')

    mocker.patch(
        'autosubmit.autosubmit.subprocess.check_output', side_effect=CalledProcessError(1, 'test')
    )

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit_exp(_EXPID, experiment_data={
            'PROJECT': {
                'PROJECT_TYPE': 'LOCAL',
                'PROJECT_DESTINATION': 'local_project'
            },
            'LOCAL': {
                'PROJECT_PATH': str(project_path)
            }
        })

    assert 'Cannot copy' in str(cm.value.message)

    # Failing to copy the contents, results in the proj folder being deleted for a fresh try.
    proj_dir = Path(tmp_path, _EXPID, 'proj')
    assert proj_dir.exists
    assert not Path(proj_dir, 'local_project').exists()


def test_copy_code_local_project_local_destination_exists_force(autosubmit_exp, tmp_path, mocker):
    """Test that Autosubmit syncs existing directories when force is enabled."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    with (project_path / 'ROBOTS.txt') as f:
        f.write_text('test')
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'LOCAL',
            'PROJECT_DESTINATION': 'local_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        }
    })

    exp_project_path = Path(
        exp.as_conf.basic_config.LOCAL_ROOT_DIR,
        exp.expid,
        exp.as_conf.basic_config.LOCAL_PROJ_DIR)
    project_destination = exp.as_conf.get_project_destination()
    local_destination = exp_project_path / project_destination

    # ``autosubmit_exp`` will call create, so we create a new file.
    with (project_path / 'AGENTS.txt') as f:
        f.write_text('noop')

    assert Path(local_destination, 'ROBOTS.txt').exists()
    assert not Path(local_destination, 'AGENTS.txt').exists()

    # Create does not force the sync.
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    exp.autosubmit.create(_EXPID, noplot=True, hide=True)
    assert not Path(local_destination, 'AGENTS.txt').exists()
    # And since the folder already exists, we should have informed the user nothing was synced.
    # There will be a few calls to ``Log.info``, but here we confirm that at least one is right.
    assert any(['will not sync' in call[0][0] for call in mocked_log.info.call_args_list])

    # Refresh does.
    exp.autosubmit.refresh(_EXPID, None, None)  # type: ignore
    assert Path(local_destination, 'AGENTS.txt').exists()


def test_copy_code_local_project_rsync_error(autosubmit_exp, tmp_path, mocker):
    """Test that catch errors when ``rsync`` fails.."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    with (project_path / 'ROBOTS.txt') as f:
        f.write_text('test')
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'LOCAL',
            'PROJECT_DESTINATION': 'local_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        }
    })

    exp_project_path = Path(
        exp.as_conf.basic_config.LOCAL_ROOT_DIR,
        exp.expid,
        exp.as_conf.basic_config.LOCAL_PROJ_DIR)
    project_destination = exp.as_conf.get_project_destination()
    local_destination = exp_project_path / project_destination

    mocker.patch(
        'autosubmit.autosubmit.subprocess.call', side_effect=CalledProcessError(1, 'test')
    )

    with pytest.raises(AutosubmitCritical) as cm:
        exp.autosubmit.refresh(_EXPID, None, None)  # type: ignore

    assert not Path(local_destination, 'AGENTS.txt').exists()
    assert 'Cannot rsync' in str(cm.value.message)

    # Failing to rsync the contents, the proj folder is left as-is.
    proj_dir = Path(tmp_path, _EXPID, 'proj')
    assert proj_dir.exists
    assert Path(proj_dir, 'local_project').exists()
    assert Path(proj_dir, 'local_project', 'ROBOTS.txt').exists()
