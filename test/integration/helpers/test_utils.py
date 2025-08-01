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

from contextlib import nullcontext as does_not_raise
from pathlib import Path
from shutil import rmtree

import pytest

from autosubmit.helpers.utils import check_jobs_file_exists
from autosubmit.log.log import AutosubmitCritical

_EXPID = 't000'


def test_check_jobs_file_exists_dummy(autosubmit_exp):
    """Test that we ignore completely if the project is dummy."""
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'none'
        }
    })

    with does_not_raise():
        check_jobs_file_exists(exp.as_conf, None)


def test_check_jobs_file_exists_missing_templates_dir(autosubmit_exp, tmp_path):
    """Test that we raise an error if the template directory is missing."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    Path(project_path, 'dummy.sh').touch()
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'local',
            'PROJECT_DESTINATION': 'git_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'FILE': 'test.sh'
            }
        }
    })

    templates_dir = Path(exp.as_conf.get_project_dir())
    rmtree(templates_dir)
    assert not templates_dir.exists()

    with pytest.raises(AutosubmitCritical) as cm:
        check_jobs_file_exists(exp.as_conf, None)

    assert 'does not exist' in str(cm.value.message)


def test_check_jobs_file_exists_template_dir_is_file(autosubmit_exp, tmp_path):
    """Test that we raise an error if the template directory is actually a file."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    Path(project_path, 'dummy.sh').touch()
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'local',
            'PROJECT_DESTINATION': 'git_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'FILE': 'test.sh'
            }
        }
    })

    templates_dir = Path(exp.as_conf.get_project_dir())
    # Remove and make it a file
    rmtree(templates_dir)
    templates_dir.parent.mkdir(exist_ok=True)
    templates_dir.touch()
    assert templates_dir.is_file()

    with pytest.raises(AutosubmitCritical) as cm:
        check_jobs_file_exists(exp.as_conf, None)

    assert 'is not a directory' in str(cm.value.message)


def test_check_jobs_file_exists_section_does_not_exist(autosubmit_exp, tmp_path):
    """Test that the ``check_jobs_file_exists`` function ignores a non-existent section."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    Path(project_path, 'dummy.sh').touch()
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'local',
            'PROJECT_DESTINATION': 'git_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'FILE': 'dummy.sh'
            }
        }
    })

    with does_not_raise():
        check_jobs_file_exists(exp.as_conf, 'B')


def test_check_jobs_file_exists_missing_script(autosubmit_exp, tmp_path):
    """Test that the ``check_jobs_file_exists`` function ignores a non-existent section."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    Path(project_path, 'dummy.sh').touch()
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'local',
            'PROJECT_DESTINATION': 'git_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'FILE': 'DOES_NOT_EXIST.sh'
            }
        }
    })

    with pytest.raises(AutosubmitCritical) as cm:
        check_jobs_file_exists(exp.as_conf)

    assert 'Templates not found' in str(cm.value.message)
    assert 'DOES_NOT_EXIST.sh' in str(cm.value.message)


def test_check_jobs_file_exists_all_good(autosubmit_exp, tmp_path):
    """Success path."""
    project_path = tmp_path / 'project'
    project_path.mkdir()
    Path(project_path, 'dummy.sh').touch()
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'local',
            'PROJECT_DESTINATION': 'git_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'FILE': 'dummy.sh'
            }
        }
    })

    with does_not_raise():
        check_jobs_file_exists(exp.as_conf)
