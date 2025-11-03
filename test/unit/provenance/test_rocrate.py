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

"""Tests for the RO-Crate generation in Autosubmit."""

from pathlib import Path
from subprocess import CalledProcessError

import pytest
from rocrate.rocrate import ROCrate  # type: ignore

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical
# noinspection PyProtectedMember
from autosubmit.provenance.rocrate import (
    _add_files,
    _get_action_status,
    _create_formal_parameter,
    _create_parameter,
    _get_project_entity,
    _get_git_branch_and_commit
)

_EXPID = 'zzzz'
"""Experiment ID used in all the tests."""
_PROJECT_URL = 'https://earth.bsc.es/gitlab/es/autosubmit.git'
"""Project URL used in all the tests. This is not actually cloned."""
_PROJECT_PATH = str(Path(__file__).parent.joinpath('../../../'))
"""We pretend the source code folder is the project path."""


@pytest.fixture
def empty_rocrate() -> ROCrate:
    return ROCrate()


@pytest.fixture
def as_conf(mocker) -> AutosubmitConfig:
    as_conf = mocker.Mock(spec=AutosubmitConfig)
    as_conf.get_project_dir = mocker.Mock(return_value=_PROJECT_PATH)
    return as_conf


def test_add_files_empty_folder(empty_rocrate: ROCrate, tmp_path):
    _add_files(empty_rocrate, tmp_path, '.', expid=_EXPID)
    assert 1 == len(empty_rocrate.data_entities)


def test_add_files(empty_rocrate: ROCrate, tmp_path):
    sub_path = Path(tmp_path, 'files')
    sub_path.mkdir(parents=True)
    file = sub_path / 'file.txt'
    file.touch()
    file.write_text('hello')
    _add_files(
        crate=empty_rocrate,
        base_path=Path(tmp_path),
        relative_path=str(sub_path),
        expid=_EXPID
    )
    assert 1 == len(empty_rocrate.data_entities)
    empty_rocrate.write_zip(tmp_path / 'file.zip')

    for entity in empty_rocrate.data_entities:
        if entity.source.name == 'file.txt':
            properties = entity.properties()
            assert properties['sdDatePublished']
            assert properties['dateModified']
            assert properties['encodingFormat'] == 'text/plain'
            break
    else:
        pytest.fail('Failed to locate the entity for files/file.txt')


def test_add_files_set_encoding(empty_rocrate: ROCrate, tmp_path):
    encoding = 'image/jpeg'
    files_dir = tmp_path / 'files'
    files_dir.mkdir(parents=True)

    file = files_dir / 'file.txt'
    file.touch()
    file.write_text('hello')

    _add_files(
        crate=empty_rocrate,
        base_path=tmp_path,
        relative_path=str(files_dir),
        encoding_format=encoding,
        expid=_EXPID
    )
    assert 1 == len(empty_rocrate.data_entities)

    for entity in empty_rocrate.data_entities:
        if entity.source.name == 'file.txt':
            properties = entity.properties()
            assert properties['sdDatePublished']
            assert properties['dateModified']
            assert properties['encodingFormat'] == encoding
            break
    else:
        pytest.fail('Failed to locate the entity for files/file.txt')


def test_get_action_status():
    for tests in [
        ([], 'PotentialActionStatus'),
        ([Job('a', 'a', Status.FAILED, 1), Job('b', 'b', Status.COMPLETED, 1)], 'FailedActionStatus'),
        ([Job('a', 'a', Status.COMPLETED, 1), Job('b', 'b', Status.COMPLETED, 1)], 'CompletedActionStatus'),
        ([Job('a', 'a', Status.DELAYED, 1)], 'PotentialActionStatus')
    ]:
        jobs = tests[0]
        expected = tests[1]
        result = _get_action_status(jobs)
        assert expected == result


def test_create_formal_parameter(empty_rocrate: ROCrate):
    formal_parameter = _create_formal_parameter(empty_rocrate, 'Name')
    properties = formal_parameter.properties()
    assert '#Name-param' == properties['@id']
    assert 'FormalParameter' == properties['@type']
    assert 'Name' == properties['name']


def test_create_parameter(empty_rocrate: ROCrate):
    formal_parameter = _create_formal_parameter(empty_rocrate, 'Answer')
    parameter = _create_parameter(
        empty_rocrate,
        'Answer',
        42,
        formal_parameter,
        'PropertyValue',
        extra='test'
    )
    properties = parameter.properties()
    assert 42 == properties['value']
    assert 'test' == properties['extra']


def test_get_local_project_entity(as_conf: AutosubmitConfig, empty_rocrate: ROCrate):
    project_path = '/tmp/project'
    project_url = f'file://{project_path}'
    as_conf.experiment_data = {
        'PROJECT': {
            'PROJECT_TYPE': 'LOCAL'
        },
        'LOCAL': {
            'PROJECT_PATH': project_path
        }
    }
    project_entity = _get_project_entity(
        as_conf,
        empty_rocrate
    )

    assert project_entity['@id'] == project_url
    assert project_entity['targetProduct'] == 'Autosubmit'
    assert project_entity['codeRepository'] == project_url
    assert project_entity['version'] == ''


def test_get_dummy_project_entity(as_conf: AutosubmitConfig, empty_rocrate: ROCrate):
    project_url = ''
    as_conf.experiment_data = {
        'PROJECT': {
            'PROJECT_TYPE': 'NONE'
        }
    }
    project_entity = _get_project_entity(
        as_conf,
        empty_rocrate
    )

    assert project_entity['@id'] == project_url
    assert project_entity['targetProduct'] == 'Autosubmit'
    assert project_entity['codeRepository'] == project_url
    assert project_entity['version'] == ''


def test_get_subversion_or_other_project_entity(as_conf: AutosubmitConfig, empty_rocrate: ROCrate):
    for key in ['SVN', 'SUBVERSION', 'MERCURY', '', ' ']:
        as_conf.experiment_data = {
            'PROJECT': {
                'PROJECT_TYPE': key
            },
            key: {
                'PROJECT_PATH': ''
            }
        }
        with pytest.raises(AutosubmitCritical):
            _get_project_entity(
                as_conf,
                empty_rocrate
            )


def test_get_git_project_entity(as_conf: AutosubmitConfig, empty_rocrate: ROCrate):
    """Test that we add the project and its data correctly into the metadata."""
    as_conf.experiment_data = {
        'PROJECT': {
            'PROJECT_TYPE': 'GIT'
        },
        'GIT': {
            'PROJECT_PATH': _PROJECT_PATH,
            'PROJECT_ORIGIN': _PROJECT_URL
        }
    }
    project_entity = _get_project_entity(
        as_conf,
        empty_rocrate
    )
    assert project_entity['@id'] == _PROJECT_URL
    assert project_entity['targetProduct'] == 'Autosubmit'
    assert project_entity['codeRepository'] == _PROJECT_URL
    assert len(project_entity['version']) > 0


def test_get_git_branch_and_commit(mocker):
    """Test the RO-Crate functions to fetch Git information."""
    mocked_check_output = mocker.patch('subprocess.check_output')
    error = CalledProcessError(1, '')
    mocked_check_output.side_effect = [error]
    with pytest.raises(AutosubmitCritical) as cm:
        _get_git_branch_and_commit(project_path='')

    assert cm.value.message == 'Failed to retrieve project branch...'

    mocked_check_output.reset_mock()
    mocked_check_output.side_effect = ['master', error]
    with pytest.raises(AutosubmitCritical) as cm:
        _get_git_branch_and_commit(project_path='')

    assert cm.value.message == 'Failed to retrieve project commit SHA...'


def test_rocrate_main_fail_missing_rocrate(mocker, tmp_path):
    mocked_as_conf = mocker.Mock(autospec=AutosubmitConfig)
    mocked_as_conf.experiment_data = {}
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    mocked_autosubmit_config = mocker.patch('autosubmit.autosubmit.AutosubmitConfig')
    mocked_autosubmit_config.return_value = mocked_as_conf

    autosubmit = Autosubmit()
    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.rocrate(_EXPID, path=tmp_path)

    assert cm.value.message == 'You must provide an ROCRATE configuration key when using RO-Crate...'
    assert mocked_log.error.call_count == 1
