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

import datetime
import json
import tempfile
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import TemporaryDirectory
from typing import Callable

import pytest
from mock import Mock, patch
from rocrate.rocrate import File
from rocrate.rocrate import ROCrate
from ruamel.yaml import YAML
from ruamel.yaml.representer import RepresenterError

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.provenance.rocrate import (
    _add_dir_and_files,
    _get_action_status,
    _create_formal_parameter,
    _create_parameter,
    _get_project_entity,
    _get_git_branch_and_commit,
    create_rocrate_archive
)
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical

"""Tests for the RO-Crate generation in Autosubmit."""

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
def as_conf() -> AutosubmitConfig:
    as_conf = Mock(spec=AutosubmitConfig)
    as_conf.get_project_dir = Mock(return_value=_PROJECT_PATH)
    return as_conf


@pytest.fixture
def create_conf_dir() -> Callable:
    def _fn(parent, as_conf):
        conf_dir = Path(parent, 'conf')
        conf_dir.mkdir(exist_ok=True)
        Path(conf_dir, 'metadata').mkdir()
        unified_config = Path(conf_dir, 'metadata/experiment_data.yml')
        unified_config.touch()
        yaml = YAML(typ='rt')
        with open(unified_config, 'w') as f:
            yaml.dump(dict(as_conf.experiment_data), f)
        as_conf.current_loaded_files = {unified_config: 0}

    return _fn


def test_add_dir_and_files_empty_folder(empty_rocrate: ROCrate):
    with TemporaryDirectory() as d:
        _add_dir_and_files(
            crate=empty_rocrate,
            base_path=d,
            relative_path=d,
            encoding_format=None
        )
    assert 1 == len(empty_rocrate.data_entities)


def test_add_dir_and_files(empty_rocrate: ROCrate):
    with TemporaryDirectory() as d:
        sub_path = Path(d, 'files')
        sub_path.mkdir(parents=True)
        with open(sub_path / 'file.txt', 'w+') as f:
            f.write('hello')
            f.flush()

            _add_dir_and_files(
                crate=empty_rocrate,
                base_path=d,
                relative_path=str(sub_path),
                encoding_format=None
            )
    assert 2 == len(empty_rocrate.data_entities)
    for entity in empty_rocrate.data_entities:
        if entity.source.name == 'file.txt':
            properties = entity.properties()
            assert properties['sdDatePublished']
            assert properties['dateModified']
            assert properties['encodingFormat'] == 'text/plain'
            break
    else:
        pytest.fail('Failed to locate the entity for files/file.txt')


def test_add_dir_and_files_set_encoding(empty_rocrate: ROCrate):
    encoding = 'image/jpeg'
    with TemporaryDirectory() as _:
        with TemporaryDirectory() as d:
            sub_path = Path(d, 'files')
            sub_path.mkdir(parents=True)
            with open(sub_path / 'file.txt', 'w+') as f:
                f.write('hello')
                f.flush()

                _add_dir_and_files(
                    crate=empty_rocrate,
                    base_path=d,
                    relative_path=str(sub_path),
                    encoding_format=encoding
                )
        assert 2 == len(empty_rocrate.data_entities)
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


@patch('subprocess.check_output')
def test_get_git_branch_and_commit(mocked_check_output: Mock):
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


@patch('autosubmit.provenance.rocrate.BasicConfig')
@patch('autosubmit.provenance.rocrate.get_experiment_descrip')
@patch('autosubmit.provenance.rocrate.get_autosubmit_version')
def test_rocrate(
        mocked_get_autosubmit_version: Mock,
        mocked_get_experiment_descrip: Mock,
        mocked_BasicConfig: Mock,
        as_conf: AutosubmitConfig,
        empty_rocrate: ROCrate,
        create_conf_dir: Callable):
    with tempfile.TemporaryDirectory() as temp_dir:
        mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, _EXPID)
        experiment_path.mkdir()
        mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        project_path = Path(experiment_path, 'proj')
        project_path.mkdir()
        # some outputs
        for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
            Path(project_path, output_file).touch()
        # required paths for AS
        for other_required_path in ['conf', 'pkl', 'plot', 'status']:
            Path(experiment_path, other_required_path).mkdir()
        as_conf.experiment_data = {
            'DEFAULT': {
                'EXPID': _EXPID
            },
            'EXPERIMENT': {},
            'CONFIG': {
                'PRE': [
                    '%PROJ%/conf/bootstrap/include.yml'
                ]
            },
            'ROOTDIR': str(experiment_path),
            'PROJECT': {
                'PROJECT_DESTINATION': '',
                'PROJECT_TYPE': 'LOCAL'
            },
            'LOCAL': {
                'PROJECT_PATH': str(project_path)
            },
            'APP': {
                'INPUT_1': 1,
                'INPUT_2': 2
            }
        }
        rocrate_json = {
            'INPUTS': ['APP'],
            'OUTPUTS': [
                'graph_*.gif'
            ],
            'PATCH': json.dumps({
                '@graph': [
                    {
                        '@id': './',
                        "license": "Apache-2.0"
                    }
                ]
            })
        }
        create_conf_dir(experiment_path, as_conf)
        jobs = []
        start_time = ''
        end_time = ''

        mocked_get_autosubmit_version.return_value = '4.0.0b0'
        mocked_get_experiment_descrip.return_value = [
            ['mocked test project']
        ]

        crate = create_rocrate_archive(
            as_conf=as_conf,
            rocrate_json=rocrate_json,
            jobs=jobs,
            start_time=start_time,
            end_time=end_time,
            path=Path(temp_dir)
        )
        assert crate is not None


@patch('autosubmit.provenance.rocrate._get_project_entity')
@patch('autosubmit.provenance.rocrate.BasicConfig')
@patch('autosubmit.provenance.rocrate.get_experiment_descrip')
@patch('autosubmit.provenance.rocrate.get_autosubmit_version')
def test_rocrate_invalid_project(
        mocked_get_autosubmit_version: Mock,
        mocked_get_experiment_descrip: Mock,
        mocked_BasicConfig: Mock,
        mocked_get_project_entity: Mock,
        as_conf: AutosubmitConfig,
        create_conf_dir: Callable):
    mocked_get_project_entity.side_effect = ValueError
    with tempfile.TemporaryDirectory() as temp_dir:
        mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, _EXPID)
        experiment_path.mkdir()
        mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        project_path = Path(experiment_path, 'proj')
        project_path.mkdir()
        # some outputs
        for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
            Path(project_path, output_file).touch()
        # required paths for AS
        for other_required_path in ['conf', 'pkl', 'plot', 'status']:
            Path(experiment_path, other_required_path).mkdir()
        as_conf.experiment_data = {
            'DEFAULT': {
                'EXPID': _EXPID
            },
            'EXPERIMENT': {},
            'CONFIG': {},
            'ROOTDIR': str(experiment_path),
            'PROJECT': {
                'PROJECT_DESTINATION': '',
                'PROJECT_TYPE': 'GIT'
            },
            'GIT': {
                'PROJECT_PATH': str(project_path),
                'PROJECT_ORIGIN': _PROJECT_URL
            }
        }
        rocrate_json = {}
        create_conf_dir(experiment_path, as_conf)
        jobs = []

        mocked_get_autosubmit_version.return_value = '4.0.0b0'
        mocked_get_experiment_descrip.return_value = [
            ['mocked test project']
        ]

        with pytest.raises(AutosubmitCritical) as cm:
            create_rocrate_archive(
                as_conf=as_conf,
                rocrate_json=rocrate_json,
                jobs=jobs,
                start_time=None,
                end_time=None,
                path=Path(temp_dir)
            )

        assert cm.value.message == 'Failed to read the Autosubmit Project for RO-Crate...'


@patch('autosubmit.provenance.rocrate.BasicConfig')
@patch('autosubmit.provenance.rocrate.get_experiment_descrip')
@patch('autosubmit.provenance.rocrate.get_autosubmit_version')
def test_rocrate_invalid_parameter_type(
        mocked_get_autosubmit_version: Mock,
        mocked_get_experiment_descrip: Mock,
        mocked_BasicConfig: Mock,
        as_conf: AutosubmitConfig,
        create_conf_dir: Callable):
    """NOTE: This is not possible at the moment, as we are using ruamel.yaml
    to parse the YAML, and we are not supporting objects. But you never know
    what the code will do in the future, so we just make sure we fail nicely."""
    with tempfile.TemporaryDirectory() as temp_dir:
        mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, _EXPID)
        experiment_path.mkdir()
        mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        project_path = Path(experiment_path, 'proj')
        project_path.mkdir()
        # some outputs
        for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
            Path(project_path, output_file).touch()
        # required paths for AS
        for other_required_path in ['conf', 'pkl', 'plot', 'status']:
            Path(experiment_path, other_required_path).mkdir()
        as_conf.experiment_data = {
            'DEFAULT': {
                'EXPID': _EXPID
            },
            'EXPERIMENT': {},
            'CONFIG': {},
            'ROOTDIR': str(experiment_path),
            'PROJECT': {
                'PROJECT_DESTINATION': '',
                'PROJECT_TYPE': 'GIT'
            },
            'GIT': {
                'PROJECT_PATH': str(project_path),
                'PROJECT_ORIGIN': _PROJECT_URL
            },
            'APP': {
                'OBJ': object()
            }
        }

        mocked_get_autosubmit_version.return_value = '4.0.0b0'
        mocked_get_experiment_descrip.return_value = [
            ['mocked test project']
        ]

        with pytest.raises(RepresenterError) as cm:
            create_conf_dir(experiment_path, as_conf)

        assert 'cannot represent an object' in str(cm.value)


@patch('autosubmit.autosubmit.Log')
@patch('autosubmit.autosubmit.AutosubmitConfig')
def test_rocrate_main_fail_missing_rocrate(
        mocked_AutosubmitConfig: Mock,
        mocked_Log: Mock):
    mocked_as_conf = Mock(autospec=AutosubmitConfig)
    mocked_as_conf.experiment_data = {}
    mocked_AutosubmitConfig.return_value = mocked_as_conf

    mocked_Log.error = Mock()
    mocked_Log.error.return_value = ''

    autosubmit = Autosubmit()
    with pytest.raises(AutosubmitCritical) as cm, tempfile.TemporaryDirectory() as temp_dir:
        autosubmit.rocrate(_EXPID, path=Path(path=Path(temp_dir)))

    assert cm.value.message == 'You must provide an ROCRATE configuration key when using RO-Crate...'
    assert mocked_Log.error.call_count == 1


@patch('autosubmit.autosubmit.JobList')
@patch('autosubmit.autosubmit.AutosubmitConfig')
@patch('autosubmit.provenance.rocrate.BasicConfig')
@patch('autosubmit.provenance.rocrate.get_experiment_descrip')
@patch('autosubmit.provenance.rocrate.get_autosubmit_version')
def test_rocrate_main(
        mocked_get_autosubmit_version: Mock,
        mocked_get_experiment_descrip: Mock,
        mocked_BasicConfig: Mock,
        mocked_AutosubmitConfig: Mock,
        mocked_JobList: Mock,
        create_conf_dir: Callable):
    with tempfile.TemporaryDirectory() as temp_dir:
        mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, _EXPID)
        experiment_path.mkdir()
        mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        project_path = Path(experiment_path, 'proj')
        project_path.mkdir()
        # some outputs
        for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
            Path(project_path, output_file).touch()
        # required paths for AS
        for other_required_path in ['conf', 'pkl', 'plot', 'status']:
            Path(experiment_path, other_required_path).mkdir()
        mocked_as_conf = Mock(autospec=AutosubmitConfig)
        mocked_AutosubmitConfig.return_value = mocked_as_conf
        mocked_as_conf.experiment_data = {
            'DEFAULT': {
                'EXPID': _EXPID
            },
            'EXPERIMENT': {},
            'CONFIG': {},
            'ROOTDIR': str(experiment_path),
            'PROJECT': {
                'PROJECT_DESTINATION': '',
                'PROJECT_TYPE': 'LOCAL'
            },
            'LOCAL': {
                'PROJECT_PATH': str(project_path)
            },
            'APP': {
                'INPUT_1': 1,
                'INPUT_2': 2
            },
            'ROCRATE': {
                'INPUTS': ['APP'],
                'OUTPUTS': [
                    'graph_*.gif'
                ],
                'PATCH': json.dumps({
                    '@graph': [
                        {
                            '@id': './',
                            "license": "Apache-2.0"
                        }
                    ]
                })
            }
        }
        create_conf_dir(experiment_path, as_conf=mocked_as_conf)
        mocked_as_conf.get_storage_type.return_value = 'pkl'
        mocked_as_conf.get_date_list.return_value = []

        mocked_get_autosubmit_version.return_value = '4.0.0b0'
        mocked_get_experiment_descrip.return_value = [
            ['mocked test project']
        ]

        mocked_job_list = Mock()
        mocked_JobList.return_value = mocked_job_list

        job1 = Mock(autospec=Job)
        job1_submit_time = datetime.datetime.strptime("21/11/06 16:30", "%d/%m/%y %H:%M")
        job1_start_time = datetime.datetime.strptime("21/11/06 16:40", "%d/%m/%y %H:%M")
        job1_finished_time = datetime.datetime.strptime("21/11/06 16:50", "%d/%m/%y %H:%M")
        job1.get_last_retrials.return_value = [
            [job1_submit_time, job1_start_time, job1_finished_time, 'COMPLETED']]
        job1.name = 'job1'
        job1.date = '2006'
        job1.member = 'fc0'
        job1.section = 'JOB'
        job1.chunk = '1'
        job1.processors = '1'

        job2 = Mock(autospec=Job)
        job2_submit_time = datetime.datetime.strptime("21/11/06 16:40", "%d/%m/%y %H:%M")
        job2_start_time = datetime.datetime.strptime("21/11/06 16:50", "%d/%m/%y %H:%M")
        job2_finished_time = datetime.datetime.strptime("21/11/06 17:00", "%d/%m/%y %H:%M")
        job2.get_last_retrials.return_value = [
            [job2_submit_time, job2_start_time, job2_finished_time, 'COMPLETED']]
        job2.name = 'job2'
        job2.date = '2006'
        job2.member = 'fc1'
        job2.section = 'JOB'
        job2.chunk = '1'
        job2.processors = '1'

        mocked_job_list.get_job_list.return_value = [job1, job2]
        mocked_job_list.get_ready.return_value = []  # Mock due the new addition in the job_list.load()
        mocked_job_list.get_waiting.return_value = []  # Mocked due the new addition in the job_list.load()
        autosubmit = Autosubmit()
        r = autosubmit.rocrate(_EXPID, path=Path(temp_dir))
        assert r


@patch('autosubmit.provenance.rocrate.BasicConfig')
@patch('autosubmit.provenance.rocrate.get_experiment_descrip')
@patch('autosubmit.provenance.rocrate.get_autosubmit_version')
def test_custom_config_loaded_file(
        mocked_get_autosubmit_version: Mock,
        mocked_get_experiment_descrip: Mock,
        mocked_BasicConfig: Mock,
        as_conf: AutosubmitConfig,
        create_conf_dir: Callable):
    with tempfile.TemporaryDirectory() as temp_dir:
        mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, _EXPID)
        experiment_path.mkdir()
        mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        project_path = Path(experiment_path, 'proj')
        project_path.mkdir()
        # required paths for AS
        for other_required_path in ['conf', 'pkl', 'plot', 'status']:
            Path(experiment_path, other_required_path).mkdir()

        # custom config file
        project_conf = Path(project_path, 'conf')
        project_conf.mkdir()
        custom_config = Path(project_conf, 'include.yml')
        custom_config.touch()
        custom_config.write_text('CUSTOM_CONFIG_LOADED: True')

        as_conf.experiment_data = {
            'DEFAULT': {
                'EXPID': _EXPID
            },
            'EXPERIMENT': {},
            'CONFIG': {
                'PRE': [
                    str(project_conf)
                ]
            },
            'ROOTDIR': str(experiment_path),
            'PROJECT': {
                'PROJECT_DESTINATION': '',
                'PROJECT_TYPE': 'LOCAL'
            },
            'LOCAL': {
                'PROJECT_PATH': str(project_path)
            },
            'APP': {
                'INPUT_1': 1,
                'INPUT_2': 2
            }
        }
        rocrate_json = {
            'INPUTS': ['APP'],
            'OUTPUTS': [
                'graph_*.gif'
            ],
            'PATCH': json.dumps({
                '@graph': [
                    {
                        '@id': './',
                        "license": "Apache-2.0"
                    }
                ]
            })
        }
        create_conf_dir(experiment_path, as_conf)
        # adding both directory and file to the list of loaded files
        as_conf.current_loaded_files[str(project_conf)] = 0
        as_conf.current_loaded_files[str(custom_config)] = 0
        jobs = []
        start_time = ''
        end_time = ''

        mocked_get_autosubmit_version.return_value = '4.0.0b0'
        mocked_get_experiment_descrip.return_value = [
            ['mocked test project']
        ]

        crate = create_rocrate_archive(
            as_conf=as_conf,
            rocrate_json=rocrate_json,
            jobs=jobs,
            start_time=start_time,
            end_time=end_time,
            path=Path(temp_dir)
        )
        assert crate is not None
        data_entities_ids = [data_entity['@id'] for data_entity in crate.data_entities]
        assert File(crate, f'file://{str(project_conf)}/').id in data_entities_ids
        assert File(crate, f'file://{str(custom_config)}').id in data_entities_ids


@patch('autosubmit.provenance.rocrate.BasicConfig')
@patch('autosubmit.provenance.rocrate.get_experiment_descrip')
@patch('autosubmit.provenance.rocrate.get_autosubmit_version')
def test_no_duplicate_ids(
        mocked_get_autosubmit_version: Mock,
        mocked_get_experiment_descrip: Mock,
        mocked_BasicConfig: Mock,
        as_conf: AutosubmitConfig,
        create_conf_dir: Callable):
    with tempfile.TemporaryDirectory() as temp_dir:
        mocked_BasicConfig.LOCAL_ROOT_DIR = temp_dir
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        experiment_path = Path(mocked_BasicConfig.LOCAL_ROOT_DIR, _EXPID)
        experiment_path.mkdir()
        mocked_BasicConfig.LOCAL_TMP_DIR = Path(experiment_path, 'tmp')
        mocked_BasicConfig.LOCAL_TMP_DIR.mkdir()
        project_path = Path(experiment_path, 'proj')
        project_path.mkdir()
        # required paths for AS
        for other_required_path in ['conf', 'pkl', 'plot', 'status']:
            Path(experiment_path, other_required_path).mkdir()

        # custom config file
        project_conf = Path(project_path, 'conf')
        project_conf.mkdir()
        custom_config = Path(project_conf, 'include.yml')
        custom_config.touch()
        custom_config.write_text('CUSTOM_CONFIG_LOADED: True')

        as_conf.experiment_data = {
            'DEFAULT': {
                'EXPID': _EXPID
            },
            'EXPERIMENT': {},
            'CONFIG': {
                'PRE': [
                    str(project_conf)
                ]
            },
            'ROOTDIR': str(experiment_path),
            'PROJECT': {
                'PROJECT_DESTINATION': '',
                'PROJECT_TYPE': 'LOCAL'
            },
            'LOCAL': {
                'PROJECT_PATH': str(project_path)
            },
            'APP': {
                'INPUT_1': 1,
                'INPUT_2': 2
            }
        }
        rocrate_json = {
            'INPUTS': ['APP'],
            'OUTPUTS': [
                'graph_*.gif'
            ],
            'PATCH': json.dumps({
                '@graph': [
                    {
                        '@id': './',
                        "license": "Apache-2.0"
                    }
                ]
            })
        }
        create_conf_dir(experiment_path, as_conf)
        # adding both directory and file to the list of loaded files
        as_conf.current_loaded_files[str(project_conf)] = 0
        as_conf.current_loaded_files[str(custom_config)] = 0
        jobs = []
        start_time = ''
        end_time = ''

        mocked_get_autosubmit_version.return_value = '4.0.0b0'
        mocked_get_experiment_descrip.return_value = [
            ['mocked test project']
        ]

        crate = create_rocrate_archive(
            as_conf=as_conf,
            rocrate_json=rocrate_json,
            jobs=jobs,
            start_time=start_time,
            end_time=end_time,
            path=Path(temp_dir)
        )
        assert crate is not None
        data_entities_ids = [data_entity['@id'] for data_entity in crate.data_entities]
        assert len(data_entities_ids) == len(
            set(data_entities_ids)), f'Duplicate IDs found in the RO-Crate data entities: {str(data_entities_ids)}'
