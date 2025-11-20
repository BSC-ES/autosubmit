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

import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from rocrate.rocrate import File  # type: ignore
from ruamel.yaml.representer import RepresenterError

from autosubmit.log.log import AutosubmitCritical
# noinspection PyProtectedMember
from autosubmit.provenance.rocrate import (
    create_rocrate_archive
)

_EXPID = 'zzzz'
"""Experiment ID used in all the tests."""
_PROJECT_URL = 'https://earth.bsc.es/gitlab/es/autosubmit.git'
"""Project URL used in all the tests. This is not actually cloned."""
_PROJECT_PATH = str(Path(__file__).parent.joinpath('../../../'))
"""We pretend the source code folder is the project path."""


def test_custom_config_loaded_file(autosubmit_exp, tmp_path):
    """Test creating an RO-Crate archive for an AS configuration that contains CUSTOM_CONFIG (PRE)."""
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()

    # This file must be included in the final RO-Crate archive because it's in the Project path
    project_file = project_path / 'graph_1.gif'
    project_file.touch()

    # Loading a custom config out of the experiment or project path
    custom_config = Path(tmp_path, 'include.yml')
    custom_config.touch()
    custom_config.write_text('CUSTOM_CONFIG_LOADED: True')

    project_destination = 'local_project'

    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_DESTINATION': project_destination,
            'PROJECT_TYPE': 'LOCAL'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path)
        },
        'APP': {
            'INPUT_1': 1,
            'INPUT_2': 2
        },
        'DEFAULT': {
            'CUSTOM_CONFIG': {
                'PRE': str(custom_config)
            },
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
    }, include_jobs=True)

    jobs = []
    start_time = ''
    end_time = ''

    rocrate_json = exp.as_conf.experiment_data['ROCRATE']

    crate = create_rocrate_archive(
        as_conf=exp.as_conf,
        rocrate_json=rocrate_json,
        jobs=jobs,
        start_time=start_time,
        end_time=end_time,
        path=Path(tmp_path)
    )
    assert crate is not None
    data_entities_ids = [data_entity['@id'] for data_entity in crate.data_entities]

    # Create the strings for the RO-Crate File object. We will use it later in assert statements.
    experiment_path = exp.exp_path
    experiment_project = Path(experiment_path, 'proj')

    # Here, we must have the file that was copied from the external folder into the exp/proj folder
    project_file_str = Path(experiment_project, project_destination, project_file.name).relative_to(experiment_path)
    assert str(File(crate, project_file_str).source) in data_entities_ids, "Missing project file"

    # Here, we must have the external config file included, as a link (i.e. we do not copy external files)
    custom_config_id = f'file://{str(custom_config)}'
    assert str(File(crate, custom_config_id).source) in data_entities_ids, "Missing external custom config file (PRE)"


def test_rocrate(tmp_path, autosubmit_exp):
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()
    include = Path(project_path, 'conf/bootstrap/include.yml')
    include.parent.mkdir(exist_ok=True, parents=True)
    include.touch()
    for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
        Path(project_path, output_file).touch()

    exp = autosubmit_exp(_EXPID, experiment_data={
        'CONFIG': {
            'PRE': [
                '%PROJ%/conf/bootstrap/include.yml'
            ]
        },
        'PROJECT': {
            'PROJECT_DESTINATION': 'local_project',
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
    }, include_jobs=False)

    as_conf = exp.as_conf
    rocrate_json = exp.as_conf.experiment_data['ROCRATE']

    jobs = []
    start_time = ''
    end_time = ''

    crate = create_rocrate_archive(
        as_conf=as_conf,
        rocrate_json=rocrate_json,
        jobs=jobs,
        start_time=start_time,
        end_time=end_time,
        path=tmp_path
    )
    assert crate is not None


def test_rocrate_invalid_project(autosubmit_exp, tmp_path, mocker):
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()
    # some outputs
    for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
        Path(project_path, output_file).touch()

    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_DESTINATION': 'local_project',
            'PROJECT_TYPE': 'LOCAL'
        },
        'LOCAL': {
            'PROJECT_PATH': str(project_path),
            'PROJECT_ORIGIN': _PROJECT_URL
        },
        'ROCRATE': {
            'INPUTS': [],
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
    })

    as_conf = exp.as_conf
    rocrate_json = as_conf.experiment_data['ROCRATE']
    jobs = []

    mocker.patch('autosubmit.provenance.rocrate._get_project_entity',
                 side_effect=AutosubmitCritical('Failed to read the Autosubmit Project for RO-Crate...'))

    with pytest.raises(AutosubmitCritical) as cm:
        create_rocrate_archive(
            as_conf=as_conf,
            rocrate_json=rocrate_json,
            jobs=jobs,
            start_time=None,
            end_time=None,
            path=tmp_path
        )

    assert cm.value.message == 'Failed to read the Autosubmit Project for RO-Crate...'


def test_rocrate_invalid_parameter_type(autosubmit_exp, tmp_path):
    """NOTE: This is not possible at the moment, as we are using ruamel.yaml
    to parse the YAML, and we are not supporting objects. But you never know
    what the code will do in the future, so we just make sure we fail nicely."""
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()
    for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
        Path(project_path, output_file).touch()
    with pytest.raises(RepresenterError) as cm:
        autosubmit_exp(_EXPID, experiment_data={
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
        })

    assert 'cannot represent an object' in str(cm.value)


def test_no_duplicate_ids(autosubmit_exp, tmp_path):
    """Test that there are no duplicated ID's.

    We must not have duplicate ID's, as they are unique for JSON-LD terms."""
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()

    # custom config file
    project_conf = Path(project_path, 'conf')
    project_conf.mkdir()
    custom_config = Path(project_conf, 'include.yml')
    custom_config.touch()
    custom_config.write_text('CUSTOM_CONFIG_LOADED: True')

    exp = autosubmit_exp(_EXPID, experiment_data={
        'CONFIG': {
            'PRE': [
                str(project_conf)
            ]
        },
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
    })

    as_conf = exp.as_conf
    rocrate_json = exp.as_conf.experiment_data['ROCRATE']
    jobs = []
    start_time = ''
    end_time = ''

    crate = create_rocrate_archive(
        as_conf=as_conf,
        rocrate_json=rocrate_json,
        jobs=jobs,
        start_time=start_time,
        end_time=end_time,
        path=tmp_path
    )
    assert crate is not None
    data_entities_ids = [data_entity['@id'] for data_entity in crate.data_entities]
    assert len(data_entities_ids) == len(
        set(data_entities_ids)), f'Duplicate IDs found in the RO-Crate data entities: {str(data_entities_ids)}'


def test_rocrate_main(autosubmit_exp, tmp_path):
    """Test calling rocrate main function, used for the command line."""
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()
    # some outputs
    for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
        Path(project_path, output_file).touch()

    exp = autosubmit_exp(_EXPID, experiment_data={
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
    }, include_jobs=True)

    autosubmit = exp.autosubmit
    r = autosubmit.rocrate(_EXPID, path=tmp_path)
    assert r


def test_do_not_include_other_crates(autosubmit_exp, tmp_path):
    """We must avoid adding crates inside crates.

    This creates large crate files unnecessarily.
    """
    project_path = Path(tmp_path, 'project')
    project_path.mkdir()

    for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
        Path(project_path, output_file).touch()

    exp = autosubmit_exp(_EXPID, experiment_data={
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
    })

    as_conf = exp.as_conf
    rocrate_json = exp.as_conf.experiment_data['ROCRATE']
    jobs = []
    start_time = ''
    end_time = ''

    def _assert_does_not_contain_zip(zip_path: Path):
        with ZipFile(zip_path, 'r') as zf:
            namelist = zf.namelist()
        inner_zips = [
            name for name in namelist
            if name.endswith(".zip")
        ]
        assert not inner_zips

    crate = create_rocrate_archive(
        as_conf=as_conf,
        rocrate_json=rocrate_json,
        jobs=jobs,
        start_time=start_time,
        end_time=end_time,
        path=tmp_path
    )
    assert crate is not None
    # Now we have 1 zip in the file system, and the zip file itself does not contain any other zip files inside.
    zip_files = list(tmp_path.glob('*.zip'))
    assert len(zip_files) == 1
    zip_file = zip_files[0]
    _assert_does_not_contain_zip(zip_file)

    crate = create_rocrate_archive(
        as_conf=as_conf,
        rocrate_json=rocrate_json,
        jobs=jobs,
        start_time=start_time,
        end_time=end_time,
        path=tmp_path
    )
    assert crate is not None
    # Now we have 2 zips in the file system, and the zip files themselves do not contain any other zip files inside.
    zip_files = list(tmp_path.glob('*.zip'))
    assert len(zip_files) == 2
    for zip_file in zip_files:
        _assert_does_not_contain_zip(zip_file)
