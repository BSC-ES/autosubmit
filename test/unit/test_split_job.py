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

import copy
from datetime import datetime

import pytest

from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.template import Language
from autosubmit.platforms.psplatform import PsPlatform


def _build_split_experiment_data(experiment_data):
    split_experiment_data = copy.deepcopy(experiment_data)
    split_job_data = split_experiment_data.setdefault('JOBS', {}).setdefault('RANDOM-SECTION', {})
    split_job_data['RUNNING'] = 'chunk'

    split_experiment_data.setdefault('PLATFORMS', {}).setdefault('dummy_platform', {'type': 'ps'})
    split_experiment_data.setdefault('ROOTDIR', 'dummy_rootdir')
    split_experiment_data.setdefault('LOCAL_TMP_DIR', 'dummy_tmpdir')
    split_experiment_data.setdefault('LOCAL_ROOT_DIR', 'dummy_rootdir')

    return split_experiment_data


def _generate_and_get_split_job(autosubmit_config, experiment_data):
    as_conf = autosubmit_config('t000', experiment_data)
    as_conf.experiment_data = as_conf.deep_normalize(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)

    parameters = as_conf.load_parameters()
    job_list_obj = JobList('t000', as_conf, as_conf.parser_factory, JobListPersistencePkl())
    job_list_obj.generate(
        as_conf=as_conf,
        date_list=[datetime(2020, 1, 1)],
        member_list=['fc0'],
        num_chunks=1,
        chunk_ini=1,
        parameters=parameters,
        date_format=None,
        default_retrials=as_conf.get_retrials(),
        default_job_type=Language.BASH,
        wrapper_jobs={},
        new=True,
        run_only_members=[],
        show_log=False,
        create=True,
    )

    split_jobs = [
        job for job in job_list_obj.get_job_list()
        if job.section == 'RANDOM-SECTION' and isinstance(job.split, int) and job.split > 0
    ]
    assert split_jobs

    for job in split_jobs:
        job.platform = PsPlatform(expid='t000', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
        job.update_parameters(as_conf, set_attributes=True)
    return split_jobs


@pytest.mark.parametrize('experiment_data, attributes_to_check, configured_splits', [
    (
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': 'test.sh',
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'NOTIFY_ON': 'COMPLETED',
                    'SPLITS': 2,
                },
            },
            'PLATFORMS': {
                'dummy_platform': {
                    'type': 'ps',
                },
            },
            'ROOTDIR': 'dummy_rootdir',
            'LOCAL_TMP_DIR': 'dummy_tmpdir',
            'LOCAL_ROOT_DIR': 'dummy_rootdir',
        },
        {'notify_on': ['COMPLETED']},
        2
    ),
    (
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': 'test.sh',
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'CPMIP_THRESHOLDS': {
                        'SYPD': {
                            'THRESHOLD': 5.0,
                            'COMPARISON': 'greater_than',
                            '%_ACCEPTED_ERROR': 10,
                        }
                    },
                    'SPLITS': 3,
                },
            },
        },
        {
            'cpmip_thresholds': {
                'SYPD': {
                    'THRESHOLD': 5.0,
                    'COMPARISON': 'greater_than',
                    '%_ACCEPTED_ERROR': 10,
                }
            }
        },
        3
    ),
    (
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': 'test.sh',
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'SPLITS': 1,
                },
            },
        },
        {'cpmip_thresholds': {}},
        1
    ),
    (
        {
            'EXPERIMENT': {
                'CHUNKSIZE': 3,
                'CHUNKSIZEUNIT': 'MONTH',
            },
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': 'test.sh',
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'SPLITS': 4,
                },
            },
        },
        {'chunk_size': 3, 'chunk_size_unit': 'month'},
        4
    ),
    (
        {
            'JOBS': {
                'RANDOM-SECTION': {
                    'FILE': 'test.sh',
                    'PLATFORM': 'DUMMY_PLATFORM',
                    'SPLITS': 5,
                },
            },
        },
        {'chunk_size': 1, 'chunk_size_unit': ''},
        5
    ),
], ids=[
    'notify_on_attribute',
    'cpmip_thresholds_from_config',
    'empty_cpmip_thresholds_when_missing',
    'chunk_metadata_from_experiment_defaults',
    'chunk_metadata_when_missing',
])
def test_update_parameters_split_attributes(autosubmit_config, experiment_data, attributes_to_check, configured_splits):
    split_experiment_data = _build_split_experiment_data(experiment_data)
    split_jobs = _generate_and_get_split_job(autosubmit_config, split_experiment_data)

    assert len(split_jobs) == configured_splits
    assert sorted(int(job.split) for job in split_jobs) == list(range(1, configured_splits + 1))

    for job in split_jobs:
        assert job.split > 0
        assert job.splits > 0
        assert int(job.splits) == configured_splits
        assert 1 <= int(job.split) <= configured_splits

        for attr, expected in attributes_to_check.items():
            assert hasattr(job, attr)
            assert getattr(job, attr) == expected
