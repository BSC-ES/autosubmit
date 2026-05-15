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

"""Tests for ``JobPackagePersistence``."""

import datetime

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter


@pytest.mark.docker
@pytest.mark.postgres
def test_load_save_load(as_db: str, autosubmit_exp):
    exp = autosubmit_exp(experiment_data={
        'JOBS': {
            '1': {
                'PLATFORM': 'TEST_SLURM_PLATFORM',
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            },
            '2': {
                'PLATFORM': 'TEST_SLURM_PLATFORM',
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            },
            '3': {
                'PLATFORM': 'TEST_SLURM_PLATFORM',
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            }
        },
        'WRAPPERS': {
            'WRAPPER_0': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': '1 2 3'
            }
        },
        'PLATFORMS': {
            'TEST_SLURM_PLATFORM': {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
            }
        }
    })

    submitter = ParamikoSubmitter(as_conf=exp.as_conf)

    # TODO: We already have the AS experiment from the call above, it'd be nicer
    #       to use the jobs from that experiment instead of recreating here.
    #       We call ``autosubmit_exp`` in order to have the correct ``LOCAL_ROOT_DIR``.
    jobs = []
    for i in range(3):
        job = Job(f'{exp.expid}_20000101_fc0_1_{str(i)}', f'{exp.expid}_20000101_fc0_1_{str(i)}', None, None)
        job.processors = 1
        job.type = 0
        job.date = '20000101'
        job.chunk = '1'
        job.member = 'fc0'
        job.platform = submitter.platforms['TEST_SLURM_PLATFORM']
        job.het = {}
        job.wallclock = '00:30'
        jobs.append(job)

    job_package_persistence = JobPackagePersistence(exp.expid)
    # Initially empty
    wrappers_info, inner_jobs = job_package_persistence.load(preview=False)
    assert not wrappers_info
    assert not inner_jobs

    wrapper_name = f"{exp.expid}_wrapper_1"
    wrapper_info = {
        "name": wrapper_name,
        "id": 1000,
        "script_name": None,
        "status": Status.SUBMITTED,
        "local_logs_out": None,
        "local_logs_err": None,
        "remote_logs_out": None,
        "remote_logs_err": None,
        "updated_log": 0,
        "platform_name": "TEST_SLURM_PLATFORM",
        "wallclock": "00:30",
        "num_processors": 0,
        "type": None,
        "sections": None,
        "method": None,
    }
    wrapper_inner_jobs = [
        {
            'package_id': 1000,
            'package_name': wrapper_name,
            'job_name': job.name,
            'timestamp': datetime.datetime.now().isoformat()
        }
        for job in jobs
    ]

    job_package_persistence.save([(wrapper_info, wrapper_inner_jobs)], preview=False)

    wrappers_info, inner_jobs = job_package_persistence.load(preview=False)
    assert len(wrappers_info) == 1
    assert len(inner_jobs) == len(jobs)

    job_package_persistence.reset_table(preview=True)
    # Production data should still be there
    wrappers_info, inner_jobs = job_package_persistence.load(preview=False)
    assert len(wrappers_info) == 1
    assert len(inner_jobs) == len(jobs)

    # Preview should be empty
    wrappers_info_preview, inner_jobs_preview = job_package_persistence.load(preview=True)
    assert not wrappers_info_preview
    assert not inner_jobs_preview
