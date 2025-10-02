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

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.job.job_packages import JobPackageVertical
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

_EXPID = 't000'


@pytest.mark.docker
@pytest.mark.postgres
def test_load_save_load(as_db: str, autosubmit_exp):
    exp = autosubmit_exp(_EXPID, experiment_data={
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

    submitter = ParamikoSubmitter()
    submitter.load_platforms(exp.as_conf)

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
    assert not job_package_persistence.load(exp.expid)

    job_package = JobPackageVertical(jobs, configuration=exp.as_conf, wrapper_section="WRAPPER_0")

    job_package_persistence.save(job_package)

    job_packages = job_package_persistence.load(exp.expid)
    assert len(jobs) == len(job_packages)

    job_package_persistence.reset_table(True)
    assert not job_package_persistence.load(exp.expid)
