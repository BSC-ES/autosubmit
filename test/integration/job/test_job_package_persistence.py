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
from autosubmit.job.job_packages import JobPackageVertical

_EXPID = 't000'


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
@pytest.mark.skip() # TODO: change to use the new db
def test_load_save_load(db_engine: str, request, autosubmit_exp, local):
    request.getfixturevalue(f"as_db_{db_engine}")

    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            '1': {
                'PLATFORM': 'local',
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            },
            '2': {
                'PLATFORM': 'local',
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            },
            '3': {
                'PLATFORM': 'local',
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            }
        },
        'WRAPPERS': {
            'WRAPPER_0': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': '1 2 3'
            }
        }
    })

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
        job.platform = local
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
