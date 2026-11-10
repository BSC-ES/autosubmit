# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

"""Integration tests for command traceability logging."""

import os

from autosubmit.job.job_common import Status
from autosubmit.job.job_list import load_job_list

# noinspection PyProtectedMember
from autosubmit.scripts.autosubmit import _autosubmit


def test_setstatus_logs_deleted_working_directory(autosubmit_exp, mocker):
    """A real workflow command should continue when getcwd raises FileNotFoundError."""
    experiment = autosubmit_exp(
        experiment_data={
            "JOBS": {
                "SIM": {
                    "RUNNING": "once",
                    "PLATFORM": "local",
                    "SCRIPT": 'echo "OK"',
                }
            }
        }
    )
    log_info = mocker.patch("autosubmit.log.log.Log.info")
    traceability_os = mocker.patch("autosubmit.scripts._traceability.os")
    traceability_os.getpid.return_value = os.getpid()
    traceability_os.getcwd.side_effect = FileNotFoundError

    result = _autosubmit(
        [
            "setstatus",
            experiment.expid,
            "--status_final",
            "COMPLETED",
            "--filter_status",
            "WAITING",
            "--save",
        ]
    )

    assert result is True
    log_info.assert_any_call("  Working directory: <deleted>")
    jobs = load_job_list(
        experiment.expid,
        experiment.as_conf,
        new=False,
        full_load=True,
        check_failed_jobs=True,
    ).get_job_list()
    assert len(jobs) == 1
    assert jobs[0].status == Status.COMPLETED
