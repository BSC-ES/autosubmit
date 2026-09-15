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

import shutil
from random import randrange
from typing import Any

import pytest
from networkx import DiGraph

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList


@pytest.fixture
def setup_job_list(autosubmit_exp, tmpdir, mocker):
    as_exp = autosubmit_exp()
    as_conf = as_exp.as_conf
    as_conf.experiment_data = {}
    as_conf.experiment_data["JOBS"] = {}
    as_conf.experiment_data["PLATFORMS"] = {}

    job_list = JobList(as_exp.expid, as_conf, YAMLParserFactory())

    dummy_serial_platform = mocker.MagicMock()
    dummy_serial_platform.name = 'serial'
    dummy_platform = mocker.MagicMock()
    dummy_platform.serial_platform = dummy_serial_platform
    dummy_platform.name = 'dummy_platform'

    jobs = {
        "completed": [_create_dummy_job_with_status(Status.COMPLETED, dummy_platform) for _ in range(4)],
        "submitted": [_create_dummy_job_with_status(Status.SUBMITTED, dummy_platform) for _ in range(3)],
        "running": [_create_dummy_job_with_status(Status.RUNNING, dummy_platform) for _ in range(2)],
        "queuing": [_create_dummy_job_with_status(Status.QUEUING, dummy_platform)],
        "failed": [_create_dummy_job_with_status(Status.FAILED, dummy_platform) for _ in range(4)],
        "ready": [_create_dummy_job_with_status(Status.READY, dummy_platform) for _ in range(3)],
        "waiting": [_create_dummy_job_with_status(Status.WAITING, dummy_platform) for _ in range(2)],
        "unknown": [_create_dummy_job_with_status(Status.UNKNOWN, dummy_platform)]
    }
    # add nodes
    job_list.graph = DiGraph()
    for job_status, jobs_ in jobs.items():
        for job in jobs_:
            job_list.add_job(job)

    waiting_job = jobs["waiting"][0]
    waiting_job.parents.update(
        jobs["ready"] + jobs["completed"] + jobs["failed"] + jobs["submitted"] + jobs["running"] + jobs["queuing"])

    yield job_list, waiting_job, jobs
    shutil.rmtree(tmpdir)


def _create_dummy_job_with_status(status, platform):
    job_name = str(randrange(999999, 999999999))
    job_id = randrange(1, 999)
    job = Job(job_name, job_id, status, 0)
    job.type = randrange(0, 2)
    job.platform = platform
    return job


@pytest.fixture
def init_jobs(setup_job_list: tuple[Any, Any, dict[str, Any]]) -> tuple[Job, Job, Job]:
    """
    Initialize jobs for testing.

    :param setup_job_list: Pytest fixture providing job list, waiting job, and jobs dict.
    :type setup_job_list: Tuple[Any, Any, Dict[str, Any]]
    :return: Tuple of job A, job B, and job C.
    :rtype: Tuple[Job, Job, Job]
    """
    job_list, _, jobs = setup_job_list
    job_list.jobs_edges = {}
    job_a = jobs["completed"][0]
    job_b = jobs["running"][0]
    job_c = jobs["waiting"][0]
    job_a.children = set()
    job_a.add_children([job_c])
    job_b.add_children([job_c])
    job_c.parents = set()
    job_c.parents.add(job_a)
    job_c.parents.add(job_b)
    job_list.graph.add_edge(job_a.name, job_c.name)
    job_list.graph.add_edge(job_b.name, job_c.name)
    return job_list, job_a, job_b, job_c


# TODO: missing scenarios
# 1) Cases when job_c.status remains in WAITING.
# 2) Cases when job_a or job_b status doesn't match the edge_info target status.
# 3) From step higher than 0. (update job.checkpoint)
@pytest.mark.parametrize(
    'job_a_edge_info,job_b_edge_info',
    [
        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.WAITING], "FROM_STEP": 0, "PARENT_STATUS": "WAITING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0, "PARENT_STATUS": "RUNNING"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0, "PARENT_STATUS": "COMPLETED"}),

        ({"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"},
         {"ACCEPTED_STATUSES": Status.VALUE_TO_KEY[Status.FAILED], "FROM_STEP": 0, "PARENT_STATUS": "FAILED"}),
    ],
    ids=[
        "1. JOB A COMPLETED, JOB B RUNNING",
        "2. JOB A RUNNING, JOB B COMPLETED",
        "3. JOB A RUNNING, JOB B RUNNING",
        "4. JOB A COMPLETED, JOB B COMPLETED",
        "5. JOB A COMPLETED, JOB B WAITING",
        "6. JOB A WAITING, JOB B COMPLETED",
        "7. JOB A WAITING, JOB B RUNNING",
        "8. JOB A WAITING, JOB B WAITING",
        "9. JOB A FAILED, JOB B WAITING",
        "10. JOB A WAITING, JOB B FAILED",
        "11. JOB A FAILED, JOB B RUNNING",
        "12. JOB A RUNNING, JOB B FAILED",
        "13. JOB A FAILED, JOB B COMPLETED",
        "14. JOB A COMPLETED, JOB B FAILED",
        "15. JOB A FAILED, JOB B COMPLETED",
        "16. JOB A FAILED, JOB B FAILED",
    ]
)
def test_handle_special_checkpoint_jobs_matching_parent_status_with_target_and_not_optional(
        job_a_edge_info: dict[str, Any],
        job_b_edge_info: dict[str, Any],
        init_jobs: tuple[JobList, Any, Any, Any]
) -> None:
    """
    Test special checkpoint job handling for various parent job status combinations.

    :param job_a_edge_info: Edge info dictionary for job A.
    :type job_a_edge_info: Dict[str, Any]
    :param job_b_edge_info: Edge info dictionary for job B.
    :type job_b_edge_info: Dict[str, Any]
    :param init_jobs: Fixture providing initialized jobs and job list.
    :type init_jobs: Tuple[JobList, Job, Job, Job]
    :return: None
    :rtype: None
    """
    job_list, job_a, job_b, job_c = init_jobs

    job_list.graph.edges[job_a.name, job_c.name].update(
        accepted_statuses=job_a_edge_info["ACCEPTED_STATUSES"],
        from_step=job_a_edge_info["FROM_STEP"],
    )
    job_list.graph.edges[job_b.name, job_c.name].update(
        accepted_statuses=job_b_edge_info["ACCEPTED_STATUSES"],
        from_step=job_b_edge_info["FROM_STEP"],
    )

    job_a.status = Status.KEY_TO_VALUE[job_a_edge_info["PARENT_STATUS"]]
    job_b.status = Status.KEY_TO_VALUE[job_b_edge_info["PARENT_STATUS"]]
    job_list._handle_special_checkpoint_jobs()
    assert job_c.status == Status.READY
