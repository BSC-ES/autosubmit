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


@pytest.mark.parametrize(
    "reached_steps,expected",
    [
        ({1, 2}, Status.READY),
        ({1}, Status.WAITING),
    ],
    ids=["step-reached", "step-pending"],
)
def test_checkpoint_job_waits_for_parent_step(init_jobs, reached_steps, expected):
    """A child runs when its parent reaches FROM_STEP and stays WAITING before that."""
    job_list, job_a, job_b, job_c = init_jobs

    job_a.status = Status.COMPLETED
    job_b.status = Status.RUNNING
    job_b.max_checkpoint_step = 2
    job_b.current_checkpoint_step = 0
    job_b.platform.check_file_exists.side_effect = lambda name, **_: int(
        name.rsplit("_", 1)[1]
    ) in reached_steps

    job_list.graph.edges[job_a.name, job_c.name].update(
        accepted_statuses="COMPLETED", from_step=0
    )
    job_list.graph.edges[job_b.name, job_c.name].update(
        accepted_statuses="RUNNING", from_step=2
    )

    job_list._refresh_checkpoint_steps()
    job_list._update_waiting_and_delayed_jobs()

    assert job_c.status == expected
