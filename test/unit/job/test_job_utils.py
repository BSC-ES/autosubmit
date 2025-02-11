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

from typing import Any, Callable, Dict, List

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_utils import cancel_jobs
from log.log import AutosubmitCritical

"""Tests for ``autosubmit.job.job_utils``."""

_EXPID = 'a000'
"""The expid used throughout the tests."""


# TODO: maybe these functions could go into conftest later.

def _create_job_mock(job_data: Dict[str, Any], mocker) -> Job:
    """Create a mocked job whose data is merged with the dict provided (as kwargs?).

    Similar to JavaScript's ``Object.assign()``.

    :param job_data: A dictionary containing job data. Each property will be assigned as a mock attribute.
    """
    job = mocker.MagicMock(spec=Job)
    for key, value in job_data.items():
        setattr(job, key, value)
    job.id = 'test-job'
    return job


@pytest.fixture
def create_job_list(mocker) -> Callable[[List[Dict[str, Any]]], JobList]:
    """Create a mocked job list for the job_utils tests."""

    def _fn(jobs_data: List[Dict[str, Any]]):
        job_list = mocker.patch('autosubmit.job.job_list.JobList', autospec=True)
        job_list.jobs = [
            _create_job_mock(data, mocker) for data in jobs_data
        ]
        job_list._job_list = job_list.jobs
        job_list.get_job_list.return_value = job_list.jobs
        return job_list

    return _fn


def test_cancellation_without_target(create_job_list):
    """Test that a cancellation without a target results in an error."""
    job_list = create_job_list([])

    with pytest.raises(AutosubmitCritical) as cm:
        cancel_jobs(job_list, None, None)

    assert 'Cancellation target status of jobs is not valid' in str(cm)


@pytest.mark.parametrize(
    'active_states',
    [
        None,
        []
    ],
    ids=[
        'active states is None',
        'active states is empty'
    ]
)
def test_cancellation_with_invalid_active_states(active_states, create_job_list, mocker):
    """Test that a cancellation with invalid active states results in errors."""
    job_list = create_job_list([
        {
            'status': Status.KEY_TO_VALUE['RUNNING']
        }
    ])

    mocked_log = mocker.patch('autosubmit.job.job_utils.Log')

    cancel_jobs(job_list, active_states, 'RUNNING')

    assert mocked_log.info.call_count == 1
    assert 'No active jobs found for expid' in mocked_log.info.call_args_list[0][0][0]


def test_cancel_jobs_platform_error(create_job_list, mocker):
    """Test the cancellation of jobs when a platform raises an error."""
    target_status = 'FAILED'
    job_list = create_job_list([
        {
            'status': Status.KEY_TO_VALUE['RUNNING']
        }
    ])

    job_list.get_job_list()[0].platform.send_command.side_effect = ValueError('platypus')

    mocked_log = mocker.patch('autosubmit.job.job_utils.Log')

    cancel_jobs(job_list, [Status.KEY_TO_VALUE['RUNNING'], Status.KEY_TO_VALUE['QUEUING']], target_status)

    for job in job_list.get_job_list():
        # Asserting as the status MUST be changed regardless of the platform error
        assert job.status == Status.KEY_TO_VALUE[target_status]

    assert mocked_log.warning.call_count == 1

    exception_message = mocked_log.warning.call_args_list[0][0][0]
    assert 'Failed to cancel job' in exception_message
    assert 'platypus' in exception_message


def test_cancel_jobs(create_job_list):
    """Test the cancellation of jobs."""
    target_status = 'FAILED'
    job_list = create_job_list([
        {
            'status': Status.KEY_TO_VALUE['RUNNING']
        },
        {
            'status': Status.KEY_TO_VALUE[target_status]
        },
        {
            'status': Status.KEY_TO_VALUE['QUEUING']
        }
    ])

    cancel_jobs(job_list, [Status.KEY_TO_VALUE['RUNNING'], Status.KEY_TO_VALUE['QUEUING']], target_status)

    for job in job_list.get_job_list():
        assert job.status == Status.KEY_TO_VALUE[target_status]
