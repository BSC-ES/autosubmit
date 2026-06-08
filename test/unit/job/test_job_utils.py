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

from datetime import datetime
from typing import Any, Callable, Dict, List

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_utils import (
    calendar_chunk_section,
    calendar_get_month_days,
    _validate_calendar_inputs,
    _count_units_between_dates,
    calendar_unitsize_getlowersize,
    cancel_jobs,
    get_split_size_unit,
)
from autosubmit.log.log import AutosubmitCritical

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

    job_list.get_job_list()[0].platform.cancel_jobs.side_effect = ValueError('platypus')

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


@pytest.mark.parametrize(
    'data,result',
    [
        ({'JOBS': {'TEST': {'SPLITSIZEUNIT': "none"}}, 'EXPERIMENT': {'CHUNKSIZEUNIT': "none"}}, "day"),
        ({'JOBS': {'TEST': {'SPLITSIZEUNIT': "none"}}, 'EXPERIMENT': {'CHUNKSIZEUNIT': "day"}}, "hour"),
        ({'JOBS': {'TEST': {'SPLITSIZEUNIT': "none"}}, 'EXPERIMENT': {'CHUNKSIZEUNIT': "month"}}, "day"),
        ({'JOBS': {'TEST': {'SPLITSIZEUNIT': "none"}}, 'EXPERIMENT': {'CHUNKSIZEUNIT': "year"}}, "month"),
        ({'JOBS': {'TEST': {'SPLITSIZEUNIT': "something-else"}}, 'EXPERIMENT': {'CHUNKSIZEUNIT': "none"}}, "something-else"),
    ],
    ids=[
        'return day',
        'return hour',
        'return day',
        'return month',
        'return something-else'
    ]
)
def test_get_split_size_unit(data, result):
    """Test the get_split_size_unit function for different combinations of chunk size unit and split size unit."""
    assert get_split_size_unit(data, 'TEST') == result


@pytest.mark.parametrize(
    "date_str, cal, expected_days",
    [
        # Februray with standard calendar (leap year)
        ("20000201", "standard", 29), # 2000 is a leap year
        ("19000201", "standard", 28), # 1900 is not a leap year
        ("20040201", "standard", 29), # 2004 is a leap year
        ("20010201", "standard", 28), # 2001 is not a leap year
        # February with noleap calendar (no leap years)
        ("20000201", "noleap", 28), # leap year ignored under noleap calendar
        ("20040201", "noleap", 28), # leap year ignored under noleap calendar
        # 30 day months
        ("20000401", "standard", 30), # regular April
        ("20000601", "standard", 30), # regular June
        ("20000901", "standard", 30), # regular September
        ("20001101", "standard", 30), # regular November
        # 31 day months
        ("20000101", "standard", 31), # regular January
        ("20000301", "standard", 31), # regular March
        ("20001201", "standard", 31), # regular December
    ],
)
def test_month_days(date_str, cal, expected_days):
    """Test the calendar_get_month_days function for different dates and calendar types."""
    assert calendar_get_month_days(date_str, cal) == expected_days


def test_default_calendar_is_standard():
    """Default calendar should behave as the standard calendar."""
    assert calendar_get_month_days("20000201") == 29 # 2000 is a leap year



def test_calendar_unitsize_getlowersize_raises():
    """Test that an invalid unit raises AutosubmitCritical."""
    with pytest.raises(AutosubmitCritical):
        calendar_unitsize_getlowersize("invalid")


def test_validate_inputs_not_raise():
    """Test that calendar_chunk_section does not raise when inputs are valid."""
    _validate_calendar_inputs("standard", "day", "hour", "flexible")
    _validate_calendar_inputs("noleap", "year", "month", "strict")


def test_split_unit_equal_to_chunk_unit_not_raise():
    """Test that calendar_chunk_section does not raise when split unit is equal to chunk unit."""
    _validate_calendar_inputs("standard", "day", "day", "flexible")
    _validate_calendar_inputs("noleap", "month", "month", "strict")

@pytest.mark.parametrize(
    "cal, chunk_unit, split_unit, split_policy",
    [
        ("standard", "hour", "hour", "flexible"), # chunk unit is hour
        ("invalid-calendar", "day", "hour", "flexible"), # invalid calendar
        ("standard", "invalid-chunk-unit", "hour", "flexible"), # invalid chunk unit
        ("standard", "day", "invalid-split-unit", "flexible"), # invalid split unit
        ("standard", "day", "year", "flexible"), # split > chunk unit
        ("standard", "day", "week", "invalid-split-policy"), # invalid split policy
    ],
    ids=[
        "chunk unit is hour raises",
        "invalid calendar raises",
        "invalid chunk unit raises",
        "invalid split unit raises",
        "split unit greater than chunk unit raises",
        "invalid split policy raises",
    ],
)
def test_validate_calendar_inputs_invalid(cal, chunk_unit, split_unit, split_policy):
    """Test that _validate_calendar_inputs raises AutosubmitCritical for invalid inputs."""
    with pytest.raises(AutosubmitCritical):
        _validate_calendar_inputs(cal, chunk_unit, split_unit, split_policy)


def test_count_hours_in_a_day():
    """Test that _count_units_between_dates returns 24 hours for a day chunk and hour split."""
    start = datetime(2000, 1, 1)
    end = datetime(2000, 1, 2)
    result = _count_units_between_dates(start, end, "hour", "standard")
    assert isinstance(result, float)
    assert result == 24.0


def test_count_days_in_month_jan():
    """Test that _count_units_between_dates returns 31 days for a month chunk and day split in January."""
    start = datetime(2000, 1, 1)
    end = datetime(2000, 2, 1)
    result = _count_units_between_dates(start, end, "day", "standard")
    assert isinstance(result, float)
    assert result == 31.0


def test_count_multiple_full_months():
    """Test that _count_units_between_dates returns 3 months for a chunk covering January to April."""
    start = datetime(2000, 1, 1)
    end = datetime(2000, 4, 1)
    result = _count_units_between_dates(start, end, "month", "standard")
    assert isinstance(result, float)
    assert result == 3.0


def test_count_multiple_partial_months():
    """Test that _count_units_between_dates returns the correct fraction of months for a chunk covering mid-January to mid-February."""
    start = datetime(2000, 1, 16)
    end = datetime(2000, 2, 15)
    result = _count_units_between_dates(start, end, "month", "standard")
    assert isinstance(result, float)
    # January: 16 days (16th to 31st inclusive) out of 31 days -> 16/31
    # February: 14 days (1st to 14th inclusive) out of 29 days (leap year) -> 14/29
    expected = (16 / 31) + (14 / 29)
    assert abs(result - expected) < 1e-6


@pytest.mark.parametrize(
    # if chunksizeunit is hour, raises error
    # if splitsizeunit is bigger than chunksizeunit, raises error
    # TEST CASE CALCULATIONS:
    # date = 2000-01-01, chunk lenght is 1
    # for day chunk_size_unit, run_days = 1
    # for month chunk_size_unit (jan 2000), run_days = 31
    # for year chunk_size_unit (2000), run_days = 366 (leap year)
    # So:
    # day + hour -> 1 x 24 = 24
    # day + day -> ceil(1 / 1) = 1
    # month + hour -> 31 x 24 = 744
    # month + day -> ceil(31 / 1) = 31
    # month + month -> ceil(31 / 30) = 2
    # month + year -> ceil(31 / 366) = 1
    # year + hour -> 366 x 24 = 8784
    # year + day -> ceil(366 / 1) = 366
    # year + month -> ceil(366 / 30) = 13
    # year + year -> ceil(366 / 366) = 1
    "chunk_size_unit, split_size_unit, expected_splits_A, expected_splits_B",
    [
        ("hour", "hour", None, None),
        ("hour", "day", None, None),
        ("hour", "month", None, None),
        ("hour", "year", None, None),
        ("day", "hour", 24, 12),
        ("day", "day", 1, None),
        ("day", "month", None, None),
        ("day", "year", None, None),
        ("month", "hour", 744, 372),
        ("month", "day", 31, 16),
        ("month", "month", 1, None),
        ("month", "year", None, None),
        ("year", "hour", 8784, 4392),
        ("year", "day", 366, 183),
        ("year", "month", 12, 6),
        ("year", "year", 1, None),
    ],
)
def test_calendar_chunk_section(
    chunk_size_unit, split_size_unit, expected_splits_A, expected_splits_B
):
    """Test the calendar_chunk_section function for different chunk size units and split units."""
    experiment_data = {
        "EXPERIMENT": {
            "DATELIST": "20000101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": chunk_size_unit,
            "CHUNKSIZE": "1",
            "NUMCHUNKS": "2",
            "CALENDAR": "standard",
        },
        "JOBS": {
            "A": {
                "FILE": "a",
                "PLATFORM": "test",
                "RUNNING": "chunk",
                "SPLITS": "auto",
                "SPLITSIZE": 1,
                "SPLITSIZEUNIT": split_size_unit,
            },
            "B": {
                "FILE": "b",
                "PLATFORM": "test",
                "RUNNING": "chunk",
                "SPLITS": "auto",
                "SPLITSIZE": 2,
                "SPLITSIZEUNIT": split_size_unit,
            },
        },
    }
    date = datetime.strptime("20000101", "%Y%m%d")
    chunk = 1
    if expected_splits_A is None:
        with pytest.raises(AutosubmitCritical):
            calendar_chunk_section(experiment_data, "A", date, chunk)
    else:
        splits_A = calendar_chunk_section(experiment_data, "A", date, chunk)
        assert splits_A == expected_splits_A

    if expected_splits_B is None:
        with pytest.raises(AutosubmitCritical):
            calendar_chunk_section(experiment_data, "B", date, chunk)
    else:
        splits_B = calendar_chunk_section(experiment_data, "B", date, chunk)
        assert splits_B == expected_splits_B
