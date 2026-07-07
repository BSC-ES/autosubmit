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
from typing import Any, Callable

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
    calendar_unitsize_isgreater,
    cancel_jobs,
    get_split_size_unit,
    is_leap_year,
)
from autosubmit.log.log import AutosubmitCritical

"""Tests for ``autosubmit.job.job_utils``."""

# TODO: maybe these functions could go into conftest later.

def _create_job_mock(job_data: dict[str, Any], mocker) -> Job:
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
def create_job_list(mocker) -> Callable[[list[dict[str, Any]]], JobList]:
    """Create a mocked job list for the job_utils tests."""

    def _fn(jobs_data: list[dict[str, Any]]):
        job_list = mocker.patch('autosubmit.job.job_list.JobList', autospec=True)
        job_list.jobs = [
            _create_job_mock(data, mocker) for data in jobs_data
        ]
        job_list.job_list = job_list.jobs
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
    "data,result",
    [
        (
            {
                "JOBS": {"TEST": {"SPLITSIZEUNIT": "none"}},
                "EXPERIMENT": {"CHUNKSIZEUNIT": "none"},
            },
            "day",
        ),
        (
            {
                "JOBS": {"TEST": {"SPLITSIZEUNIT": "none"}},
                "EXPERIMENT": {"CHUNKSIZEUNIT": "day"},
            },
            "hour",
        ),
        (
            {
                "JOBS": {"TEST": {"SPLITSIZEUNIT": "none"}},
                "EXPERIMENT": {"CHUNKSIZEUNIT": "month"},
            },
            "day",
        ),
        (
            {
                "JOBS": {"TEST": {"SPLITSIZEUNIT": "none"}},
                "EXPERIMENT": {"CHUNKSIZEUNIT": "year"},
            },
            "month",
        ),
        (
            {
                "JOBS": {"TEST": {"SPLITSIZEUNIT": "something-else"}},
                "EXPERIMENT": {"CHUNKSIZEUNIT": "none"},
            },
            "something-else",
        ),
    ],
    ids=[
        "return day",
        "return hour",
        "return day",
        "return month",
        "return something-else",
    ],
)
def test_get_split_size_unit(data, result):
    """Test the get_split_size_unit function for different combinations of chunk size unit and split size unit."""
    assert get_split_size_unit(data, "TEST") == result


def test_calendar_unitsize_isgreater_raises():
    """Test that an invalid unit in calendar_unitsize_isgreater raises AutosubmitCritical."""
    with pytest.raises(AutosubmitCritical):
        calendar_unitsize_isgreater("invalid", "hour")


@pytest.mark.parametrize(
    "date_str, cal, expected_days",
    [
        ("20000201", "standard", 29),
        ("19000201", "standard", 28),
        ("20040201", "standard", 29),
        ("20010201", "standard", 28),
        ("20000201", "noleap", 28),
        ("20040201", "noleap", 28),
        ("20000401", "standard", 30),
        ("20000601", "standard", 30),
        ("20000901", "standard", 30),
        ("20001101", "standard", 30),
        ("20000101", "standard", 31),
        ("20000301", "standard", 31),
        ("20001201", "standard", 31),
    ],
    ids=[
        "february 2000 standard, leap year, returns 29 days",
        "february 1900 standard, non-leap year, returns 28 days",
        "february 2004 standard, leap year, returns 29 days",
        "february 2001 standard, non-leap year, returns 28 days",
        "february 2000 noleap, returns 28 days",
        "february 2004 noleap, returns 28 days",
        "april standard, returns 30 days",
        "june standard, returns 30 days",
        "september standard, returns 30 days",
        "november standard, returns 30 days",
        "january standard, returns 31 days",
        "march standard, returns 31 days",
        "december standard, returns 31 days",
    ],
)
def test_month_days(date_str, cal, expected_days):
    """Test the calendar_get_month_days function for different dates and calendar types."""
    assert calendar_get_month_days(date_str, cal) == expected_days


def test_default_calendar_is_standard():
    """Default calendar should behave as the standard calendar."""
    assert calendar_get_month_days("20000201") == 29


def test_calendar_unitsize_getlowersize_raises():
    """Test that an invalid unit raises AutosubmitCritical."""
    with pytest.raises(AutosubmitCritical):
        calendar_unitsize_getlowersize("invalid")


@pytest.mark.parametrize(
    "unit, expected_lower_unit",
    [
        ("hour", "hour"),
        ("day", "hour"),
        ("month", "day"),
        ("year", "month"),
    ],
    ids=[
        "hour returns hour, already the lowest unit",
        "day returns hour",
        "month returns day",
        "year returns month",
    ],
)
def test_calendar_unitsize_getlowersize(unit, expected_lower_unit):
    """Test that calendar_unitsize_getlowersize returns the correct lower unit."""
    assert calendar_unitsize_getlowersize(unit) == expected_lower_unit


@pytest.mark.parametrize(
    "year, cal, expected",
    [
        (2000, "standard", True),
        (1900, "standard", False),
        (2004, "standard", True),
        (2001, "standard", False),
        (2000, "noleap", False),
        (2004, "noleap", False),
        (2001, "noleap", False),
    ],
    ids=[
        "2000 standard leap year",
        "1900 standard not leap year",
        "2004 standard leap year",
        "2001 standard not leap year",
        "2000 noleap not leap year",
        "2004 noleap not leap year",
        "2001 noleap not leap year",
    ],
)
def test_is_leap_year(year, cal, expected):
    """Test is_leap_year for standard and noleap calendars."""
    assert is_leap_year(year, cal) == expected


@pytest.mark.parametrize(
    "cal, chunk_unit, split_unit, split_policy",
    [
        ("standard", "day", "hour", "flexible"),
        ("standard", "day", "day", "flexible"),
        ("noleap", "year", "month", "strict"),
        ("noleap", "month", "month", "strict"),
    ],
    ids=[
        "standard day chunk, hour split",
        "standard day chunk, day split",
        "noleap year chunk, month split",
        "noleap month chunk, month split",
    ],
)
def test_validate_calendar_inputs_valid(cal, chunk_unit, split_unit, split_policy):
    """Test that calendar_chunk_section does not raise when inputs are valid."""
    _validate_calendar_inputs(cal, chunk_unit, split_unit, split_policy)


@pytest.mark.parametrize(
    "cal, chunk_unit, split_unit, split_policy",
    [
        ("standard", "hour", "hour", "flexible"),
        ("invalid-calendar", "day", "hour", "flexible"),
        ("standard", "invalid-chunk-unit", "hour", "flexible"),
        ("standard", "day", "invalid-split-unit", "flexible"),
        ("standard", "day", "year", "flexible"),
        ("standard", "day", "hour", "invalid-split-policy"),
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


@pytest.mark.parametrize(
    "start, end, unit, cal, expected",
    [
        (datetime(2000, 1, 1), datetime(2000, 1, 2), "hour", "standard", 24.0),
        (datetime(2000, 1, 1), datetime(2000, 1, 8), "hour", "standard", 168.0),
        (datetime(2000, 1, 1), datetime(2000, 2, 1), "day", "standard", 31.0),
        (datetime(2000, 2, 1), datetime(2000, 3, 1), "day", "standard", 29.0),
        (datetime(2000, 2, 1), datetime(2000, 3, 1), "day", "noleap", 29.0),
        (datetime(2000, 1, 1), datetime(2000, 4, 1), "month", "standard", 3.0),
        (datetime(2000, 2, 1), datetime(2000, 3, 1), "month", "standard", 1.0),
        (datetime(2000, 2, 1), datetime(2000, 3, 1), "month", "noleap", 1.0),
        (
            datetime(2000, 1, 16),
            datetime(2000, 2, 15),
            "month",
            "standard",
            (16 / 31) + (14 / 29),
        ),
        (datetime(2001, 1, 1), datetime(2002, 1, 1), "year", "standard", 1.0),
        (datetime(2001, 1, 1), datetime(2002, 1, 1), "year", "noleap", 1.0),
        (datetime(2000, 1, 1), datetime(2001, 1, 1), "year", "standard", 1.0),
        (datetime(2000, 1, 1), datetime(2001, 1, 1), "year", "noleap", 1.0),
        (datetime(1999, 1, 1), datetime(2001, 1, 1), "year", "standard", 2.0),
    ],
    ids=[
        "hour unit with standard cal, 1 day apart returns 24",
        "hour unit with standard cal, 1 week apart returns 168",
        "day unit with standard cal, jan returns 31",
        "day unit with standard cal, feb 2000 leap year returns 29",
        "day unit with noleap cal, feb 2000 leap year returns 29",
        "month unit with standard cal, jan to apr returns 3",
        "month unit with standard cal, full feb 2000 leap year returns 1",
        "month unit with noleap cal, full feb 2000 leap year returns 1",
        "month unit with standard cal, mid jan to mid feb 2000 returns (16/31) + (14/29)",
        "year unit with standard cal, non-leap year 2001 returns 1",
        "year unit with noleap cal, non-leap year 2001 returns 1",
        "year unit with standard cal, leap year 2000 returns 1",
        "year unit with noleap cal, leap year 2000 returns 1",
        "year unit with standard cal, 2 years 1999 to 2001 returns 2",
    ],
)
def test_count_units_between_dates(start, end, unit, cal, expected):
    """Test that _count_units_between_dates returns the correct number of units."""
    result = _count_units_between_dates(start, end, unit, cal)
    assert isinstance(result, float)
    assert abs(result - expected) < 1e-9


def test_count_units_between_dates_invalid_unit():
    """Test that _count_units_between_dates raises AutosubmitCritical for an invalid unit."""
    with pytest.raises(AutosubmitCritical):
        _count_units_between_dates(
            datetime(2000, 1, 1), datetime(2001, 1, 1), "invalid-unit", "standard"
        )


@pytest.mark.parametrize(
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
    ids=[
        "hour chunk, hour split raises",
        "hour chunk, day split raises",
        "hour chunk, month split raises",
        "hour chunk, year split raises",
        "day chunk, hour split returns 24 and 12",
        "day chunk, day split returns 1 and None",
        "day chunk, month split raises",
        "day chunk, year split raises",
        "month chunk, hour split returns 744 and 372",
        "month chunk, day split returns 31 and 16",
        "month chunk, month split returns 1 and None",
        "month chunk, year split raises",
        "year chunk, hour split returns 8784 and 4392",
        "year chunk, day split returns 366 and 183",
        "year chunk, month split returns 12 and 6",
        "year chunk, year split returns 1 and None",
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


def test_non_integer_splits_with_strict_policy_raises():
    """Test that calendar_chunk_section raises AutosubmitCritical when the split policy is strict and the splits are not an integer."""
    experiment_data = {
        "EXPERIMENT": {
            "DATELIST": "20000101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "day",
            "CHUNKSIZE": "1",
            "NUMCHUNKS": "2",
            "CALENDAR": "standard",
            "SPLITPOLICY": "strict",
        },
        "JOBS": {
            "A": {
                "FILE": "a",
                "PLATFORM": "test",
                "RUNNING": "chunk",
                "SPLITS": "auto",
                "SPLITSIZE": 5,
                "SPLITSIZEUNIT": "hour",
            },
        },
    }
    date = datetime.strptime("20000101", "%Y%m%d")
    chunk = 1
    with pytest.raises(AutosubmitCritical):
        calendar_chunk_section(experiment_data, "A", date, chunk)


def test_calendar_chunk_section_running_not_chunk():
    """Test that calendar_chunk_section returns 0 when the job is not set to run by chunk."""
    experiment_data = {
        "EXPERIMENT": {
            "DATELIST": "20000101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "day",
            "CHUNKSIZE": "1",
            "NUMCHUNKS": "2",
            "CALENDAR": "standard",
        },
        "JOBS": {
            "A": {
                "FILE": "a",
                "PLATFORM": "test",
                "RUNNING": "date",
                "SPLITS": "auto",
                "SPLITSIZE": 1,
                "SPLITSIZEUNIT": "hour",
            },
        },
    }
    date = datetime.strptime("20000101", "%Y%m%d")
    chunk = 1
    assert calendar_chunk_section(experiment_data, "A", date, chunk) == 0
