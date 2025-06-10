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

from datetime import timedelta, datetime
from typing import Optional

import pytest

from autosubmit.job.job import Job
from autosubmit.statistics.utils import (
    filter_by_section, filter_by_time_period, timedelta2hours, parse_number_processors
)
from log.log import AutosubmitCritical

DEFAULT_NUMBER_PROCESSORS = 1


@pytest.mark.parametrize(
    'filter_section,expected',
    [
        (None, 10),
        ('', 10),
        ('Any', 10),
        ('any', 10),
        ('ANY', 10),
        ('aNY', 10),
        ('SIM', 5),
        ('RASPBERRY', 0)
    ]
)
def test_filter_by_section(filter_section, expected):
    jobs = []
    for i in range(10):
        section = 'SIM' if i % 2 == 0 else 'PREPARE'
        job = Job(f'a000_{section}', f'a000_{section}', None, 1, None)
        job.section = section
        jobs.append(job)

    assert len(filter_by_section(jobs, filter_section)) == expected


@pytest.mark.parametrize(
    'hours_span',
    [
        0,
        -1
    ]
)
def test_filter_by_time_period_invalid_hours_span(hours_span):
    with pytest.raises(AutosubmitCritical):
        filter_by_time_period([], hours_span)


def test_filter_by_time_period_no_hours_span():
    jobs, start_time, current_time = filter_by_time_period([], None)
    assert jobs is not None
    assert start_time is None
    assert current_time is not None


def test_filter_by_time_period(mocker, monkeypatch):
    """Test that the filter by time works as expected.

    We mock the ``datetime`` module to control how the start time is computed
    inside the function.

    All dates in this test use the same year, month, and day. They only vary
    the hour.

    The current time in the test will be set to 20h00.

    This test creates ten jobs. The first five get set the same start time,
    10h00. The following five have the start time set to +5 hours, 15h00.

    We filter it three times. The first with two hours, which is not enough
    to find any jobs, so we get an empty list.

    Then we filter by 6 hours, which returns half the list of jobs.

    Finally, we filter by 15 hours, which brings all ten jobs.
    """

    # FIXME: Here we are basically rewriting the logic in mocks because the
    #        original code is hard to test -- we need to re-think/design
    #        that so that our tests are simpler. We probably don't need a mock at all...
    def check_started_after(other: datetime) -> bool:
        """Start time of job hard-coded at 10h00"""
        start_time = datetime(2020, 5, 17, 10, 0, 0)
        return start_time > other

    jobs = []
    for i in range(10):
        job = mocker.MagicMock(spec=Job)
        job.check_started_after.side_effect = check_started_after
        job.check_running_after.return_value = False
        jobs.append(job)

    mocked_datetime = mocker.patch('autosubmit.statistics.utils.datetime', autospec=True)
    mocked_current_time = mocker.MagicMock()
    mocked_datetime.now.return_value = mocked_current_time
    mocked_current_time.replace.return_value = mocked_current_time

    def sub_fn(time_delta):
        """Subtract the delta from hard-coded current time 12h00"""
        current_time = datetime(2020, 5, 17, 12, 0, 0)
        return current_time - time_delta

    mocked_current_time.__sub__.side_effect = sub_fn

    # 1 hour, start time is 10h00, current is 12h00
    filtered, start, current = filter_by_time_period(jobs, 1)
    assert filtered is not None
    assert start is not None
    assert current is not None
    assert len(filtered) == 0

    # 10 hours, start time is 10h00, current is 12h00
    filtered, start, current = filter_by_time_period(jobs, 10)
    assert filtered is not None
    assert start is not None
    assert current is not None
    assert len(filtered) == len(jobs)


def test_timedelta2hours():
    delta_time = timedelta(days=1, seconds=3600)
    assert timedelta2hours(delta_time) == 25


@pytest.mark.parametrize(
    'n,expected',
    [
        (None, DEFAULT_NUMBER_PROCESSORS),
        ('', DEFAULT_NUMBER_PROCESSORS),
        ('   ', DEFAULT_NUMBER_PROCESSORS),
        ('0', DEFAULT_NUMBER_PROCESSORS),
        ('  0  ', DEFAULT_NUMBER_PROCESSORS),
        ('10', 10),
        ('1.1', 1),
        ('1:2:3', 108)
    ]
)
def test_parse_number_processors(n: Optional[str], expected: int):
    assert parse_number_processors(n) == expected
