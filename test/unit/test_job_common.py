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
import pytest

from autosubmit.job import job_common
from autosubmit.job.job_common import Status

"""This test is intended to prevent wrong changes on the Status class definition."""


def test_value_to_key_has_the_same_values_as_status_constants():
    assert 'SUSPENDED' == Status.VALUE_TO_KEY[Status.SUSPENDED]
    assert 'UNKNOWN' == Status.VALUE_TO_KEY[Status.UNKNOWN]
    assert 'FAILED' == Status.VALUE_TO_KEY[Status.FAILED]
    assert 'WAITING' == Status.VALUE_TO_KEY[Status.WAITING]
    assert 'READY' == Status.VALUE_TO_KEY[Status.READY]
    assert 'SUBMITTED' == Status.VALUE_TO_KEY[Status.SUBMITTED]
    assert 'HELD' == Status.VALUE_TO_KEY[Status.HELD]
    assert 'QUEUING' == Status.VALUE_TO_KEY[Status.QUEUING]
    assert 'RUNNING' == Status.VALUE_TO_KEY[Status.RUNNING]
    assert 'COMPLETED' == Status.VALUE_TO_KEY[Status.COMPLETED]


def test_status_active_and_rerunnable_groups():
    """Test the ACTIVE and RE_RUNNABLE status groups."""
    assert Status.ACTIVE == (Status.SUBMITTED, Status.QUEUING, Status.RUNNING)
    assert Status.RE_RUNNABLE == (Status.WAITING, Status.READY, Status.DELAYED, Status.PREPARED, Status.SUSPENDED)
    assert set(Status.ACTIVE).isdisjoint(Status.RE_RUNNABLE)


@pytest.mark.parametrize(
    'status_str, expected',
    [
        ('SUBMITTED', Status.SUBMITTED),
        ('submitted', Status.SUBMITTED),
        ('READY', Status.READY),
        ('NONEXISTENT', None),
    ],
    ids=['submitted', 'lowercase', 'ready', 'unknown-status']
)
def test_get_job_status_submitted(status_str, expected):
    """Test that get_job_status parses the SUBMITTED status."""
    assert expected == job_common.get_job_status(status_str)


@pytest.mark.parametrize(
    'number,result',
    [
        ('1G', 1000000000),
        ('1M', 1000000),
        ('1K', 1000),
        ('1', 1),
        ('Not a Number', 0.0),
    ],
    ids=['G', 'M', 'K', 'Any', 'Not a Number']
)
def test_parse_output_number(number, result):
    assert result == job_common.parse_output_number(number)


@pytest.mark.parametrize(
    'wallclock, expected',
    [
        ('07:30', 27000),
        ('07:30:15', 27015),
        ('00:00', 0),
        ('00:00:00', 0),
        ('0:05', 300),
        (None, None),
        ('', None),
        ('not-a-time', None),
        (10, None),
    ],
    ids=[
        'hh-mm', 'hh-mm-ss', 'zero', 'zero-with-seconds', 'single-digit-hours',
        'none', 'empty', 'unparseable', 'not-a-string'
    ]
)
def test_wallclock_to_seconds(wallclock, expected):
    """Test the wallclock_to_seconds function."""
    assert expected == job_common.wallclock_to_seconds(wallclock)


@pytest.mark.parametrize(
    'wallclocks, platform_max_wallclock, fallback, expected',
    [
        (['00:10', '00:30', '01:00'], '24:00', 0, 3600),
        (['00:10', '00:30'], '24:00', 0, 1800),
        (['00:00', '00:00'], '24:00', 0, 86400),
        (['00:00', ''], None, 120, 120),
        (['00:10', 'bad'], '02:00', 0, 7200),
        ([], None, 45, 45),
    ],
    ids=[
        'longest-wins', 'minutes', 'platform-max-fallback', 'no-platform-all-fallback',
        'unparseable-entry-falls-back-to-platform', 'empty-collection-uses-fallback'
    ]
)
def test_max_wallclock_seconds(wallclocks, platform_max_wallclock, fallback, expected):
    """Test the max_wallclock_seconds function."""
    assert expected == job_common.max_wallclock_seconds(wallclocks, platform_max_wallclock, fallback)
