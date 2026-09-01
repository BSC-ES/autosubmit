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

import autosubmit.job.job_common as job_common
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
        ('00:10', 600),
        ('07:30', 27000),
        ('07:30:00', 27000),
        ('24:00', 86400),
        ('00:00', 0),
        ('', None),
        (None, None),
        ('garbage', None),
        ('0000', None),
        (123, None),
    ],
    ids=['minutes', 'hour-minute', 'with-seconds', 'day', 'zero', 'empty', 'none',
         'garbage', 'no-colon', 'non-string']
)
def test_wallclock_to_seconds(wallclock, expected):
    assert job_common.wallclock_to_seconds(wallclock) == expected


@pytest.mark.parametrize(
    'wallclocks, platform_max_wallclock, fallback, expected',
    [
        (['00:10', '00:30', '00:15'], None, 0, 1800),
        (['00:00', '', None], '24:00', 0, 86400),
        (['00:00', ''], None, 42, 42),
        ([], None, 42, 42),
    ],
    ids=['longest-section', 'platform-fallback', 'fallback-arg', 'empty']
)
def test_max_wallclock_seconds(wallclocks, platform_max_wallclock, fallback, expected):
    assert job_common.max_wallclock_seconds(
        wallclocks, platform_max_wallclock=platform_max_wallclock, fallback=fallback) == expected
