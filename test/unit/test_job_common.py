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
