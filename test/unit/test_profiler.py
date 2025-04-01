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

from autosubmit.profiler.profiler import Profiler
from log.log import AutosubmitCritical


@pytest.fixture
def profiler():
    """ Creates a profiler object and yields it to the test. """
    yield Profiler("a000")


# Black box techniques for status machine based software
#
#   O--->__init__----> start
#                           |
#                           |
#                         stop (----> report) --->0

# Transition coverage
def test_transitions(profiler):
    # __init__ -> start
    profiler.start()

    # start -> stop
    profiler.stop()


def test_transitions_fail_cases(profiler):
    # __init__ -> stop
    with pytest.raises(AutosubmitCritical):
        profiler.stop()

    # start -> start
    profiler.start()
    with pytest.raises(AutosubmitCritical):
        profiler.start()

    # stop -> stop
    profiler.stop()
    with pytest.raises(AutosubmitCritical):
        profiler.stop()


# White box tests
def test_writing_permission_check_fails(profiler, mocker):
    mocker.patch("os.access", return_value=False)

    profiler.start()
    with pytest.raises(AutosubmitCritical):
        profiler.stop()


def test_memory_profiling_loop(profiler):
    profiler.start()
    bytearray(1024 * 1024)
    profiler.stop()
