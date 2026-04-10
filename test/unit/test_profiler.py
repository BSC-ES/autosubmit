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
from typing import Optional

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.log.log import AutosubmitCritical
from autosubmit.profiler.profiler import Profiler


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

    profiler.iteration_checkpoint(0, 0)

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


@pytest.mark.parametrize(
    "argv, expected_profile, expected_trace",
    [
        (["autosubmit", "run", "a000"], None, False),
        (["autosubmit", "run", "a000", "--profile"], 0, False),
        (["autosubmit", "run", "a000", "--profile", "3"], 3, False),
        (["autosubmit", "run", "a000", "--profile", "--trace"], 0, True),
    ],
)
def test_run_command_forwards_profile_arguments(
        argv: list[str],
        expected_profile: Optional[int],
        expected_trace: bool,
        mocker,
) -> None:
    mocker.patch("sys.argv", argv)
    mocked_run = mocker.patch(
        "autosubmit.autosubmit.Autosubmit.run_experiment",
        return_value=0,
    )
    mocker.patch("autosubmit.autosubmit.Autosubmit._init_logs", return_value=None)

    status, args = Autosubmit.parse_args()

    assert status == 0
    assert args is not None

    Autosubmit.run_command(args)

    mocked_run.assert_called_once_with(
        "a000",
        None,
        None,
        None,
        expected_profile,
        expected_trace,
    )


def test_run_command_rejects_trace_without_profile(mocker) -> None:
    mocker.patch("sys.argv", ["autosubmit", "run", "a000", "--trace"])
    mocker.patch("autosubmit.autosubmit.Autosubmit._init_logs", return_value=None)

    status, args = Autosubmit.parse_args()

    assert status == 0
    assert args is not None

    with pytest.raises(AutosubmitCritical) as exc_info:
        Autosubmit.run_command(args)

    assert exc_info.value.code == 7012
