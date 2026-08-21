# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Unit tests for the Autosubmit profiler."""

from types import SimpleNamespace

import pytest

from autosubmit.log.log import AutosubmitCritical
from autosubmit.profiler.profiler import (
    Profiler,
    ProfilerState,
    _generate_title,
    _get_current_memory,
    _get_current_object_count,
    _get_current_open_fds,
)


def test_profiler_initial_state():
    """Test that a newly created profiler is stopped.

    The profiler should start in the ``STOPPED`` state, with no checkpoints
    recorded and no maximum checkpoint limit configured.
    """
    profiler = Profiler("a001")

    assert profiler.stopped
    assert not profiler.started
    assert profiler._expid == "a001"
    assert profiler.max_checkpoints == 0
    assert profiler.checkpoints == 0


def test_profiler_start(mocker):
    """Test starting the profiler.

    :param mocker: Pytest mocker fixture used to replace the profiler,
        memory, and garbage collection operations.
    """
    profiler = Profiler("a001")

    mocked_profile = mocker.patch.object(profiler._profiler, "enable")
    mocked_memory = mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=12345,
    )
    mocked_gc = mocker.patch("autosubmit.profiler.profiler.gc.collect")

    profiler.start()

    assert profiler.started
    assert not profiler.stopped
    assert profiler._state == ProfilerState.STARTED
    assert profiler._mem_init == 12345

    mocked_profile.assert_called_once()
    mocked_memory.assert_called_once()
    mocked_gc.assert_called_once()


def test_profiler_start_twice_raises():
    """Test that starting an already started profiler raises an error.

    The profiler should raise ``AutosubmitCritical`` with error code 7074
    when ``start()`` is called while profiling is already active.
    """
    profiler = Profiler("a001")

    profiler.start()

    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.start()

    assert exc_info.value.code == 7074
    assert "already started" in exc_info.value.message


def test_profiler_stop_before_start_raises():
    """Test that stopping a profiler that has not been started raises an error.

    The profiler should raise ``AutosubmitCritical`` with error code 7074
    when ``stop()`` is called before profiling has started.
    """
    profiler = Profiler("a001")

    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.stop()

    assert exc_info.value.code == 7074
    assert "was not running" in exc_info.value.message


def test_profiler_stop(mocker):
    """Test stopping a running profiler.

    The profiler should disable the underlying ``cProfile`` profiler,
    generate the profiling report, and transition to the ``STOPPED`` state.

    :param mocker: Pytest mocker fixture used to replace profiler,
        memory, report, and garbage collection operations.
    """
    profiler = Profiler("a001")

    profiler.start()

    mocker.patch.object(profiler._profiler, "disable")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=20000,
    )
    mocked_report = mocker.patch.object(profiler, "_report")
    mocked_gc = mocker.patch("autosubmit.profiler.profiler.gc.collect")

    profiler.stop()

    assert profiler.stopped
    assert not profiler.started
    mocked_report.assert_called_once()
    mocked_gc.assert_not_called()


def test_iteration_checkpoint_records_values(mocker):
    """Test that an iteration checkpoint records all expected metrics.

    A checkpoint should record memory usage, object count, file descriptor
    information, and the number of loaded jobs and edges.

    :param mocker: Pytest mocker fixture used to provide deterministic
        metric values and disable garbage collection.
    """
    profiler = Profiler("a001")

    profiler.start()

    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1000,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_object_count",
        return_value=200,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds",
        return_value=10,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=["[fd=1] stdout"],
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")

    result = profiler.iteration_checkpoint(
        loaded_jobs=25,
        loaded_edges=40,
    )

    assert result is False

    assert len(profiler._mem_iteration) == 1
    assert profiler._mem_iteration[0] < 1000

    assert profiler._obj_iteration == [200]
    assert profiler._fd_iteration == [10]
    assert profiler._fd_names_iteration == [["[fd=1] stdout"]]
    assert profiler._jobs_iteration == [25]
    assert profiler._edges_iteration == [40]


def test_iteration_checkpoint_respects_max_checkpoints(mocker):
    """Test that the maximum checkpoint limit is respected.

    The profiler should return ``True`` once the number of checkpoints
    exceeds the configured maximum.

    :param mocker: Pytest mocker fixture used to provide deterministic
        metric values and disable garbage collection.
    """
    profiler = Profiler("a001", max_checkpoints=2)

    profiler.start()

    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1000,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_object_count",
        return_value=200,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds",
        return_value=10,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=[],
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")

    assert profiler.iteration_checkpoint(10, 20) is False
    assert profiler.checkpoints == 1

    assert profiler.iteration_checkpoint(11, 21) is False
    assert profiler.checkpoints == 2

    assert profiler.iteration_checkpoint(12, 22) is True
    assert profiler.checkpoints == 3


def test_calculate_grow():
    """Test calculation of memory, object, and file descriptor growth.

    Growth between consecutive checkpoints should be calculated correctly,
    as well as the total growth between the first and last checkpoints.
    """
    profiler = Profiler("a001")

    profiler._mem_iteration = [100, 150, 180]
    profiler._obj_iteration = [10, 15, 21]
    profiler._fd_iteration = [4, 5, 7]

    profiler._calculate_grow()

    assert profiler._mem_grow == [50, 30]
    assert profiler._obj_grow == [5, 6]
    assert profiler._fd_grow == [1, 2]

    assert profiler._mem_total_grow == 80
    assert profiler._obj_total_grow == 11
    assert profiler._fd_total_grow == 3


def test_calculate_grow_uses_fourth_checkpoint_for_objects_and_fds():
    """Test growth calculation after the fourth checkpoint.

    Object and file descriptor growth should use the fourth checkpoint as
    the baseline when more than three checkpoints have been recorded.
    Memory growth continues to use the first checkpoint as its baseline.
    """
    profiler = Profiler("a001")

    profiler._mem_iteration = [100, 150, 180, 200, 250]
    profiler._obj_iteration = [10, 15, 21, 30, 42]
    profiler._fd_iteration = [4, 5, 7, 8, 11]
    profiler.checkpoints = 4

    profiler._calculate_grow()

    assert profiler._mem_total_grow == 150
    assert profiler._obj_total_grow == 12
    assert profiler._fd_total_grow == 3


def test_format_top_allocations_empty():
    """Test formatting when no allocation statistics are available.

    :return: An empty string should be returned when there are no
        allocation statistics.
    """
    profiler = Profiler("a001")

    assert profiler._format_top_allocations([]) == ""


def test_format_top_allocations():
    """Test formatting of tracemalloc allocation statistics.

    The formatted output should contain the source location, allocation
    size, and number of allocated blocks.
    """
    profiler = Profiler("a001")

    frame = SimpleNamespace(
        filename="/tmp/example.py",
        lineno=42,
    )
    stat = SimpleNamespace(
        traceback=[frame],
        size_diff=2048,
        count_diff=3,
    )

    result = profiler._format_top_allocations([stat])

    assert "Top allocation deltas:" in result
    assert "/tmp/example.py:42" in result
    assert "+2.0 KiB" in result
    assert "(+3 blocks)" in result


def test_generate_title():
    """Test generation of an 80-character profiling report title.

    :return: The generated title should contain two separator lines and
        a centered title.
    """
    result = _generate_title("Test Title")

    lines = result.splitlines()

    assert len(lines) == 3
    assert lines[0] == "=" * 80
    assert lines[1] == "Test Title".center(80)
    assert lines[2] == "=" * 80


def test_get_current_memory(mocker):
    """Test retrieval of the current process memory usage.

    :param mocker: Pytest mocker fixture used to replace ``psutil.Process``.
    """
    mocked_process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )
    mocked_process.return_value.memory_info.return_value.rss = 123456

    result = _get_current_memory()

    assert result == 123456
    mocked_process.assert_called_once()


def test_get_current_object_count(mocker):
    """Test retrieval of the number of tracked Python objects.

    :param mocker: Pytest mocker fixture used to replace
        ``gc.get_objects()``.
    """
    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=[object(), object(), object()],
    )

    assert _get_current_object_count() == 3


def test_get_current_open_fds_num_fds(mocker):
    """Test retrieval of open file descriptors using ``num_fds``.

    This is the normal code path on platforms where ``psutil.Process``
    provides the ``num_fds`` method.

    :param mocker: Pytest mocker fixture used to replace
        ``psutil.Process``.
    """
    mocked_process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )
    mocked_process.return_value.num_fds.return_value = 7

    assert _get_current_open_fds() == 7


def test_get_current_open_fds_num_handles(mocker):
    """Test retrieval of open handles using ``num_handles``.

    This covers the fallback used on platforms where ``num_fds`` is not
    available.

    :param mocker: Pytest mocker fixture used to replace
        ``psutil.Process``.
    """
    mocked_process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )

    del mocked_process.return_value.num_fds

    mocked_process.return_value.num_handles.return_value = 9

    assert _get_current_open_fds() == 9


def test_trace_enabled_starts_tracemalloc(mocker):
    """Test that trace-enabled profiling starts ``tracemalloc``.

    When allocation tracing is enabled and ``tracemalloc`` is not already
    running, starting the profiler should start allocation tracing.

    :param mocker: Pytest mocker fixture used to replace ``tracemalloc``,
        memory, and garbage collection operations.
    """
    profiler = Profiler("a001", trace_enabled=True)

    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=False,
    )
    mocked_start = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.start",
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1000,
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")

    profiler.start()

    mocked_start.assert_called_once()
    assert profiler.started


def test_trace_enabled_stops_tracemalloc(mocker):
    """Test that trace-enabled profiling stops ``tracemalloc`` on shutdown.

    When allocation tracing is enabled and ``tracemalloc`` is running,
    stopping the profiler should stop allocation tracing after generating
    the report.

    :param mocker: Pytest mocker fixture used to replace ``tracemalloc``,
        the profiler's report generation, and the underlying profiler.
    """
    profiler = Profiler("a001", trace_enabled=True)

    profiler.start()

    mocker.patch.object(profiler._profiler, "disable")
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=True,
    )
    mocked_stop = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.stop",
    )
    mocker.patch.object(profiler, "_report")

    profiler.stop()

    mocked_stop.assert_called_once()
    assert profiler.stopped
