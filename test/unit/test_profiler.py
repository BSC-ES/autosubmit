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

import gc
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from autosubmit.log.log import AutosubmitCritical
from autosubmit.profiler import profiler as profiler_module

# noinspection PyProtectedMember
from autosubmit.profiler.profiler import (
    Profiler,
    ProfilerState,
    _generate_title,
    _get_current_memory,
    _get_current_object_count,
    _get_current_open_fds,
    _get_current_open_fds_names,
    _get_fd_connection_map,
    _get_pipe_direction,
    _now,
)


@pytest.fixture
def profiler(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a000",
    )

    # Unit tests must never execute or depend on real cProfile data.
    # noinspection PyProtectedMember
    mocker.patch.object(profiler._profiler, "enable")
    # noinspection PyProtectedMember
    mocker.patch.object(profiler._profiler, "disable")
    # noinspection PyProtectedMember
    mocker.patch.object(profiler._profiler, "dump_stats")

    class FakeStats:
        def __init__(self, _, stream=None):
            self.stream = stream

        def strip_dirs(self):
            return self

        def sort_stats(self, *_, **__):
            return self

        def print_stats(self, *_, **__):
            if self.stream is not None:
                self.stream.write("Fake cProfile statistics\n")

        # noinspection PyMethodMayBeStatic
        def dump_stats(self, filename):
            Path(filename).touch()

    mocker.patch(
        "autosubmit.profiler.profiler.pstats.Stats",
        FakeStats,
    )

    yield profiler

    # Do not let fixture clean-up invoke the real reporting machinery.
    if profiler.started:
        profiler._state = ProfilerState.STOPPED


@pytest.fixture
def profiled_profiler(profiler):
    # noinspection PyProtectedMember
    profiler._profiler.enable()
    # noinspection PyProtectedMember
    profiler._profiler.disable()
    return profiler


@pytest.fixture
def started_profiler(profiler, mocker):
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1000,
    )
    profiler.start()
    return profiler


@pytest.fixture
def checkpoint_mocks(mocker):
    mocker.patch("gc.collect")
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


@pytest.fixture
def report_setup(profiler, mocker, tmp_path):
    report_path = tmp_path / "profile"

    mocker.patch.object(
        type(profiler),
        "report_path",
        new_callable=mocker.PropertyMock,
        return_value=report_path,
    )
    mocker.patch.object(
        type(profiler),
        "file_name",
        new_callable=mocker.PropertyMock,
        return_value="profile.prof",
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.access",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=[],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.Log.info",
    )

    return report_path


def test_initial_state(profiler):
    assert profiler.stopped
    assert not profiler.started
    assert profiler._state == ProfilerState.STOPPED
    assert profiler.checkpoints == 0


def test_now_format():
    value = _now()

    assert len(value) == 15
    assert value[8] == "-"
    assert value[:8].isdigit()
    assert value[9:].isdigit()


def test_generate_title():
    result = _generate_title("Test")
    lines = result.splitlines()

    assert len(lines) == 3
    assert all(len(line) == 80 for line in lines)
    assert lines[0] == "=" * 80
    assert lines[1].strip() == "Test"
    assert lines[2] == "=" * 80


def test_generate_title_without_title():
    result = _generate_title()

    assert len(result.splitlines()) == 3
    assert all(len(line) == 80 for line in result.splitlines())


def test_file_name_with_expid(profiler, mocker):
    mocker.patch(
        "autosubmit.profiler.profiler._now",
        return_value="20260101-120000",
    )

    assert profiler.file_name == "a000_run_profile_20260101-120000.prof"


def test_file_name_without_expid(mocker):
    mocker.patch(
        "autosubmit.profiler.profiler._now",
        return_value="20260101-120000",
    )

    profiler = Profiler(
        subcommand="run",
        expid=None,
    )

    assert profiler.file_name == "run_profile_20260101-120000.prof"


def test_report_path_with_expid(profiler):
    """Test that the profiler report path uses the experiment directory.

    :param profiler: Profiler fixture configured with an experiment identifier.
    :return: None.
    :raises AssertionError: If the generated report path is incorrect.
    """
    # noinspection PyProtectedMember
    expected = (
        Path(profiler_module.BasicConfig.LOCAL_ROOT_DIR)
        / profiler._expid  # type: ignore
        / "tmp"
        / "profile"
    )

    assert profiler.report_path == expected


def test_report_path_without_expid():
    """Test that the profiler report path uses the global profile directory.

    :return: None.
    :raises AssertionError: If the generated report path is incorrect.
    """
    profiler = Profiler(
        subcommand="run",
        expid=None,
    )

    expected = Path(profiler_module.BasicConfig.GLOBAL_LOG_DIR) / "profile"

    assert profiler.report_path == expected


def test_start(profiler, mocker):
    enable = mocker.patch.object(profiler._profiler, "enable")
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=123,
    )

    profiler.start()

    enable.assert_called_once()
    assert profiler.started
    assert profiler._mem_init == 123


def test_start_twice_fails(profiler, mocker):
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=123,
    )

    profiler.start()

    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.start()

    assert exc_info.value.code == 7074


def test_stop_before_start_fails(profiler):
    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.stop()

    assert exc_info.value.code == 7074


def test_stop_twice_fails(profiler, mocker):
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=123,
    )
    mocker.patch.object(profiler._profiler, "disable")
    mocker.patch.object(profiler, "_report")

    profiler.start()
    profiler.stop()

    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.stop()

    assert exc_info.value.code == 7074


def test_stop_disables_profiler(started_profiler, mocker):
    disable = mocker.patch.object(started_profiler._profiler, "disable")
    mocker.patch.object(started_profiler, "_report")

    started_profiler.stop()

    disable.assert_called_once()
    assert started_profiler.stopped


def test_start_enables_tracemalloc(profiler, mocker):
    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=100,
    )
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=False,
    )
    start = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.start",
    )

    profiler._trace_enabled = True
    profiler.start()

    start.assert_called_once()
    assert profiler._trace_started is True


def test_start_does_not_restart_tracemalloc(profiler, mocker):
    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=100,
    )
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=True,
    )
    start = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.start",
    )

    profiler._trace_enabled = True
    profiler.start()

    start.assert_not_called()
    assert profiler._trace_started is False


def test_stop_stops_tracemalloc(profiler, mocker):
    profiler._trace_enabled = True

    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        side_effect=[100, 200],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        side_effect=[False, True],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.start",
    )
    stop = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.stop",
    )
    mocker.patch.object(profiler._profiler, "disable")
    mocker.patch.object(profiler, "_report")

    profiler.start()
    profiler.stop()

    stop.assert_called_once()


def test_iteration_checkpoint(
    started_profiler,
    checkpoint_mocks,
):
    result = started_profiler.iteration_checkpoint(
        loaded_jobs=5,
        loaded_edges=7,
    )

    assert result is False
    assert started_profiler._obj_iteration == [200]
    assert started_profiler._fd_iteration == [10]
    assert started_profiler._fd_names_iteration == [["[fd=1] stdout"]]
    assert started_profiler._jobs_iteration == [5]
    assert started_profiler._edges_iteration == [7]


def test_iteration_checkpoint_calls_gc(
    started_profiler,
    checkpoint_mocks,
):
    collect = gc.collect

    started_profiler.iteration_checkpoint(1, 2)

    assert collect.called  # type: ignore


def test_iteration_checkpoint_max_checkpoints(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a000",
        max_checkpoints=2,
    )

    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch.object(profiler._profiler, "disable")
    mocker.patch.object(profiler, "_report")

    profiler.start()

    mocker.patch("gc.collect")
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

    assert profiler.iteration_checkpoint(1, 1) is False
    assert profiler.iteration_checkpoint(1, 1) is False
    assert profiler.iteration_checkpoint(1, 1) is True
    assert profiler.checkpoints == 3


def test_iteration_checkpoint_unlimited(started_profiler, checkpoint_mocks):
    for _ in range(10):
        assert started_profiler.iteration_checkpoint(1, 1) is False

    assert started_profiler.checkpoints == 0


def test_checkpoint_takes_snapshot(profiler, mocker):
    profiler._trace_enabled = True

    mocker.patch("gc.collect")
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
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=True,
    )

    snapshot = MagicMock()
    take_snapshot = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.take_snapshot",
        return_value=snapshot,
    )
    capture = mocker.patch.object(
        profiler,
        "_capture_allocation_delta",
        return_value=["stat"],
    )

    profiler.start()
    profiler.iteration_checkpoint(1, 2)

    take_snapshot.assert_called_once()
    capture.assert_called_once_with(snapshot)
    assert profiler._trace_snapshots == [snapshot]
    assert profiler._trace_stats_by_iter == [["stat"]]


def test_checkpoint_does_not_take_snapshot(
    started_profiler,
    checkpoint_mocks,
    mocker,
):
    take_snapshot = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.take_snapshot",
    )

    started_profiler.iteration_checkpoint(1, 2)

    take_snapshot.assert_not_called()


def test_capture_allocation_delta_without_previous_snapshot(profiler):
    assert profiler._capture_allocation_delta(MagicMock()) == []


def test_capture_allocation_delta_returns_positive_stats(profiler):
    previous = MagicMock()
    current = MagicMock()

    stats = [
        SimpleNamespace(size_diff=100),
        SimpleNamespace(size_diff=-20),
        SimpleNamespace(size_diff=50),
        SimpleNamespace(size_diff=10),
        SimpleNamespace(size_diff=5),
        SimpleNamespace(size_diff=4),
        SimpleNamespace(size_diff=3),
    ]

    current.compare_to.return_value = stats
    profiler._trace_snapshots.append(previous)

    assert profiler._capture_allocation_delta(current) == [
        stats[0],
        stats[2],
        stats[3],
        stats[4],
        stats[5],
    ]


def test_calculate_grow(profiler):
    profiler._mem_iteration = [100, 150, 190, 250]
    profiler._obj_iteration = [10, 15, 20, 30]
    profiler._fd_iteration = [3, 4, 5, 7]
    profiler.checkpoints = 3

    profiler._calculate_grow()

    assert profiler._mem_grow == [50, 40, 60]
    assert profiler._obj_grow == [5, 5, 10]
    assert profiler._fd_grow == [1, 1, 2]
    assert profiler._mem_total_grow == 150
    assert profiler._obj_total_grow == 20
    assert profiler._fd_total_grow == 4


def test_calculate_grow_after_three_checkpoints(profiler):
    """Test that object and FD growth starts after the first three checkpoints.

    :param profiler: Profiler fixture used to calculate growth metrics.
    :return: None.
    :raises AssertionError: If growth is calculated from the wrong checkpoint.
    """
    profiler._mem_iteration = [100, 150, 190, 250, 310]
    profiler._obj_iteration = [10, 15, 20, 30, 45]
    profiler._fd_iteration = [3, 4, 5, 7, 9]
    profiler.checkpoints = 4

    profiler._calculate_grow()

    assert profiler._mem_grow == [50, 40, 60, 60]
    assert profiler._obj_grow == [5, 5, 10, 15]
    assert profiler._fd_grow == [1, 1, 2, 2]
    assert profiler._mem_total_grow == 210
    assert profiler._obj_total_grow == 15
    assert profiler._fd_total_grow == 2


def test_calculate_grow_with_three_checkpoints():
    profiler = Profiler(
        subcommand="run",
        expid="a000",
    )

    profiler._mem_iteration = [100, 150, 190]
    profiler._obj_iteration = [10, 15, 20]
    profiler._fd_iteration = [3, 4, 5]
    profiler.checkpoints = 3

    profiler._calculate_grow()

    assert profiler._mem_grow == [50, 40]
    assert profiler._obj_grow == [5, 5]
    assert profiler._fd_grow == [1, 1]
    assert profiler._mem_total_grow == 90
    assert profiler._obj_total_grow == 10
    assert profiler._fd_total_grow == 2


def test_calculate_grow_empty(profiler):
    profiler._calculate_grow()

    assert profiler._mem_grow == []
    assert profiler._obj_grow == []
    assert profiler._fd_grow == []
    assert profiler._mem_total_grow == 0
    assert profiler._obj_total_grow == 0
    assert profiler._fd_total_grow == 0


def test_format_top_allocations_empty(profiler):
    assert profiler._format_top_allocations([]) == ""


def test_format_top_allocations(profiler):
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


def test_report_grow():
    profiler = Profiler(
        subcommand="run",
        expid="a000",
    )

    profiler._mem_iteration = [1024, 2048, 4096]
    profiler._obj_iteration = [10, 20, 30]
    profiler._fd_iteration = [3, 4, 5]
    profiler._fd_names_iteration = [
        ["[fd=1] stdout"],
        ["[fd=1] stdout", "[fd=5] foo"],
        ["[fd=1] stdout"],
    ]
    profiler._jobs_iteration = [1, 2, 3]
    profiler._edges_iteration = [5, 6, 7]
    profiler._trace_stats_by_iter = [[], []]

    result = profiler._report_grow()

    assert "Iteration 1:" in result
    assert "Memory:" in result
    assert "Objects: 10" in result
    assert "File Descriptors: 3" in result
    assert "Loaded jobs: 1" in result
    assert "Loaded edges: 5" in result


def test_report_grow_reports_fd_changes(profiler):
    profiler._mem_iteration = [1000, 2000, 3000, 4000]
    profiler._obj_iteration = [10, 20, 30, 40]
    profiler._fd_iteration = [2, 3, 2, 2]
    profiler._fd_names_iteration = [
        ["[fd=1] stdout", "[fd=2] old"],
        ["[fd=1] stdout", "[fd=3] new"],
        ["[fd=1] stdout"],
        ["[fd=1] stdout"],
    ]
    profiler._jobs_iteration = [1, 2, 3, 4]
    profiler._edges_iteration = [1, 2, 3, 4]
    profiler._trace_stats_by_iter = [[], [], [], []]

    result = profiler._report_grow()

    assert "Iteration 2: Opened file descriptor: [fd=3] new" in result
    assert "Iteration 2: Closed file descriptor: [fd=2] old" in result


def test_report_grow_includes_allocation_statistics():
    profiler = Profiler(
        subcommand="run",
        expid="a000",
    )

    profiler._mem_iteration = [1000, 2000, 3000]
    profiler._obj_iteration = [10, 20, 30]
    profiler._fd_iteration = [2, 3, 4]
    profiler._fd_names_iteration = [[], [], []]
    profiler._jobs_iteration = [1, 2, 3]
    profiler._edges_iteration = [1, 2, 3]

    frame = SimpleNamespace(
        filename="/tmp/example.py",
        lineno=10,
    )
    stat = SimpleNamespace(
        traceback=[frame],
        size_diff=4096,
        count_diff=2,
    )

    profiler._trace_stats_by_iter = [[stat], []]

    result = profiler._report_grow()

    assert "Top allocation deltas:" in result
    assert "/tmp/example.py:10" in result


def test_report_creates_files(profiled_profiler, report_setup):
    profiler = profiled_profiler
    profiler._mem_init = 100
    profiler._mem_final = 200
    profiler._mem_iteration = [100, 200, 300]
    profiler._obj_grow = [1, 1]
    profiler._fd_grow = [1, 1]
    profiler._obj_total_grow = 2
    profiler._fd_total_grow = 2

    profiler._report()

    assert report_setup.exists()
    assert (report_setup / "profile.prof").exists()
    assert (report_setup / "profile.txt").exists()

    report = (report_setup / "profile.txt").read_text(encoding="UTF-8")

    assert "Time & Calls Profiling" in report
    assert "Overall Memory, Object and File Descriptor Growth" in report
    assert "FINAL OPEN FILE DESCRIPTORS:" in report


def test_report_includes_growth_and_converts_memory_units(
    profiled_profiler,
    report_setup,
    mocker,
):
    """Test that the report includes growth details and converts byte values.

    :param profiled_profiler: Profiler with cProfile data already collected.
    :param report_setup: Fixture configuring the temporary report directory.
    :param mocker: pytest-mock fixture used to provide final FD names.
    :return: None.
    :raises AssertionError: If growth, memory conversion, or FD reporting is missing.
    """
    profiler = profiled_profiler
    profiler._mem_init = 2 * 1024**3
    profiler._mem_final = 5 * 1024**3
    profiler._mem_iteration = [1024, 2048, 4096]
    profiler._obj_iteration = [10, 20, 30]
    profiler._fd_iteration = [2, 3, 4]
    profiler._fd_names_iteration = [[], [], []]
    profiler._jobs_iteration = [1, 2, 3]
    profiler._edges_iteration = [1, 2, 3]

    profiler._mem_grow = [1024]
    profiler._obj_grow = [10]
    profiler._fd_grow = [1]
    profiler._obj_total_grow = 20
    profiler._fd_total_grow = 2

    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=["[fd=9] /tmp/example.txt"],
    )

    profiler._report()

    report = (report_setup / "profile.txt").read_text(encoding="UTF-8")

    assert "Memory, object and file descriptor by iteration" in report
    assert "MEMORY GROW: 3.00 GiB." in report
    assert "INITIAL MEMORY: 2.00 GiB." in report
    assert "FINAL MEMORY: 5.00 GiB." in report
    assert "OBJECTS GROW: 20 objects." in report
    assert "FILE DESCRIPTORS GROW: 2 file descriptors." in report
    assert "[fd=9] /tmp/example.txt" in report


def test_report_includes_traceback_information(
    profiled_profiler,
    report_setup,
    mocker,
):
    """Test that the report includes final file descriptors and object tracebacks.

    :param profiled_profiler: Profiler with cProfile data already collected.
    :param report_setup: Fixture configuring the temporary report directory.
    :param mocker: pytest-mock fixture used to provide final FD names.
    :return: None.
    :raises AssertionError: If traced objects or final file descriptors are omitted.
    """
    profiler = profiled_profiler
    profiler._mem_init = 100
    profiler._mem_final = 200
    profiler._trace_enabled = True

    traceback = (MagicMock(),)
    profiler._obj_diffs_between_iter.add(traceback)

    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=["[fd=8] socket: example"],
    )

    profiler._report()

    report = (report_setup / "profile.txt").read_text(encoding="UTF-8")

    assert "FINAL OPEN FILE DESCRIPTORS:" in report
    assert "[fd=8] socket: example" in report
    assert "Unique object tracebacks between iterations:" in report
    assert str(traceback) in report


def test_report_fails_when_directory_is_not_writable(
    profiler,
    mocker,
    tmp_path,
):
    report_path = tmp_path / "profile"

    mocker.patch.object(
        type(profiler),
        "report_path",
        new_callable=mocker.PropertyMock,
        return_value=report_path,
    )
    mocker.patch("os.access", return_value=False)

    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler._report()

    assert exc_info.value.code == 7012


def test_report_without_iterations(
    profiled_profiler,
    report_setup,
):
    profiler = profiled_profiler
    profiler._mem_init = 100
    profiler._mem_final = 200

    profiler._report()

    report = (report_setup / "profile.txt").read_text(encoding="UTF-8")

    assert "MEMORY GROW:" in report
    assert "INITIAL MEMORY:" in report
    assert "FINAL MEMORY:" in report


def test_stop_uses_iteration_memory(profiler, mocker):
    profiler.start()

    profiler._mem_iteration = [100, 150, 200]
    profiler._obj_iteration = [10, 20, 30]
    profiler._fd_iteration = [2, 3, 4]

    calculate = mocker.patch.object(
        profiler,
        "_calculate_grow",
    )
    mocker.patch.object(
        profiler._profiler,
        "disable",
    )
    mocker.patch.object(
        profiler,
        "_report",
    )

    profiler.stop()

    calculate.assert_called_once()

    # noinspection PyProtectedMember
    assert profiler._mem_init == 100

    # noinspection PyUnreachableCode
    assert profiler._mem_final == 200


def test_stop_without_iterations_uses_current_memory(
    profiler,
    mocker,
):
    mocker.patch("gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        side_effect=[100, 500],
    )
    mocker.patch.object(
        profiler._profiler,
        "disable",
    )
    mocker.patch.object(
        profiler,
        "_report",
    )

    profiler.start()
    profiler.stop()

    assert profiler._mem_final == 500


def test_get_current_memory(mocker):
    process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )
    process.return_value.memory_info.return_value = SimpleNamespace(
        rss=123456,
    )

    assert _get_current_memory() == 123456
    process.assert_called_once_with(os.getpid())


def test_get_current_object_count(mocker):
    objects = mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=[1, 2, 3],
    )

    assert _get_current_object_count() == 3
    objects.assert_called_once()


def test_get_current_open_fds(mocker):
    process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )
    process.return_value.num_fds.return_value = 42

    assert _get_current_open_fds() == 42


def test_get_current_open_handles_when_num_fds_unavailable(
    mocker,
):
    process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )
    proc = process.return_value

    del proc.num_fds
    proc.num_handles.return_value = 17

    assert _get_current_open_fds() == 17


def test_get_current_open_fds_returns_none_when_unsupported(
    mocker,
):
    process = mocker.patch(
        "autosubmit.profiler.profiler.Process",
    )
    proc = process.return_value

    del proc.num_fds
    del proc.num_handles

    assert _get_current_open_fds() is None


def test_get_fd_connection_map_unix_socket():
    proc = MagicMock()
    proc.net_connections.return_value = [
        SimpleNamespace(
            fd=5,
            family=profiler_module._socket.AF_UNIX,
            laddr=SimpleNamespace(path="/tmp/test.sock"),
        )
    ]

    assert _get_fd_connection_map(proc) == {
        5: "unix-socket: /tmp/test.sock",
    }


def test_get_fd_connection_map_unnamed_unix_socket():
    proc = MagicMock()
    proc.net_connections.return_value = [
        SimpleNamespace(
            fd=5,
            family=profiler_module._socket.AF_UNIX,
            laddr=SimpleNamespace(path=""),
        )
    ]

    assert _get_fd_connection_map(proc) == {
        5: "unix-socket: (unnamed)",
    }


def test_get_fd_connection_map_inet_socket():
    proc = MagicMock()
    proc.net_connections.return_value = [
        SimpleNamespace(
            fd=7,
            family=profiler_module._socket.AF_INET,
            laddr=SimpleNamespace(
                ip="127.0.0.1",
                port=1234,
            ),
            raddr=SimpleNamespace(
                ip="127.0.0.1",
                port=5678,
            ),
            status="ESTABLISHED",
        )
    ]

    assert _get_fd_connection_map(proc) == {
        7: "socket: 127.0.0.1:1234 -> 127.0.0.1:5678 [ESTABLISHED]",
    }


def test_get_fd_connection_map_without_remote_address():
    proc = MagicMock()
    proc.net_connections.return_value = [
        SimpleNamespace(
            fd=7,
            family=profiler_module._socket.AF_INET,
            laddr=SimpleNamespace(
                ip="0.0.0.0",
                port=8080,
            ),
            raddr=None,
            status="LISTEN",
        )
    ]

    assert _get_fd_connection_map(proc) == {
        7: "socket: 0.0.0.0:8080 [LISTEN]",
    }


def test_get_fd_connection_map_skips_negative_fd():
    proc = MagicMock()
    proc.net_connections.return_value = [
        SimpleNamespace(
            fd=-1,
            family=profiler_module._socket.AF_INET,
            laddr=None,
            raddr=None,
            status="",
        )
    ]

    assert _get_fd_connection_map(proc) == {}


def test_get_fd_connection_map_handles_error():
    proc = MagicMock()
    proc.net_connections.side_effect = RuntimeError("psutil failure")

    assert _get_fd_connection_map(proc) == {}


@pytest.mark.parametrize(
    ("flags", "expected"),
    [
        ("00000000", "read"),
        ("00000001", "write"),
        ("00000002", "write"),
    ],
    ids=["read", "write_low", "write_high"],
)
def test_get_pipe_direction(
    mocker,
    flags,
    expected,
):
    mocker.patch(
        "builtins.open",
        mocker.mock_open(read_data=f"flags:\t{flags}\n"),
    )

    assert _get_pipe_direction(123, 4) == expected


def test_get_pipe_direction_without_flags(mocker):
    mocker.patch(
        "builtins.open",
        mocker.mock_open(read_data="position:\t0\n"),
    )

    assert _get_pipe_direction(123, 4) == "unknown"


def test_get_pipe_direction_handles_error(mocker):
    mocker.patch(
        "builtins.open",
        side_effect=OSError,
    )

    assert _get_pipe_direction(123, 4) == "unknown"


def test_get_current_open_fds_names(mocker):
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["0", "1", "2", "3", "4"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.readlink",
        side_effect=[
            "/dev/stdin",
            "/dev/stdout",
            "/dev/stderr",
            "pipe:[12345]",
            "/tmp/test.txt",
        ],
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={},
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_pipe_direction",
        return_value="read",
    )

    result = _get_current_open_fds_names()

    assert "[fd=0] stdin (/dev/stdin)" in result
    assert "[fd=1] stdout (/dev/stdout)" in result
    assert "[fd=2] stderr (/dev/stderr)" in result
    assert "[fd=3] pipe (read) pipe:[12345]" in result
    assert "[fd=4] /tmp/test.txt" in result


def test_get_current_open_fds_names_uses_socket_mapping(mocker):
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["5"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.readlink",
        return_value="socket:[123]",
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={
            5: "socket: 127.0.0.1:1234 -> 127.0.0.1:5678 [ESTABLISHED]",
        },
    )

    assert _get_current_open_fds_names() == [
        "[fd=5] socket: 127.0.0.1:1234 -> 127.0.0.1:5678 [ESTABLISHED]"
    ]


def test_get_current_open_fds_names_ignores_invalid_entries(
    mocker,
):
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["not-a-fd"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={},
    )

    assert _get_current_open_fds_names() == []


def test_get_current_open_fds_names_handles_disappearing_fd(
    mocker,
):
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["3"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.readlink",
        side_effect=OSError("fd disappeared"),
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={},
    )

    assert _get_current_open_fds_names() == []


def test_capture_allocation_delta_collects_autosubmit_objects(
    profiler,
    mocker,
):
    profiler._trace_snapshots.append(MagicMock())
    profiler.checkpoints = 4

    old_obj = object()
    new_obj = object()

    snapshot = MagicMock()
    snapshot.compare_to.return_value = []

    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        side_effect=[
            [old_obj],
            [old_obj, new_obj],
        ],
    )

    frame = MagicMock(
        filename="/home/user/Autosubmit/autosubmit/workflow.py",
        lineno=123,
    )
    traceback = (frame,)

    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.get_object_traceback",
        return_value=traceback,
    )

    profiler._capture_allocation_delta(snapshot)
    profiler._capture_allocation_delta(snapshot)

    assert traceback in profiler._obj_diffs_between_iter


@pytest.mark.parametrize(
    "filename",
    [
        "/home/user/Autosubmit/autosubmit/profiler/profiler.py",
        "/tmp/external.py",
    ],
    ids=["profiler_module", "external_module"],
)
def test_capture_allocation_delta_ignores_unwanted_objects(
    profiler,
    mocker,
    filename,
):
    profiler._trace_snapshots.append(MagicMock())
    profiler.checkpoints = 4

    old_obj = object()
    new_obj = object()

    snapshot = MagicMock()
    snapshot.compare_to.return_value = []

    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        side_effect=[
            [old_obj],
            [old_obj, new_obj],
        ],
    )

    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.get_object_traceback",
        return_value=SimpleNamespace(
            filename=filename,
            lineno=123,
        ),
    )

    profiler._capture_allocation_delta(snapshot)

    assert not profiler._obj_diffs_between_iter


def test_capture_allocation_delta_ignores_missing_traceback(
    profiler,
    mocker,
):
    profiler._trace_snapshots.append(MagicMock())
    profiler.checkpoints = 4

    old_obj = object()
    new_obj = object()

    snapshot = MagicMock()
    snapshot.compare_to.return_value = []

    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        side_effect=[
            [old_obj],
            [old_obj, new_obj],
        ],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.get_object_traceback",
        return_value=None,
    )

    profiler._capture_allocation_delta(snapshot)

    assert not profiler._obj_diffs_between_iter


def test_start_does_not_stop_existing_tracemalloc(
    profiler,
    mocker,
):
    """An existing tracemalloc session must not be stopped by the profiler."""
    mocker.patch.object(
        profiler._profiler,
        "enable",
    )
    mocker.patch.object(
        profiler._profiler,
        "disable",
    )
    mocker.patch.object(
        profiler,
        "_report",
    )
    mocker.patch(
        "gc.collect",
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=100,
    )
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=True,
    )

    stop = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.stop",
    )

    profiler._trace_enabled = True

    profiler.start()

    assert profiler._trace_started is False

    profiler.stop()

    stop.assert_not_called()


def test_stop_stops_only_tracemalloc_started_by_profiler(
    profiler,
    mocker,
):
    """The profiler stops tracemalloc only when it started the session."""
    mocker.patch.object(
        profiler._profiler,
        "enable",
    )
    mocker.patch.object(
        profiler._profiler,
        "disable",
    )
    mocker.patch.object(
        profiler,
        "_report",
    )
    mocker.patch(
        "gc.collect",
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=100,
    )

    is_tracing = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        side_effect=[
            False,
            True,
        ],
    )
    start = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.start",
    )
    stop = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.stop",
    )

    profiler._trace_enabled = True

    profiler.start()

    assert profiler._trace_started is True
    start.assert_called_once()

    profiler.stop()

    is_tracing.assert_any_call()
    stop.assert_called_once()
    assert profiler._trace_started is False


def test_report_includes_all_sections(
    profiled_profiler,
    report_setup,
    mocker,
):
    profiler = profiled_profiler

    profiler._mem_init = 1024
    profiler._mem_final = 4096

    profiler._mem_iteration = [1024, 2048, 4096]
    profiler._obj_iteration = [10, 20, 30]
    profiler._fd_iteration = [2, 3, 4]

    profiler._mem_grow = [1024, 2048]
    profiler._obj_grow = [10, 10]
    profiler._fd_grow = [1, 1]

    profiler._obj_total_grow = 20
    profiler._fd_total_grow = 2

    profiler._fd_names_iteration = [
        ["[fd=1] stdout"],
        ["[fd=1] stdout", "[fd=5] new.txt"],
        ["[fd=1] stdout"],
    ]

    profiler._jobs_iteration = [1, 2, 3]
    profiler._edges_iteration = [4, 5, 6]

    profiler._trace_enabled = True

    traceback = ("autosubmit/workflow.py:123",)
    profiler._obj_diffs_between_iter.add(traceback)

    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=["[fd=7] socket: example"],
    )

    profiler._report()

    report = (report_setup / "profile.txt").read_text(encoding="UTF-8")

    # cProfile section
    assert "Time & Calls Profiling" in report
    assert "Fake cProfile statistics" in report

    # Per-iteration section
    assert "Memory, object and file descriptor by iteration" in report
    assert "Iteration 1: Memory: 1.00 KiB" in report
    assert "Iteration 1: Objects: 10" in report
    assert "Iteration 1: File Descriptors: 2" in report
    assert "Iteration 1: Loaded jobs: 1" in report
    assert "Iteration 1: Loaded edges: 4" in report

    # The final checkpoint is intentionally not rendered as an iteration.
    assert "Iteration 2:" not in report

    # Overall growth section
    assert "Overall Memory, Object and File Descriptor Growth" in report
    assert "MEMORY GROW: 3.00 KiB." in report
    assert "INITIAL MEMORY: 1.00 KiB." in report
    assert "FINAL MEMORY: 4.00 KiB." in report
    assert "OBJECTS GROW: 20 objects." in report
    assert "FILE DESCRIPTORS GROW: 2 file descriptors." in report

    # Final FD section
    assert "FINAL OPEN FILE DESCRIPTORS:" in report
    assert "[fd=7] socket: example" in report

    # tracemalloc/object traceback section
    assert "Unique object tracebacks between iterations:" in report
    assert str(traceback) in report
