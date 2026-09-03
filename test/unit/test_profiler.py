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

import socket
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from autosubmit.log.log import AutosubmitCritical

# noinspection PyProtectedMember
from autosubmit.profiler.profiler import (
    Profiler,
    ProfilerState,
    _calculate_fd_growth,
    _calculate_fd_total_growth,
    _calculate_growth,
    _format_bytes,
    _format_connection,
    _format_fd_changes,
    _format_fd_label,
    _format_signed_bytes,
    _format_signed_int,
    _format_socket_address,
    _format_top_allocations,
    _generate_title,
    _get_current_memory,
    _get_current_object_count,
    _get_current_open_fds,
    _get_current_open_fds_names,
    _get_fd_connection_map,
    _get_pipe_direction,
    _is_autosubmit_traceback,
    _now,
)


@pytest.fixture
def profiler():
    return Profiler(subcommand="run", expid="a001")


@pytest.fixture
def started_profiler(profiler, mocker):
    # noinspection PyProtectedMember
    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1000,
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")

    profiler.start()
    return profiler


def test_profiler_initial_state(profiler):
    assert profiler.stopped
    assert not profiler.started
    assert profiler._state == ProfilerState.STOPPED
    assert profiler._subcommand == "run"
    assert profiler._expid == "a001"
    assert profiler.max_checkpoints == 0
    assert profiler.checkpoint_count == 0


def test_profiler_constructor_without_expid():
    profiler = Profiler(subcommand="run", expid=None)

    assert profiler._expid is None
    assert profiler.report_path == Path(profiler.report_path)


def test_profiler_constructor_with_options():
    profiler = Profiler(
        subcommand="monitor",
        expid="a002",
        trace_enabled=True,
        max_checkpoints=4,
    )

    assert profiler._subcommand == "monitor"
    assert profiler._expid == "a002"
    assert profiler._trace_enabled is True
    assert profiler.max_checkpoints == 4


def test_profiler_start(started_profiler):
    assert started_profiler.started
    assert not started_profiler.stopped
    assert started_profiler._mem_initial == 1000


def test_profiler_start_twice_raises(started_profiler):
    with pytest.raises(AutosubmitCritical) as exc_info:
        started_profiler.start()

    assert exc_info.value.code == 7074
    assert "already started" in exc_info.value.message


def test_profiler_stop_before_start_raises(profiler):
    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.stop()

    assert exc_info.value.code == 7074
    assert "was not running" in exc_info.value.message


def test_profiler_stop(started_profiler, mocker):
    disable = mocker.patch.object(started_profiler._profiler, "disable")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=2000,
    )
    report = mocker.patch.object(started_profiler, "_report")

    started_profiler.stop()

    assert started_profiler.stopped
    assert started_profiler._mem_final == 2000
    disable.assert_called_once()
    report.assert_called_once()


def test_profiler_stop_stops_tracemalloc_on_report_error(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )

    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch.object(profiler._profiler, "disable")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        side_effect=[1000, 2000],
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=True,
    )
    stop_trace = mocker.patch("autosubmit.profiler.profiler.tracemalloc.stop")
    mocker.patch.object(
        profiler,
        "_report",
        side_effect=RuntimeError("report failed"),
    )

    profiler.start()

    with pytest.raises(RuntimeError, match="report failed"):
        profiler.stop()

    assert profiler.stopped
    stop_trace.assert_called_once()


def test_iteration_checkpoint_requires_started_profiler(profiler):
    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler.iteration_checkpoint(10, 20)

    assert exc_info.value.code == 7074
    assert "not running" in exc_info.value.message


def test_iteration_checkpoint_records_metrics(started_profiler, mocker):
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1200,
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

    assert started_profiler.iteration_checkpoint(25, 40) is False
    assert started_profiler._mem_iteration == [1200]
    assert started_profiler._obj_iteration == [200]
    assert started_profiler._fd_iteration == [10]
    assert started_profiler._fd_names_iteration == [["[fd=1] stdout"]]
    assert started_profiler._jobs_iteration == [25]
    assert started_profiler._edges_iteration == [40]
    assert started_profiler.checkpoint_count == 1


def test_iteration_checkpoint_respects_max_checkpoints(started_profiler):
    started_profiler.max_checkpoints = 2

    assert started_profiler.iteration_checkpoint(10, 20) is False
    assert started_profiler.iteration_checkpoint(11, 21) is True
    assert started_profiler.checkpoint_count == 2
    assert started_profiler.iteration_checkpoint(12, 22) is True


def test_iteration_checkpoint_captures_trace(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )

    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        side_effect=[1000, 1100],
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_object_count",
        return_value=10,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds",
        return_value=5,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=[],
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")
    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=True,
    )

    snapshot = mocker.MagicMock()
    take_snapshot = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.take_snapshot",
        return_value=snapshot,
    )
    capture = mocker.patch.object(
        profiler,
        "_capture_allocation_delta",
        return_value=[],
    )

    profiler.start()
    profiler.iteration_checkpoint(10, 20)

    take_snapshot.assert_called_once()
    capture.assert_called_once_with(snapshot)
    assert profiler._trace_stats_by_iter == [[]]


def test_calculate_growth():
    assert _calculate_growth([100, 150, 180]) == [50, 30]


def test_profiler_calculate_growth():
    profiler = Profiler(subcommand="run", expid="a001")
    profiler._mem_iteration = [100, 150, 180, 200]
    profiler._obj_iteration = [10, 15, 21, 30]
    profiler._fd_iteration = [4, 5, 7, 8]

    profiler._calculate_growth()

    assert profiler._mem_growth == [50, 30, 20]
    assert profiler._obj_growth == [5, 6, 9]
    assert profiler._fd_growth == [1, 2, 1]
    assert profiler._obj_total_growth == 9
    assert profiler._fd_total_growth == 1


def test_profiler_calculate_growth_with_short_run():
    profiler = Profiler(subcommand="run", expid="a001")
    profiler._mem_iteration = [100, 150]
    profiler._obj_iteration = [10, 15]
    profiler._fd_iteration = [4, 7]

    profiler._calculate_growth()

    assert profiler._obj_total_growth == 5
    assert profiler._fd_total_growth == 3


def test_profiler_calculate_growth_with_unavailable_fds():
    profiler = Profiler(subcommand="run", expid="a001")
    profiler._mem_iteration = [100, 150]
    profiler._obj_iteration = [10, 15]
    profiler._fd_iteration = [None, 7]

    profiler._calculate_growth()

    assert profiler._fd_growth == []
    assert profiler._fd_total_growth == 0


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([4, 5, 7], [1, 2]),
        ([4, None, 7], []),
        ([None, 5, 7], [2]),
        ([4, 5, None], [1]),
        ([None, None], []),
    ],
)
def test_calculate_fd_growth(values, expected):
    assert _calculate_fd_growth(values) == expected


@pytest.mark.parametrize(
    ("values", "baseline", "expected"),
    [
        ([4, 5, 7], 0, 3),
        ([4, 5, 7], 1, 2),
        ([4, None, 7], 1, 0),
        ([None, 5, 7], 0, 0),
    ],
)
def test_calculate_fd_total_growth(values, baseline, expected):
    assert _calculate_fd_total_growth(values, baseline) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0.00 B"),
        (1023, "1023.00 B"),
        (1024, "1.00 KiB"),
        (1024**2, "1.00 MiB"),
        (-1024, "-1.00 KiB"),
        (1024**3, "1.00 GiB"),
    ],
)
def test_format_bytes(value, expected):
    assert _format_bytes(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1024, "+1.00 KiB"),
        (0, "+0.00 B"),
        (-1024, "-1.00 KiB"),
    ],
)
def test_format_signed_bytes(value, expected):
    assert _format_signed_bytes(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (5, "+5"),
        (0, "+0"),
        (-5, "-5"),
    ],
)
def test_format_signed_int(value, expected):
    assert _format_signed_int(value) == expected


def test_format_top_allocations_empty():
    assert _format_top_allocations([]) == []


def test_format_top_allocations():
    frame = SimpleNamespace(filename="/tmp/example.py", lineno=42)
    stat = SimpleNamespace(
        traceback=[frame],
        size_diff=2048,
        count_diff=3,
    )

    assert _format_top_allocations([stat]) == [  # type: ignore
        "  Top allocation deltas:",
        "    /tmp/example.py:42 +2.00 KiB (+3 blocks)",
    ]


def test_generate_title():
    assert _generate_title("Test Title").splitlines() == [
        "=" * 80,
        "Test Title".center(80),
        "=" * 80,
    ]


def test_now_returns_utc_timestamp(mocker):
    fixed_datetime = datetime(
        2026,
        9,
        3,
        12,
        30,
        45,
        tzinfo=timezone.utc,
    )
    mocked_datetime = mocker.MagicMock()
    mocked_datetime.now.return_value = fixed_datetime
    mocker.patch("autosubmit.profiler.profiler.datetime", mocked_datetime)

    assert _now() == "20260903-123045"


def test_capture_allocation_delta_without_previous_snapshot(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    snapshot = mocker.MagicMock()

    assert profiler._capture_allocation_delta(snapshot) == []
    assert profiler._previous_trace_snapshot is snapshot


def test_capture_allocation_delta_filters_positive_and_limits_to_five(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    previous = mocker.MagicMock()
    current = mocker.MagicMock()

    stats = []
    for index in range(7):
        stat = mocker.MagicMock()
        stat.size_diff = index + 1
        stats.append(stat)

    negative = mocker.MagicMock()
    negative.size_diff = -100
    current.compare_to.return_value = stats + [negative]

    profiler._previous_trace_snapshot = previous
    mocker.patch.object(profiler, "_capture_object_growth")

    assert profiler._capture_allocation_delta(current) == stats[:5]
    current.compare_to.assert_called_once_with(previous, "lineno")
    assert profiler._previous_trace_snapshot is current


def test_capture_allocation_delta_records_object_growth(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    profiler._previous_trace_snapshot = mocker.MagicMock()

    current = mocker.MagicMock()
    current.compare_to.return_value = []

    capture = mocker.patch.object(profiler, "_capture_object_growth")

    profiler._capture_allocation_delta(current)

    capture.assert_called_once()


def test_capture_object_growth_ignores_first_three_checkpoints(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=[object()],
    )

    for checkpoint in range(1, 4):
        profiler._mem_iteration = [0] * checkpoint
        profiler._capture_object_growth()

    assert profiler._previous_object_ids is None
    assert profiler._obj_diffs_between_iter == set()


def test_capture_object_growth_initialises_object_ids_after_baseline(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    profiler._mem_iteration = [0, 0, 0, 0]

    objects = [object(), object()]
    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=objects,
    )

    profiler._capture_object_growth()

    assert profiler._previous_object_ids == {id(obj) for obj in objects}
    assert profiler._obj_diffs_between_iter == set()


def test_capture_object_growth_records_autosubmit_tracebacks(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    profiler._mem_iteration = [0, 0, 0, 0]

    old_object = object()
    new_object = object()

    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=[old_object],
    )
    profiler._capture_object_growth()

    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=[old_object, new_object],
    )
    traceback = mocker.MagicMock()

    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.get_object_traceback",
        return_value=traceback,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._is_autosubmit_traceback",
        return_value=True,
    )

    profiler._capture_object_growth()

    assert traceback in profiler._obj_diffs_between_iter


def test_record_object_tracebacks_ignores_non_autosubmit(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    traceback = mocker.MagicMock()

    mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.get_object_traceback",
        return_value=traceback,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._is_autosubmit_traceback",
        return_value=False,
    )

    profiler._record_object_tracebacks([object()])

    assert profiler._obj_diffs_between_iter == set()


def test_is_autosubmit_traceback():
    autosubmit = SimpleNamespace(
        filename="/src/Autosubmit/autosubmit/foo.py",
    )
    profiler_file = SimpleNamespace(
        filename="/src/Autosubmit/autosubmit/profiler/profiler.py",
    )
    external = SimpleNamespace(
        filename="/usr/lib/python/example.py",
    )

    assert _is_autosubmit_traceback([autosubmit])  # type: ignore
    assert not _is_autosubmit_traceback([profiler_file])  # type: ignore
    assert not _is_autosubmit_traceback([external])  # type: ignore
    assert _is_autosubmit_traceback([external, autosubmit])  # type: ignore


def test_format_fd_changes():
    assert _format_fd_changes(
        ["fd1", "fd2"],
        ["fd1", "fd3"],
    ) == [
        "Opened: fd3",
        "Closed: fd2",
    ]


def test_format_fd_changes_sorts_output():
    assert _format_fd_changes(
        ["z", "b"],
        ["a", "y"],
    ) == [
        "Opened: a",
        "Opened: y",
        "Closed: b",
        "Closed: z",
    ]


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        (None, ""),
        ("localhost", "localhost"),
        (SimpleNamespace(ip="127.0.0.1", port=8080), "127.0.0.1:8080"),
        (SimpleNamespace(path="/tmp/socket"), "/tmp/socket"),
        (SimpleNamespace(foo="bar"), ""),
    ],
)
def test_format_socket_address(address, expected):
    assert _format_socket_address(address) == expected


def test_format_connection_unix_socket():
    connection = SimpleNamespace(
        family=socket.AF_UNIX,
        laddr="/tmp/autosubmit.sock",
        raddr=None,
        status="",
    )

    assert _format_connection(connection) == "unix-socket: /tmp/autosubmit.sock"


def test_format_connection_network_socket():
    connection = SimpleNamespace(
        family=socket.AF_INET,
        laddr=SimpleNamespace(ip="127.0.0.1", port=1234),
        raddr=SimpleNamespace(ip="127.0.0.1", port=5678),
        status="ESTABLISHED",
    )

    assert _format_connection(connection) == (
        "socket: 127.0.0.1:1234 -> 127.0.0.1:5678 [ESTABLISHED]"
    )


def test_format_connection_without_addresses():
    connection = SimpleNamespace(
        family=socket.AF_INET,
        laddr=None,
        raddr=None,
        status="",
    )

    assert _format_connection(connection) == ""


def test_get_fd_connection_map(mocker):
    process = mocker.MagicMock()
    connection = SimpleNamespace(
        fd=4,
        family=socket.AF_INET,
        laddr=SimpleNamespace(ip="127.0.0.1", port=1234),
        raddr=None,
        status="LISTEN",
    )
    process.net_connections.return_value = [connection]

    assert _get_fd_connection_map(process) == {
        4: "socket: 127.0.0.1:1234 [LISTEN]",
    }


def test_get_fd_connection_map_ignores_negative_fd(mocker):
    process = mocker.MagicMock()
    process.net_connections.return_value = [
        SimpleNamespace(
            fd=-1,
            family=socket.AF_INET,
            laddr=None,
            raddr=None,
            status="",
        )
    ]

    assert _get_fd_connection_map(process) == {}


def test_get_fd_connection_map_handles_os_error(mocker):
    process = mocker.MagicMock()
    process.net_connections.side_effect = OSError

    assert _get_fd_connection_map(process) == {}


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("read", "read"),
        ("write", "write"),
        ("unknown", "unknown"),
    ],
)
def test_get_pipe_direction(mocker, target, expected):
    flags = {
        "read": "0100000",
        "write": "01",
        "unknown": "03",
    }[target]

    mocker.patch(
        "builtins.open",
        mocker.mock_open(read_data=f"flags:\t{flags}\n"),
    )

    assert _get_pipe_direction(123, 5) == expected


def test_get_pipe_direction_handles_missing_fdinfo(mocker):
    mocker.patch("builtins.open", side_effect=OSError)

    assert _get_pipe_direction(123, 5) == "unknown"


def test_get_pipe_direction_handles_missing_flags(mocker):
    mocker.patch(
        "builtins.open",
        mocker.mock_open(read_data="pos:\t123\nmnt_id:\t456\n"),
    )

    assert _get_pipe_direction(123, 5) == "unknown"


def test_format_fd_label_standard_stream():
    assert (
        _format_fd_label(
            1,
            1,
            "/dev/pts/0",
            {},
            {0: "stdin", 1: "stdout", 2: "stderr"},
        )
        == "stdout (/dev/pts/0)"
    )


def test_format_fd_label_socket():
    assert (
        _format_fd_label(
            1,
            4,
            "socket:[123]",
            {4: "socket: 127.0.0.1:1 [LISTEN]"},
            {},
        )
        == "socket: 127.0.0.1:1 [LISTEN]"
    )


def test_format_fd_label_pipe(mocker):
    mocker.patch(
        "autosubmit.profiler.profiler._get_pipe_direction",
        return_value="write",
    )

    assert (
        _format_fd_label(
            123,
            9,
            "pipe:[1770463]",
            {},
            {},
        )
        == "pipe (write) pipe:[1770463]"
    )


def test_format_fd_label_regular_target():
    assert (
        _format_fd_label(
            123,
            7,
            "/tmp/example.log",
            {},
            {},
        )
        == "/tmp/example.log"
    )


def test_get_current_open_fds_names_non_linux(mocker):
    process = mocker.MagicMock()
    mocker.patch(
        "autosubmit.profiler.profiler.Path.is_dir",
        return_value=False,
    )

    assert _get_current_open_fds_names(process) == []


def test_get_current_open_fds_names(mocker):
    process = mocker.MagicMock()
    process.pid = 123

    mocker.patch(
        "autosubmit.profiler.profiler.Path.is_dir",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={4: "socket: 127.0.0.1:1234 [LISTEN]"},
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["9", "1", "4", "0"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.readlink",
        side_effect=[
            "/dev/pts/0",
            "/dev/pts/0",
            "socket:[123]",
            "pipe:[456]",
        ],
    )

    assert _get_current_open_fds_names(process) == [
        "[fd=0] stdin (/dev/pts/0)",
        "[fd=1] stdout (/dev/pts/0)",
        "[fd=4] socket: 127.0.0.1:1234 [LISTEN]",
        "[fd=9] pipe (unknown) pipe:[456]",
    ]


def test_get_current_open_fds_names_handles_listdir_error(mocker):
    process = mocker.MagicMock()
    process.pid = 123

    mocker.patch(
        "autosubmit.profiler.profiler.Path.is_dir",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        side_effect=OSError,
    )

    assert _get_current_open_fds_names(process) == []


def test_get_current_open_fds_names_skips_unreadable_fd(mocker):
    process = mocker.MagicMock()
    process.pid = 123

    mocker.patch(
        "autosubmit.profiler.profiler.Path.is_dir",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={},
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["3"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.readlink",
        side_effect=OSError,
    )

    assert _get_current_open_fds_names(process) == []


def test_get_current_open_fds_names_skips_non_numeric_entries(mocker):
    process = mocker.MagicMock()
    process.pid = 123

    mocker.patch(
        "autosubmit.profiler.profiler.Path.is_dir",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_fd_connection_map",
        return_value={},
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.listdir",
        return_value=["3", "not-a-fd"],
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.readlink",
        return_value="/tmp/example",
    )

    assert _get_current_open_fds_names(process) == [
        "[fd=3] /tmp/example",
    ]


def test_get_current_memory():
    process = SimpleNamespace(
        memory_info=lambda: SimpleNamespace(rss=123456),  # type: ignore
    )

    assert _get_current_memory(process) == 123456  # type: ignore


def test_get_current_object_count(mocker):
    mocker.patch(
        "autosubmit.profiler.profiler.gc.get_objects",
        return_value=[object(), object(), object()],
    )

    assert _get_current_object_count() == 3


def test_get_current_open_fds_num_fds():
    process = SimpleNamespace(num_fds=lambda: 7)

    assert _get_current_open_fds(process) == 7  # type: ignore


def test_get_current_open_fds_num_handles():
    class ProcessWithoutFds:
        # noinspection PyMethodMayBeStatic
        def num_handles(self):
            return 9

    assert _get_current_open_fds(ProcessWithoutFds()) == 9  # type: ignore


def test_get_current_open_fds_unavailable():
    class ProcessWithoutFdMetrics:
        pass

    assert _get_current_open_fds(ProcessWithoutFdMetrics()) is None  # type: ignore


def test_file_name_with_expid(profiler, mocker):
    mocker.patch(
        "autosubmit.profiler.profiler._now",
        return_value="20260903-120000",
    )

    assert profiler.file_name == "a001_run_profile_20260903-120000.prof"


def test_file_name_without_expid(mocker):
    profiler = Profiler(subcommand="run", expid=None)
    mocker.patch(
        "autosubmit.profiler.profiler._now",
        return_value="20260903-120000",
    )

    assert profiler.file_name == "run_profile_20260903-120000.prof"


def test_report_path_with_expid(profiler, mocker, tmp_path):
    mocker.patch(
        "autosubmit.profiler.profiler.BasicConfig.LOCAL_ROOT_DIR",
        str(tmp_path),
    )

    assert profiler.report_path == tmp_path / "a001" / "tmp" / "profile"


def test_report_path_without_expid(mocker, tmp_path):
    profiler = Profiler(subcommand="run", expid=None)
    mocker.patch(
        "autosubmit.profiler.profiler.BasicConfig.GLOBAL_LOG_DIR",
        str(tmp_path),
    )

    assert profiler.report_path == tmp_path / "profile"


def test_report_growth_without_checkpoints(profiler):
    assert profiler._report_growth() == []


def test_report_growth_contains_iteration_metrics(profiler):
    profiler._mem_iteration = [1024, 2048]
    profiler._obj_iteration = [10, 20]
    profiler._fd_iteration = [4, 5]
    profiler._fd_names_iteration = [["fd1"], ["fd1", "fd2"]]
    profiler._jobs_iteration = [10, 20]
    profiler._edges_iteration = [15, 25]
    profiler._mem_growth = [1024]
    profiler._obj_growth = [10]
    profiler._fd_growth = [1]
    profiler._trace_stats_by_iter = [[], []]

    result = profiler._report_growth()

    assert result[0] == "Iteration metrics:"
    assert "Iteration | Memory" in result[2]
    assert "         1 |" in result[4]
    assert "         2 |" in result[5]
    assert "Iteration 2 details:" in result
    assert any("Opened: fd2" in line for line in result)


def test_report_growth_includes_allocation_details(profiler):
    profiler._mem_iteration = [100, 200]
    profiler._obj_iteration = [10, 20]
    profiler._fd_iteration = [4, 4]
    profiler._fd_names_iteration = [[], []]
    profiler._jobs_iteration = [1, 2]
    profiler._edges_iteration = [3, 4]

    stat = SimpleNamespace(
        traceback=[SimpleNamespace(filename="foo.py", lineno=10)],
        size_diff=1024,
        count_diff=2,
    )
    profiler._trace_stats_by_iter = [[], [stat]]  # type: ignore
    profiler._mem_growth = [100]
    profiler._obj_growth = [10]
    profiler._fd_growth = [0]

    result = profiler._report_growth()

    assert any("Top allocation deltas:" in line for line in result)
    assert any("foo.py:10 +1.00 KiB (+2 blocks)" in line for line in result)


def test_report_growth_handles_unavailable_file_descriptors(profiler):
    profiler._mem_iteration = [1024, 2048]
    profiler._obj_iteration = [10, 20]
    profiler._fd_iteration = [4, None]
    profiler._fd_names_iteration = [["fd1"], []]
    profiler._jobs_iteration = [10, 20]
    profiler._edges_iteration = [15, 25]
    profiler._mem_growth = [1024]
    profiler._obj_growth = [10]
    profiler._fd_growth = []
    profiler._trace_stats_by_iter = [[], []]

    result = profiler._report_growth()

    assert any("         2 |" in line for line in result)
    assert any("|   - |" in line for line in result)


def test_report_growth_handles_missing_previous_fd_measurement(profiler):
    profiler._mem_iteration = [1024, 2048]
    profiler._obj_iteration = [10, 20]
    profiler._fd_iteration = [None, 5]
    profiler._fd_names_iteration = [[], []]
    profiler._jobs_iteration = [10, 20]
    profiler._edges_iteration = [15, 25]
    profiler._mem_growth = [1024]
    profiler._obj_growth = [10]
    profiler._fd_growth = []
    profiler._trace_stats_by_iter = [[], []]

    result = profiler._report_growth()

    assert any("         2 |" in line for line in result)


def test_report_growth_skips_iteration_without_details(profiler):
    profiler._mem_iteration = [1024, 2048]
    profiler._obj_iteration = [10, 20]
    profiler._fd_iteration = [4, 4]
    profiler._fd_names_iteration = [["fd1"], ["fd1"]]
    profiler._jobs_iteration = [10, 20]
    profiler._edges_iteration = [15, 25]
    profiler._mem_growth = [1024]
    profiler._obj_growth = [10]
    profiler._fd_growth = [0]
    profiler._trace_stats_by_iter = [[], []]

    result = profiler._report_growth()

    assert "Iteration 2 details:" not in result


def _mock_report(mocker, tmp_path):
    mocker.patch(
        "autosubmit.profiler.profiler.BasicConfig.LOCAL_ROOT_DIR",
        str(tmp_path),
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.access",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=[],
    )
    mocker.patch("autosubmit.profiler.profiler.Log.info")

    stats = mocker.patch("autosubmit.profiler.profiler.pstats.Stats")
    stats_instance = stats.return_value
    stats_instance.strip_dirs.return_value = stats_instance
    stats_instance.sort_stats.return_value = stats_instance
    return stats_instance


def test_report_writes_prof_and_txt_files(mocker, tmp_path):
    profiler = Profiler(subcommand="run", expid="a001")
    stats_instance = _mock_report(mocker, tmp_path)
    stats_instance.dump_stats.side_effect = lambda path: Path(path).touch()

    profiler._mem_initial = 1000
    profiler._mem_final = 2000

    profiler._report()

    report_dir = tmp_path / "a001" / "tmp" / "profile"

    assert report_dir.is_dir()
    assert len(list(report_dir.glob("*.prof"))) == 1
    assert len(list(report_dir.glob("*.txt"))) == 1

    txt_file = next(report_dir.glob("*.txt"))
    assert "Overall Memory, Object and File Descriptor Growth" in (
        txt_file.read_text(encoding="UTF-8")
    )
    stats_instance.dump_stats.assert_called_once_with(next(report_dir.glob("*.prof")))


def test_report_includes_object_and_fd_growth(mocker, tmp_path):
    profiler = Profiler(subcommand="run", expid="a001")
    _mock_report(mocker, tmp_path)

    profiler._mem_initial = 1000
    profiler._mem_final = 2024
    profiler._obj_growth = [1]
    profiler._obj_total_growth = 12
    profiler._fd_growth = [1]
    profiler._fd_total_growth = -4

    profiler._report()

    txt_file = next((tmp_path / "a001" / "tmp" / "profile").glob("*.txt"))
    report = txt_file.read_text(encoding="UTF-8")

    assert "Growth  : +1.00 KiB" in report
    assert "Objects:" in report
    assert "Growth  : +12" in report
    assert "File descriptors:" in report
    assert "Growth  : -4" in report


def test_report_includes_object_tracebacks(mocker, tmp_path):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )
    _mock_report(mocker, tmp_path)

    profiler._mem_initial = 1000
    profiler._mem_final = 2000
    profiler._obj_diffs_between_iter = {  # type: ignore
        "Autosubmit/foo.py:10",
        "Autosubmit/bar.py:20",
    }

    profiler._report()

    txt_file = next((tmp_path / "a001" / "tmp" / "profile").glob("*.txt"))
    report = txt_file.read_text(encoding="UTF-8")

    assert "Unique Object Tracebacks Between Iterations" in report
    assert "Autosubmit/bar.py:20" in report
    assert "Autosubmit/foo.py:10" in report


def test_report_raises_when_directory_not_writable(mocker, tmp_path):
    profiler = Profiler(subcommand="run", expid="a001")

    mocker.patch(
        "autosubmit.profiler.profiler.BasicConfig.LOCAL_ROOT_DIR",
        str(tmp_path),
    )
    mocker.patch(
        "autosubmit.profiler.profiler.os.access",
        return_value=False,
    )

    with pytest.raises(AutosubmitCritical) as exc_info:
        profiler._report()

    assert exc_info.value.code == 7012
    assert "not writable" in exc_info.value.message


def test_profiler_start_starts_tracemalloc_when_trace_enabled(mocker):
    profiler = Profiler(
        subcommand="run",
        expid="a001",
        trace_enabled=True,
    )

    mocker.patch.object(profiler._profiler, "enable")
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=1000,
    )
    mocker.patch("autosubmit.profiler.profiler.gc.collect")

    is_tracing = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.is_tracing",
        return_value=False,
    )
    start_trace = mocker.patch(
        "autosubmit.profiler.profiler.tracemalloc.start",
    )

    profiler.start()

    is_tracing.assert_called_once()
    start_trace.assert_called_once()


def test_profiler_stop_calculates_growth_when_checkpoints_exist(
    started_profiler,
    mocker,
):
    mocker.patch(
        "autosubmit.profiler.profiler._get_current_memory",
        return_value=2000,
    )
    mocker.patch.object(started_profiler._profiler, "disable")
    mocker.patch.object(started_profiler, "_report")

    started_profiler._mem_iteration = [1000, 1500]
    started_profiler._obj_iteration = [10, 15]
    started_profiler._fd_iteration = [4, 5]

    calculate_growth = mocker.patch.object(
        started_profiler,
        "_calculate_growth",
    )

    started_profiler.stop()

    calculate_growth.assert_called_once()


def test_report_includes_iteration_section_when_checkpoints_exist(
    mocker,
    tmp_path,
):
    profiler = Profiler(subcommand="run", expid="a001")
    _mock_report(mocker, tmp_path)

    profiler._mem_initial = 1000
    profiler._mem_final = 2000
    profiler._mem_iteration = [1000, 2000]
    profiler._obj_iteration = [10, 20]
    profiler._fd_iteration = [4, 5]
    profiler._fd_names_iteration = [["fd1"], ["fd1", "fd2"]]
    profiler._jobs_iteration = [10, 20]
    profiler._edges_iteration = [15, 25]
    profiler._mem_growth = [1000]
    profiler._obj_growth = [10]
    profiler._fd_growth = [1]
    profiler._trace_stats_by_iter = [[], []]

    profiler._report()

    txt_file = next((tmp_path / "a001" / "tmp" / "profile").glob("*.txt"))
    report = txt_file.read_text(encoding="UTF-8")

    assert "Memory, Object and File Descriptor Usage by Iteration" in report
    assert "Iteration metrics:" in report


def test_report_includes_final_open_file_descriptors(mocker, tmp_path):
    """Test that final open file descriptors are included in the report."""
    profiler = Profiler(subcommand="run", expid="a001")
    _mock_report(mocker, tmp_path)

    mocker.patch(
        "autosubmit.profiler.profiler._get_current_open_fds_names",
        return_value=[
            "[fd=0] stdin (/dev/pts/0)",
            "[fd=4] socket: 127.0.0.1:1234 [LISTEN]",
        ],
    )

    profiler._mem_initial = 1000
    profiler._mem_final = 2024

    profiler._report()

    txt_file = next((tmp_path / "a001" / "tmp" / "profile").glob("*.txt"))
    report = txt_file.read_text(encoding="UTF-8")

    assert "Final Open File Descriptors" in report
    assert "[fd=0] stdin (/dev/pts/0)" in report
    assert "[fd=4] socket: 127.0.0.1:1234 [LISTEN]" in report
