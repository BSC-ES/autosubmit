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

"""Profiling support for Autosubmit command-line commands."""

import cProfile
import gc
import io
import itertools
import os
import pstats
import socket
import tracemalloc
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from pstats import SortKey
from typing import Any

from psutil import Process

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log

_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
"""File size units."""


class ProfilerState(str, Enum):
    """Enumeration of profiler states."""

    STOPPED = "stopped"
    """Profiler stopped."""

    STARTED = "started"
    """Profiler started."""


def _now() -> str:
    """Return a UTC timestamp.

    The value returned is in the format year, month, day, hour, minute,
    and second, with a dash between the date and time.
    """
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")


def _format_bytes(value: float) -> str:
    """Format a byte value using binary units (KiB, MiB, GiB).

    Values are divided by 1024 for each unit step.

    :param value: The value in bytes.
    :return: The value formatted with a binary unit.
    """
    value = float(value)
    unit = 0

    while abs(value) >= 1024 and unit < len(_UNITS) - 1:
        value /= 1024
        unit += 1

    return f"{value:.2f} {_UNITS[unit]}"


def _format_signed_bytes(value: float) -> str:
    """Format a byte delta using a suitable binary unit.

    :param value: The byte delta.
    :return: The formatted signed byte delta.
    """
    sign = "+" if value >= 0 else "-"
    return f"{sign}{_format_bytes(abs(value))}"


def _format_signed_int(value: int) -> str:
    """Format an integer delta with an explicit sign.

    :param value: Integer delta.
    :return: Signed integer delta.
    """
    return f"{value:+d}"


def _format_top_allocations(
    stats: list[tracemalloc.StatisticDiff],
) -> list[str]:
    """Format positive tracemalloc allocation deltas.

    :param stats: Allocation delta statistics.
    :return: Formatted allocation information.
    """
    if not stats:
        return []

    lines = ["  Top allocation deltas:"]

    for stat in stats:
        frame = stat.traceback[0]
        size_diff = stat.size_diff
        count_diff = stat.count_diff

        lines.append(
            f"    {frame.filename}:{frame.lineno} "
            f"{_format_signed_bytes(size_diff)} "
            f"({_format_signed_int(count_diff)} blocks)"
        )

    return lines


class Profiler:
    """Profile the execution of Autosubmit commands."""

    def __init__(
        self,
        *,
        subcommand: str,
        expid: str | None,
        trace_enabled: bool = False,
        max_checkpoints: int = 0,
    ):
        """Initialise the profiler.

        :param subcommand: Autosubmit command being profiled.
        :param expid: Experiment identifier.
        :param trace_enabled: Whether allocation tracing is enabled.
        :param max_checkpoints: Maximum number of iteration checkpoints.
            A value of zero means unlimited.
        """
        self._profiler = cProfile.Profile()
        self._process = Process(os.getpid())

        self._subcommand = subcommand
        self._expid = expid
        self.max_checkpoints = max_checkpoints

        # Memory profiling.
        self._mem_initial = 0
        self._mem_final = 0

        # Per-iteration metrics.
        #
        # The initial sample is stored separately. These lists therefore
        # contain only actual iteration checkpoints.
        self._mem_iteration: list[int] = []
        self._obj_iteration: list[int] = []
        self._fd_iteration: list[int | None] = []
        self._fd_names_iteration: list[list[str]] = []
        self._jobs_iteration: list[int] = []
        self._edges_iteration: list[int] = []

        # Growth metrics between consecutive iteration checkpoints.
        self._mem_growth: list[int] = []
        self._obj_growth: list[int] = []
        self._fd_growth: list[int] = []

        # Overall object and file-descriptor growth.
        self._obj_total_growth = 0
        self._fd_total_growth = 0

        # Allocation tracing.
        #
        # Only the immediately preceding snapshot is retained. The
        # StatisticDiff results are kept because they are needed when
        # generating the final report.
        self._trace_enabled = trace_enabled
        self._previous_trace_snapshot: tracemalloc.Snapshot | None = None
        self._trace_stats_by_iter: list[list[tracemalloc.StatisticDiff]] = []

        # Keep only IDs from the previous object snapshot. Keeping the
        # actual objects would retain them and could therefore alter the
        # behaviour being measured.
        self._previous_object_ids: set[int] | None = None
        self._obj_diffs_between_iter: set[tracemalloc.Traceback] = set()

        # Profiler state.
        self._state = ProfilerState.STOPPED

    @property
    def started(self) -> bool:
        """Return whether the profiler is started."""
        return self._state == ProfilerState.STARTED

    @property
    def stopped(self) -> bool:
        """Return whether the profiler is stopped."""
        return self._state == ProfilerState.STOPPED

    @property
    def checkpoint_count(self) -> int:
        """Return the number of recorded iteration checkpoints."""
        return len(self._mem_iteration)

    def start(self) -> None:
        """Start the profiling process.

        The initial memory, object and file-descriptor values are recorded
        here as the baseline. Actual iteration checkpoints are recorded
        through :meth:`iteration_checkpoint`.

        :raises AutosubmitCritical: If the profiler was already started.
        """
        if self.started:
            raise AutosubmitCritical(
                "The profiling process was already started.",
                7074,
            )

        self._state = ProfilerState.STARTED
        self._profiler.enable()

        gc.collect()

        self._mem_initial = _get_current_memory(self._process)

        if self._trace_enabled and not tracemalloc.is_tracing():
            tracemalloc.start()

    def iteration_checkpoint(
        self,
        loaded_jobs: int,
        loaded_edges: int,
    ) -> bool:
        """Record metrics for one completed iteration.

        :param loaded_jobs: Number of jobs loaded in the current iteration.
        :param loaded_edges: Number of edges loaded in the current iteration.
        :return: True when the configured checkpoint limit is reached.
        """
        if self.stopped:
            raise AutosubmitCritical(
                "Cannot record a checkpoint because the profiler is not running.",
                7074,
            )

        gc.collect()

        self._mem_iteration.append(_get_current_memory(self._process))
        self._obj_iteration.append(_get_current_object_count())
        self._fd_iteration.append(_get_current_open_fds(self._process))
        self._fd_names_iteration.append(_get_current_open_fds_names(self._process))

        self._jobs_iteration.append(loaded_jobs)
        self._edges_iteration.append(loaded_edges)

        if self._trace_enabled and tracemalloc.is_tracing():
            snapshot = tracemalloc.take_snapshot()

            self._trace_stats_by_iter.append(self._capture_allocation_delta(snapshot))

        return 0 < self.max_checkpoints <= self.checkpoint_count

    def stop(self) -> None:
        """Finish profiling and generate the profiling reports.

        :raises AutosubmitCritical: If the profiler was not running.
        """
        if self.stopped:
            raise AutosubmitCritical(
                "Cannot stop the profiler because it was not running.",
                7074,
            )

        try:
            self._profiler.disable()

            self._mem_final = _get_current_memory(self._process)

            if self.checkpoint_count:
                self._calculate_growth()

            self._report()
        finally:
            self._state = ProfilerState.STOPPED

            if self._trace_enabled and tracemalloc.is_tracing():
                tracemalloc.stop()

    def _calculate_growth(self) -> None:
        """Calculate growth metrics from the recorded checkpoints."""
        self._mem_growth = _calculate_growth(self._mem_iteration)
        self._obj_growth = _calculate_growth(self._obj_iteration)
        self._fd_growth = _calculate_fd_growth(self._fd_iteration)

        # Ignore the startup checkpoints when enough iterations exist.
        # For short runs, use the first checkpoint as the baseline.
        baseline_index = 2 if self.checkpoint_count > 2 else 0

        self._obj_total_growth = (
            self._obj_iteration[-1] - self._obj_iteration[baseline_index]
        )

        self._fd_total_growth = _calculate_fd_total_growth(
            self._fd_iteration,
            baseline_index,
        )

    def _report_growth(self) -> list[str]:
        """Format per-iteration metrics for the report.

        :return: Formatted iteration metrics.
        """
        if not self.checkpoint_count:
            return []

        lines = [
            "Iteration metrics:",
            "",
            "  Iteration | Memory       | Δ Memory    | Objects | Δ Objects "
            "| FDs | Δ FDs | Jobs | Edges",
            "  ----------+--------------+-------------+---------+-----------"
            "+-----+-------+------+------",
        ]

        for index in range(self.checkpoint_count):
            iteration = index + 1

            memory = _format_bytes(self._mem_iteration[index])
            objects = self._obj_iteration[index]
            file_descriptors = self._fd_iteration[index]

            if index == 0:
                memory_growth = "-"
                object_growth = "-"
                fd_growth = "-"
            else:
                memory_growth = _format_signed_bytes(self._mem_growth[index - 1])
                object_growth = _format_signed_int(self._obj_growth[index - 1])

                previous_file_descriptors = self._fd_iteration[index - 1]

                if file_descriptors is None or previous_file_descriptors is None:
                    fd_growth = "-"
                else:
                    fd_growth = _format_signed_int(self._fd_growth[index - 1])

            fd_value = str(file_descriptors) if file_descriptors is not None else "-"

            lines.append(
                f"  {iteration:>9} | "
                f"{memory:>12} | "
                f"{memory_growth:>11} | "
                f"{objects:>7} | "
                f"{object_growth:>9} | "
                f"{fd_value:>3} | "
                f"{fd_growth:>5} | "
                f"{self._jobs_iteration[index]:>4} | "
                f"{self._edges_iteration[index]:>5}"
            )

        for index in range(1, self.checkpoint_count):
            fd_changes = _format_fd_changes(
                self._fd_names_iteration[index - 1],
                self._fd_names_iteration[index],
            )

            allocation_report = []

            if index < len(self._trace_stats_by_iter):
                allocation_report = _format_top_allocations(
                    self._trace_stats_by_iter[index]
                )

            if not fd_changes and not allocation_report:
                continue

            lines.extend(
                [
                    "",
                    f"Iteration {index + 1} details:",
                ]
            )

            if fd_changes:
                lines.append("  File descriptor changes:")
                lines.extend(f"    {change}" for change in fd_changes)

            if allocation_report:
                lines.extend(allocation_report)

        return lines

    def _capture_allocation_delta(
        self,
        snapshot: tracemalloc.Snapshot,
    ) -> list[tracemalloc.StatisticDiff]:
        """Return the largest positive allocation deltas.

        :param snapshot: Current tracemalloc snapshot.
        :return: Largest positive allocation deltas.
        """
        previous = self._previous_trace_snapshot
        self._previous_trace_snapshot = snapshot

        if previous is None:
            return []

        stats = snapshot.compare_to(previous, "lineno")

        self._capture_object_growth()

        positive_stats = [stat for stat in stats if stat.size_diff > 0]

        return positive_stats[:5]

    def _capture_object_growth(self) -> None:
        """Record unique Autosubmit tracebacks for new objects."""
        checkpoint_count = self.checkpoint_count

        # The first three iterations are treated as the startup baseline.
        if checkpoint_count <= 3:
            return

        previous_ids = self._previous_object_ids
        current_objects = gc.get_objects()

        if previous_ids is None:
            self._previous_object_ids = {id(obj) for obj in current_objects}
            return

        new_objects = [obj for obj in current_objects if id(obj) not in previous_ids]

        self._record_object_tracebacks(new_objects)

        self._previous_object_ids = {id(obj) for obj in current_objects}

    def _record_object_tracebacks(
        self,
        objects: list[Any],
    ) -> None:
        """Record unique Autosubmit tracebacks for newly observed objects.

        :param objects: Objects observed since the previous checkpoint.
        """
        for obj in objects:
            traceback = tracemalloc.get_object_traceback(obj)

            if traceback is not None and _is_autosubmit_traceback(traceback):
                self._obj_diffs_between_iter.add(traceback)

    @property
    def file_name(self) -> str:
        """Return the name of the profiler report file."""
        if self._expid:
            return f"{self._expid}_{self._subcommand}_profile_{_now()}.prof"

        return f"{self._subcommand}_profile_{_now()}.prof"

    @property
    def report_path(self) -> Path:
        """Return the path of the profiler report."""
        if self._expid:
            return Path(
                BasicConfig.LOCAL_ROOT_DIR,
                self._expid,
                "tmp",
                "profile",
            )

        return Path(
            BasicConfig.GLOBAL_LOG_DIR,
            "profile",
        )

    def _report(self) -> None:
        """Generate and save the profiling reports.

        :raises AutosubmitCritical: If the report directory is not writable.
        """
        self.report_path.mkdir(
            parents=True,
            exist_ok=True,
        )
        self.report_path.chmod(0o755)

        if not os.access(self.report_path, os.W_OK):
            raise AutosubmitCritical(
                f"Directory {self.report_path} not writable. Please check permissions.",
                7012,
            )

        with io.StringIO() as stream:
            stats = pstats.Stats(
                self._profiler,
                stream=stream,
            )
            stats.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats()

            report_lines = [
                _generate_title("Time & Calls Profiling"),
                "",
                stream.getvalue().rstrip(),
            ]

            if self.checkpoint_count:
                report_lines.extend(
                    [
                        "",
                        _generate_title(
                            "Memory, Object and File Descriptor Usage by Iteration"
                        ),
                        "",
                        *self._report_growth(),
                    ]
                )

            # Calculate memory growth directly from the authoritative
            # initial and final RSS measurements. Do not rely on a cached
            # derived value here.
            memory_growth = self._mem_final - self._mem_initial

            report_lines.extend(
                [
                    "",
                    _generate_title(
                        "Overall Memory, Object and File Descriptor Growth"
                    ),
                    "",
                    "Memory:",
                    f"  Initial : {_format_bytes(self._mem_initial)}",
                    f"  Final   : {_format_bytes(self._mem_final)}",
                    f"  Growth  : {_format_signed_bytes(memory_growth)}",
                ]
            )

            if self._obj_growth:
                report_lines.extend(
                    [
                        "",
                        "Objects:",
                        f"  Growth  : {_format_signed_int(self._obj_total_growth)}",
                    ]
                )

            if self._fd_growth:
                report_lines.extend(
                    [
                        "",
                        "File descriptors:",
                        f"  Growth  : {_format_signed_int(self._fd_total_growth)}",
                    ]
                )

            final_fd_names = _get_current_open_fds_names(self._process)

            if final_fd_names:
                report_lines.extend(
                    [
                        "",
                        _generate_title("Final Open File Descriptors"),
                        "",
                        *(f"  {fd_name}" for fd_name in final_fd_names),
                    ]
                )

            if self._trace_enabled and self._obj_diffs_between_iter:
                report_lines.extend(
                    [
                        "",
                        _generate_title("Unique Object Tracebacks Between Iterations"),
                        "",
                        *(
                            f"  {traceback}"
                            for traceback in sorted(
                                self._obj_diffs_between_iter,
                                key=str,
                            )
                        ),
                    ]
                )

            report = "\n".join(report_lines)

            report = report.replace(
                "{",
                "{{",
            ).replace(
                "}",
                "}}",
            )

            Log.info(report)

            report_file_path = self.report_path / self.file_name
            stats.dump_stats(report_file_path)

            report_log_path = report_file_path.with_suffix(".txt")

            report_log_path.write_text(
                report,
                encoding="UTF-8",
            )

            Log.info(
                f"You can also find the report and profiler files at "
                f"{report_log_path}\n"
            )


def _calculate_growth(
    values: list[int],
) -> list[int]:
    """Calculate growth between consecutive values.

    :param values: Values recorded at each checkpoint.
    :return: Differences between consecutive values.
    """
    return [current - previous for previous, current in itertools.pairwise(values)]


def _calculate_fd_growth(
    fd_values: list[int | None],
) -> list[int]:
    """Calculate growth between consecutive valid FD counts.

    :param fd_values: FD counts recorded at each checkpoint.
    :return: Differences between consecutive valid FD counts.
    """
    growth: list[int] = []

    for previous, current in itertools.pairwise(fd_values):
        if previous is not None and current is not None:
            growth.append(current - previous)

    return growth


def _calculate_fd_total_growth(
    fd_values: list[int | None],
    baseline_index: int,
) -> int:
    """Calculate total FD growth from a baseline checkpoint.

    :param fd_values: FD counts recorded at each checkpoint.
    :param baseline_index: Index of the baseline checkpoint.
    :return: FD growth, or zero when either value is unavailable.
    """
    final_value = fd_values[-1]
    baseline_value = fd_values[baseline_index]

    if final_value is None or baseline_value is None:
        return 0

    return final_value - baseline_value


def _format_fd_changes(
    previous_names: list[str],
    current_names: list[str],
) -> list[str]:
    """Format opened and closed file descriptors.

    :param previous_names: FD names from the previous checkpoint.
    :param current_names: FD names from the current checkpoint.
    :return: Formatted FD changes.
    """
    lines: list[str] = []

    previous_set = set(previous_names)
    current_set = set(current_names)

    for name in sorted(current_set - previous_set):
        lines.append(f"Opened: {name}")

    for name in sorted(previous_set - current_set):
        lines.append(f"Closed: {name}")

    return lines


def _is_autosubmit_traceback(
    traceback: tracemalloc.Traceback,
) -> bool:
    """Return whether a traceback contains Autosubmit code.

    The profiler itself is excluded so that its bookkeeping does not
    appear as application growth.

    :param traceback: Tracemalloc traceback.
    :return: True when the traceback belongs to Autosubmit.
    """
    return any(
        "Autosubmit" in frame.filename and "profiler.py" not in frame.filename
        for frame in traceback
    )


def _generate_title(
    title: str = "",
) -> str:
    """Generate a title banner with the specified text.

    :param title: The title to display in the banner.
    :return: The banner with the specified title.
    """
    separator = "=" * 80

    return "\n".join(
        [
            separator,
            title.center(80),
            separator,
        ]
    )


def _get_current_memory(
    process: Process,
) -> int:
    """Return the current process RSS in bytes."""
    return process.memory_info().rss


def _get_current_object_count() -> int:
    """Return the number of tracked Python objects."""
    return len(gc.get_objects())


def _get_current_open_fds(
    process: Process,
) -> int | None:
    """Return the number of open file descriptors or handles.

    ``num_fds`` is available on Unix-like systems and ``num_handles`` is
    available on Windows. ``None`` is returned when neither is available.
    """
    if hasattr(process, "num_fds"):
        return int(process.num_fds())

    if hasattr(process, "num_handles"):
        return int(process.num_handles())

    return None


def _format_socket_address(
    address: Any,
) -> str:
    """Format a psutil socket address.

    :param address: Address returned by psutil.
    :return: Human-readable address, or an empty string when unsupported.
    """
    if address is None:
        return ""

    if isinstance(address, str):
        return address

    ip = getattr(address, "ip", None)
    port = getattr(address, "port", None)

    if ip is not None and port is not None:
        return f"{ip}:{port}"

    path = getattr(address, "path", None)

    if path is not None:
        return str(path)

    return ""


def _get_fd_connection_map(
    process: Process,
) -> dict[int, str]:
    """Build a map of FD numbers to human-readable socket descriptions.

    :param process: The psutil process to inspect.
    :return: Mapping of FD number to socket description.
    """
    fd_to_connection: dict[int, str] = {}

    try:
        connections = process.net_connections(kind="all")
    except OSError:
        return fd_to_connection

    for connection in connections:
        if connection.fd < 0:
            continue

        description = _format_connection(connection)

        if description:
            fd_to_connection[connection.fd] = description

    return fd_to_connection


def _format_connection(
    connection: Any,
) -> str:
    """Format one psutil network connection."""
    if connection.family == socket.AF_UNIX:
        address = _format_socket_address(connection.laddr)
        return f"unix-socket: {address or '(unnamed)'}"

    local_address = _format_socket_address(connection.laddr)
    remote_address = _format_socket_address(connection.raddr)

    if not local_address and not remote_address:
        return ""

    direction = local_address

    if remote_address:
        direction = f"{local_address} -> {remote_address}"

    status = f" [{connection.status}]" if connection.status else ""

    return f"socket: {direction}{status}"


def _get_pipe_direction(
    pid: int,
    fd_num: int,
) -> str:
    """Return the access direction of a Linux pipe file descriptor.

    :param pid: Process ID.
    :param fd_num: File descriptor number.
    :return: ``read``, ``write``, or ``unknown``.
    """
    try:
        with open(f"/proc/{pid}/fdinfo/{fd_num}", encoding="utf-8") as file:
            for line in file:
                if line.startswith("flags:"):
                    flags = int(line.split(":", 1)[1].strip(), 8)
                    break
            else:
                return "unknown"
    except (OSError, ValueError):
        return "unknown"

    # The flags are stored in /proc as an octal number.
    # 0100000 is used here for the read end of a pipe. It is not part of
    # the usual O_RDONLY/O_WRONLY access-mode bits, so check it first.
    if flags == 0o100000:
        return "read"

    # For the write end, the access mode is stored in the lowest bits.
    # O_WRONLY means that the descriptor is used for writing.
    access_mode = flags & os.O_ACCMODE

    if access_mode == os.O_WRONLY:
        return "write"

    return "unknown"


def _get_current_open_fds_names(
    process: Process,
) -> list[str]:
    """Return human-readable names of the current process FDs.

    On Linux, ``/proc/<pid>/fd`` is used because psutil does not expose
    equivalent names for arbitrary files, pipes and standard streams.

    :param process: The psutil process to inspect.
    :return: Annotated FD descriptor strings.
    """
    pid = process.pid
    fd_dir = Path("/proc") / str(pid) / "fd"

    if not fd_dir.is_dir():
        return []

    fd_to_connection = _get_fd_connection_map(process)

    standard_fds = {
        0: "stdin",
        1: "stdout",
        2: "stderr",
    }

    try:
        fd_entries = os.listdir(fd_dir)
    except OSError:
        return []

    names: list[str] = []

    for fd_name in sorted(
        fd_entries,
        key=lambda value: int(value) if value.isdigit() else 0,
    ):
        if not fd_name.isdigit():
            continue

        fd_num = int(fd_name)

        try:
            target = os.readlink(fd_dir / fd_name)
        except OSError:
            continue

        label = _format_fd_label(
            pid,
            fd_num,
            target,
            fd_to_connection,
            standard_fds,
        )

        names.append(f"[fd={fd_num}] {label}")

    return names


def _format_fd_label(
    pid: int,
    fd_num: int,
    target: str,
    fd_to_connection: dict[int, str],
    standard_fds: dict[int, str],
) -> str:
    """Return a human-readable label for one file descriptor.

    :param pid: Process ID.
    :param fd_num: File descriptor number.
    :param target: Target reported by ``/proc/<pid>/fd``.
    :param fd_to_connection: Socket descriptions indexed by FD.
    :param standard_fds: Standard stream names indexed by FD.
    :return: Human-readable FD label.
    """
    if fd_num in fd_to_connection:
        return fd_to_connection[fd_num]

    if fd_num in standard_fds:
        return f"{standard_fds[fd_num]} ({target})"

    if target.startswith("pipe:"):
        direction = _get_pipe_direction(
            pid,
            fd_num,
        )
        return f"pipe ({direction}) {target}"

    return target
