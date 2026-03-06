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

import cProfile
import gc
import io
import os
import pstats
import signal
import sys
import tracemalloc
from datetime import datetime
from enum import Enum
from pathlib import Path
from pstats import SortKey

from psutil import Process

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import Log, AutosubmitCritical

_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]


class ProfilerState(Enum):
    """Enumeration of profiler states"""
    STOPPED = "stopped"
    STARTED = "started"


class Profiler:
    """Class to profile the execution of experiments."""

    def __init__(self, expid: str, trace_enabled: bool = False, max_checkpoints: int = 0):
        """Initialize the profiler with an experiment ID.

        :param expid: The experiment identifier.
        :type expid: str
        """
        self._profiler = cProfile.Profile()
        self._expid = expid

        # Memory profiling variables
        self._mem_init = 0.0
        self._mem_final = 0.0
        self.max_checkpoints = max_checkpoints
        self.checkpoints = 0

        # Error handling
        self._state = ProfilerState.STOPPED

        # Run exclusive iteration profiling variables

        self._mem_iteration: list = []

        # Object profiling variables
        self._obj_iteration: list = []
        self._obj_grow: list = []

        # File descriptor / handle profiling variables
        self._fd_iteration: list = []
        self._fd_grow: list = []

        # Workflow stats
        self._jobs_iteration: list = []
        self._edges_iteration: list = []

        # Allocation tracing
        self._trace_enabled = trace_enabled
        self._trace_snapshots: list = []
        self._trace_stats_by_iter: list = []

        self._mem_grow: list = []
        self._mem_total_grow: float = 0.0
        self._obj_total_grow: int = 0
        self._fd_total_grow: int = 0

    @property
    def started(self) -> bool:
        """Check if the profiler is in the started state.

        :return: True if profiler is started, False otherwise.
        :rtype: bool
        """
        return self._state == ProfilerState.STARTED

    @property
    def stopped(self):
        """Check if the profiler is in the stopped state.
        :return: True if profiler is stopped, False otherwise.
        :rtype: bool
        """
        return self._state == ProfilerState.STOPPED

    def start(self) -> None:
        """Start the profiling process.

        :raises AutosubmitCritical: If the profiler was already started.
        """
        if self.started:
            raise AutosubmitCritical('The profiling process was already started.', 7074)

        self._state = ProfilerState.STARTED
        self._profiler.enable()
        gc.collect()
        self._mem_init = _get_current_memory()

        if self._trace_enabled and not tracemalloc.is_tracing():
            tracemalloc.start()

    def iteration_checkpoint(self, loaded_jobs: int, loaded_edges: int) -> bool:
        """Record metrics at the checkpoint of an iteration.
        :param loaded_jobs: The number of jobs loaded in the current iteration.
        :param loaded_edges: The number of edges loaded in the current iteration.
        :return: True if the maximum number of checkpoints has been reached, False otherwise.
        :rtype: bool
        """
        gc.collect()
        self._mem_iteration.append(_get_current_memory())
        self._obj_iteration.append(_get_current_object_count())
        self._fd_iteration.append(_get_current_open_fds())
        if self._trace_enabled and tracemalloc.is_tracing():
            snapshot = tracemalloc.take_snapshot()
            self._trace_stats_by_iter.append(
                self._capture_allocation_delta(snapshot)
            )
            self._trace_snapshots.append(snapshot)

        self._jobs_iteration.append(loaded_jobs)
        self._edges_iteration.append(loaded_edges)

        self._mem_iteration[-1] -= sys.getsizeof(self._mem_iteration) + sys.getsizeof(self._obj_iteration) + sys.getsizeof(self._fd_iteration) + sys.getsizeof(
            self._jobs_iteration) + sys.getsizeof(self._edges_iteration)
        if self.max_checkpoints != 0:
            self.checkpoints += 1
            if self.checkpoints > self.max_checkpoints:
                # send signal so Autosubmit.exit is 1
                return True
        return False

    def stop(self) -> None:
        """Finish the profiling process and generate reports.

        :raises AutosubmitCritical: If the profiler was not running.
        """
        if not self.started or self.stopped:
            raise AutosubmitCritical('Cannot stop the profiler because it was not running.', 7074)

        self._profiler.disable()
        if self._mem_iteration:
            self._mem_init = self._mem_iteration[0]  # Remove the initial memory value from the iteration list
            self._mem_final = self._mem_iteration[-1]
            self._calculate_grow()
        else:
            self._mem_final = _get_current_memory()

        self._report()
        self._state = ProfilerState.STOPPED

        if self._trace_enabled and tracemalloc.is_tracing():
            tracemalloc.stop()

    def _calculate_grow(self) -> None:
        """Calculate total growth metrics for objects and file descriptors."""

        # grow by iteration
        self._mem_grow = [self._mem_iteration[i] - self._mem_iteration[i - 1]
                          for i in range(1, len(self._mem_iteration))]
        self._obj_grow = [self._obj_iteration[i] - self._obj_iteration[i - 1]
                          for i in range(1, len(self._obj_iteration))]
        self._fd_grow = [self._fd_iteration[i] - self._fd_iteration[i - 1]
                         for i in range(1, len(self._fd_iteration))]

        # total grow
        self._mem_total_grow = self._mem_iteration[-1] - self._mem_iteration[0] if self._mem_iteration else 0
        self._obj_total_grow = self._obj_iteration[-1] - self._obj_iteration[0] if self._obj_iteration else 0
        self._fd_total_grow = self._fd_iteration[-1] - self._fd_iteration[0] if self._fd_iteration else 0

    def _report_grow(self) -> str:
        """Append growth metrics to the report.

        :return: The updated report string with growth metrics.
        :rtype: str
        """
        report = ""
        for i in range(len(self._mem_iteration[1:-1])):
            mem = self._mem_iteration[i]
            obj = self._obj_iteration[i]
            fd = self._fd_iteration[i]

            mem_unit = 0
            while mem >= 1024 and mem_unit <= len(_UNITS) - 1:
                mem_unit += 1
                mem /= 1024

            report += f"Iteration {i + 1}:\n"
            report += f"  Memory: {mem:.2f} {_UNITS[mem_unit]}\n"
            report += f"  Objects: {obj}\n"
            report += f"  File Descriptors: {fd}\n"
            report += f"  Loaded jobs: {self._jobs_iteration[i]}\n"
            report += f"  Loaded edges: {self._edges_iteration[i]}\n"

            if i < len(self._trace_stats_by_iter):
                report += self._format_top_allocations(self._trace_stats_by_iter[i])
        return report

    def _capture_allocation_delta(self, snapshot: tracemalloc.Snapshot) -> list:
        """Return top allocation deltas since the previous snapshot.

        :param snapshot: The current tracemalloc snapshot.
        :return: A list of tracemalloc StatisticDiff entries.
        :rtype: list
        """
        if not self._trace_snapshots:
            return []
        previous = self._trace_snapshots[-1]
        stats = snapshot.compare_to(previous, "lineno")
        return [stat for stat in stats if stat.size_diff > 0][:5]

    def _format_top_allocations(self, stats: list) -> str:
        """Format tracemalloc allocation deltas for the report.

        :param stats: Allocation delta statistics.
        :return: A formatted string for the report.
        :rtype: str
        """
        if not stats:
            return ""
        lines = ["  Top allocation deltas:\n"]
        for stat in stats:
            frame = stat.traceback[0]
            lines.append(
                f"    {frame.filename}:{frame.lineno} "
                f"+{stat.size_diff / 1024:.1f} KiB "
                f"({stat.count_diff:+d} blocks)\n"
            )
        return "".join(lines)

    def _report(self) -> None:
        """Print the final report to stdout, log, and filesystem.

        :raises AutosubmitCritical: If the report directory is not writable.
        """
        # Create the profiler path if it does not exist
        report_path = Path(BasicConfig.LOCAL_ROOT_DIR, self._expid, "tmp", "profile")
        report_path.mkdir(parents=True, exist_ok=True)
        report_path.chmod(0o755)
        if not os.access(report_path, os.W_OK):  # Check for write access
            raise AutosubmitCritical(
                f'Directory {report_path} not writable. Please check permissions.', 7012)

        stream = io.StringIO()
        date_time = datetime.now().strftime('%Y%m%d-%H%M%S')

        # Generate function-by-function profiling results
        sort_by = SortKey.CUMULATIVE
        stats = pstats.Stats(self._profiler, stream=stream)  # generate statistics
        stats.strip_dirs().sort_stats(sort_by).print_stats()  # format and save in the stream

        # Create and save report
        report = "\n".join([
            _generate_title("Time & Calls Profiling"),
            "",
            stream.getvalue(),
            ""
        ])
        # Generate memory profiling results
        if self._mem_grow and self._obj_grow and self._fd_grow:
            report += "\n" + _generate_title("Memory, object and file descriptor by iteration") + "\n"
            report += self._report_grow()
        report += "\n" + _generate_title("Overall Memory, Object and File Descriptor Growth") + "\n"

        mem_total: float = self._mem_final - self._mem_init  # memory in Bytes
        mem_init = self._mem_init
        mem_final = self._mem_final
        unit = 0
        # reduces the value to its most suitable unit
        while mem_total >= 1024 and unit <= len(_UNITS) - 1:
            unit += 1
            mem_total /= 1024
        unit = 0
        while mem_init >= 1024 and unit <= len(_UNITS) - 1:
            unit += 1
            mem_init /= 1024
        unit = 0
        while mem_final >= 1024 and unit <= len(_UNITS) - 1:
            unit += 1
            mem_final /= 1024
        report += f"\nMEMORY GROW: {mem_total:.2f} {_UNITS[unit]}."
        if self._obj_grow and self._fd_grow:
            report += f"\nOBJECTS GROW: {self._obj_total_grow} objects."
            report += f"\nFILE DESCRIPTORS GROW: {self._fd_total_grow} file descriptors."
        report += f"\nINITIAL MEMORY: {mem_init:.2f} {_UNITS[unit]}."
        report += f"\nFINAL MEMORY: {mem_final:.2f} {_UNITS[unit]}."
        report = report.replace('{', '{{').replace('}', '}}')
        Log.info(report)

        stats.dump_stats(Path(report_path, f"{self._expid}_profile_{date_time}.prof"))
        with open(Path(report_path, f"{self._expid}_profile_{date_time}.txt"),
                  'w', encoding='UTF-8') as report_file:
            report_file.write(report)

        Log.info(f"[INFO] You can also find report and prof files at {report_path}\n")


def _generate_title(title="") -> str:
    """Generate a title banner with the specified text.

    :param title: The title to display in the banner.
    :type title: str
    :return: The banner with the specified title.
    :rtype: str
    """
    max_len = 80
    separator = "=" * max_len
    message = title.center(max_len)
    return "\n".join([separator, message, separator])


def _get_current_memory() -> int:
    """Return the current memory consumption of the process in Bytes.

    :return: The current memory used by the process in Bytes.
    :rtype: int
    """
    return Process(os.getpid()).memory_info().rss


def _get_current_object_count() -> int:
    """Return total number of tracked Python objects.

    :return: The count of all tracked objects.
    :rtype: int
    """
    return len(gc.get_objects())


def _get_current_open_fds() -> int:
    """Return count of open file descriptors.

    Falls back to approximating using open files and connections
    if direct FD/handle count is unavailable.

    :return: The number of open file descriptors or handles.
    :rtype: int
    """
    proc = Process(os.getpid())
    if hasattr(proc, "num_fds"):
        return proc.num_fds()
    if hasattr(proc, "num_handles"):
        return proc.num_handles()

    # Fallback: approximate using open files + connections
    open_files = len(proc.open_files())
    connections = len(proc.net_connections(kind="all"))
    return open_files + connections
