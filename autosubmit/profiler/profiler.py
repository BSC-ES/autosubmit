#!/usr/bin/env python3

# Copyright 2015-2024 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import cProfile
import io
import os
import pstats
from enum import Enum
from datetime import datetime
from pathlib import Path
from pstats import SortKey

import psutil
from autosubmitconfigparser.config.basicconfig import BasicConfig

from log.log import Log, AutosubmitCritical


_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]

class ProfilerState(Enum):
    """Enumeration of profiler states"""
    STOPPED = "stopped"
    STARTED = "started"

class Profiler:
    """Class to profile the execution of experiments."""

    def __init__(self, expid:str):
        self._profiler = cProfile.Profile()
        self._expid = expid

        # Memory profiling variables
        self._mem_init = 0
        self._mem_final = 0

        # Error handling
        self._state = ProfilerState.STOPPED

    @property
    def started(self):
        """
        Check if the profiler is in the started state.

        Returns:
            bool: True if the profiler is in the started state, False otherwise.
        """
        return self._state == ProfilerState.STARTED

    @property
    def stopped(self):
        """
        Check if the profiler is in the stopped state.

        Returns:
            bool: True if the profiler is in the stopped state, False otherwise.
        """
        return self._state == ProfilerState.STOPPED

    def start(self) -> None:
        """Function to start the profiling process."""
        if self.started:
            raise AutosubmitCritical('The profiling process was already started.', 7074)

        self._state = ProfilerState.STARTED
        self._profiler.enable()
        self._mem_init += _get_current_memory()

    def stop(self) -> None:
        """Function to finish the profiling process."""
        if not self.started or self.stopped:
            raise AutosubmitCritical('Cannot stop the profiler because it was not running.', 7074)

        self._profiler.disable()
        self._mem_final += _get_current_memory()
        self._report()
        self._state = ProfilerState.STOPPED

    def _report(self) -> None:
        """Function to print the final report into the stdout, log and filesystem."""

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
        stats.strip_dirs().sort_stats(sort_by).print_stats() # format and save in the stream

        # Generate memory profiling results
        mem_total : float = self._mem_final - self._mem_init  # memory in Bytes
        unit = 0
        # reduces the value to its most suitable unit
        while mem_total >= 1024 and unit <= len(_UNITS) - 1:
            unit += 1
            mem_total /= 1024

        # Create and save report
        report = "\n".join([
            _generate_title("Time & Calls Profiling"),
            "",
            stream.getvalue(),
            _generate_title("Memory Profiling"),
            f"MEMORY CONSUMPTION: {mem_total} {_UNITS[unit]}.",
            ""
        ]).replace('{', '{{').replace('}', '}}') # escape {} so Log can call str.format

        Log.info(report)

        stats.dump_stats(Path(report_path, f"{self._expid}_profile_{date_time}.prof"))
        with open(Path(report_path, f"{self._expid}_profile_{date_time}.txt"),
            'w', encoding='UTF-8') as report_file:
            report_file.write(report)

        Log.info(f"[INFO] You can also find report and prof files at {report_path}\n")

def _generate_title(title="") -> str:
    """
    Generates a title banner with the specified text.

    :param title: The title that will be shown in the banner.
    :type title: str
    :return: The banner with the specified title.
    :rtype: str
    """
    max_len = 80
    separator = "=" * max_len
    message = title.center(max_len)
    return "\n".join([separator, message, separator])

def _get_current_memory() -> int:
    """
    Return the current memory consumption of the process in Bytes.

    :return: The current memory used by the process (Bytes).
    :rtype: int
    """
    return psutil.Process(os.getpid()).memory_info().rss
