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


from autosubmit.log.log import Log, AutosubmitError


class LoopDebugger:
    """Minimal run-loop debugger that raises a recoverable error after N iterations.

    The interface intentionally mirrors the relevant subset of
    :class:`~autosubmit.profiler.profiler.Profiler` (``start``,
    ``iteration_checkpoint``, ``stop``) so that ``run_experiment`` can use
    either object interchangeably without extra branching.

    :param max_iterations: Number of loop iterations to allow before raising
        :class:`~autosubmit.log.log.AutosubmitError`.  Must be ``>= 1``.
    :type max_iterations: int
    """

    def __init__(self, max_iterations: int = 1) -> None:
        """Initialise the debugger.

        :param max_iterations: Positive integer controlling after how many
            :meth:`iteration_checkpoint` calls the error is raised.
        :type max_iterations: int
        """
        if max_iterations < 1:
            raise ValueError("max_iterations must be >= 1.")
        self._max_iterations = max_iterations
        self._iteration_count: int = 0

    def start(self) -> None:
        """Mimics `~autosubmit.profiler.profiler.Profiler.start` """
        Log.debug("LoopDebugger started (max_iterations=%d).", self._max_iterations)

    def iteration_checkpoint(self, loaded_jobs: int, loaded_edges: int) -> bool:
        """Increment the iteration counter and raise after *max_iterations*.

        :param loaded_jobs: Passed for interface compatibility; not used.
        :type loaded_jobs: int
        :param loaded_edges: Passed for interface compatibility; not used.
        :type loaded_edges: int
        :return: Always ``False`` – the debugger never requests a clean exit;
            it only raises.
        :rtype: bool
        :raises AutosubmitError: After ``max_iterations`` iterations (code 6014).
        """
        self._iteration_count += 1
        Log.debug(f"Loaded {loaded_jobs} job(s) and {loaded_edges} edge(s).")
        Log.debug(f"LoopDebugger iteration {self._iteration_count}.")
        if self._iteration_count > self._max_iterations:
            self._max_iterations *= 2  # Avoid raising again immediately if the loop continues
            raise AutosubmitError(
                f"[LoopDebugger] Stopping run loop after {self._max_iterations} "
                "iteration(s) as requested by --debug.",
                6014,
            )
        return False

    def stop(self) -> None:
        """Mimics `~autosubmit.profiler.profiler.Profiler.stop` """
        Log.debug(
            "LoopDebugger stopped after %d iteration(s).", self._iteration_count
        )
