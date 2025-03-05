"""
A wrapper around the tracemalloc standard library module.
"""
import functools
import logging
import sys
import tracemalloc

import typing


class TraceMalloc:
    """A wrapper around the tracemalloc module that can compensate for tracemalloc's memory usage."""

    # Central flag to control all instances of TraceMalloc's
    TRACE_ON = True
    ALLOWED_GRANULARITY = ('filename', 'lineno', 'traceback')

    def __init__(self, statistics_granularity: str = 'lineno'):
        """statistics_granularity can be 'filename', 'lineno' or 'traceback'."""
        if statistics_granularity not in self.ALLOWED_GRANULARITY:
            raise ValueError(
                f'statistics_granularity must be in {self.ALLOWED_GRANULARITY} not {statistics_granularity}'
            )
        self.statistics_granularity = statistics_granularity
        if self.TRACE_ON:
            if not tracemalloc.is_tracing():
                tracemalloc.start()
            self.tracemalloc_snapshot_start: typing.Optional[tracemalloc.Snapshot] = None
            self.tracemalloc_snapshot_finish: typing.Optional[tracemalloc.Snapshot] = None
            self.memory_start: typing.Optional[int] = None
            self.memory_finish: typing.Optional[int] = None
            self.statistics: typing.List[tracemalloc.StatisticDiff] = []
            self._diff: typing.Optional[int] = None

    def __enter__(self):
        """Take a tracemalloc snapshot."""
        if self.TRACE_ON:
            self.tracemalloc_snapshot_start = tracemalloc.take_snapshot()
            self.memory_start = tracemalloc.get_tracemalloc_memory()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Take a tracemalloc snapshot and subtract the initial snapshot. Also note the tracemalloc memory usage."""
        if self.TRACE_ON:
            self.tracemalloc_snapshot_finish = tracemalloc.take_snapshot()
            self.memory_finish = tracemalloc.get_tracemalloc_memory()
            self.statistics = self.tracemalloc_snapshot_finish.compare_to(
                self.tracemalloc_snapshot_start, self.statistics_granularity
            )
            self._diff = None
        return False

    @property
    def tracemalloc_memory_usage(self) -> typing.Optional[int]:
        """Returns the tracemalloc memory usage between snapshots of None of no tracing."""
        if self.TRACE_ON:
            return self.memory_finish - self.memory_start

    @property
    def diff(self) -> int:
        """The net memory usage difference recorded by tracemalloc allowing for the memory usage of tracemalloc."""
        if self.TRACE_ON:
            if self._diff is None:
                self._diff = sum(s.size_diff for s in self.statistics) - self.tracemalloc_memory_usage
            return self._diff
        return -sys.maxsize - 1

    def net_statistics(self):
        """Returns the list of statistics ignoring those from the tracemalloc module itself."""
        ret = []
        for statistic in self.statistics:
            file_name = statistic.traceback[0].filename
            if file_name != tracemalloc.__file__:
                ret.append(statistic)
        return ret


def trace_malloc_log(log_level: int):
    """Decorator that logs the decorated function the use of Python memory in bytes at the desired log level.
    This can be switched to a NOP by setting TraceMalloc.TRACE_ON to False."""
    def memory_inner(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with TraceMalloc() as tm:
                result = fn(*args, ** kwargs)
            logging.log(log_level, f'TraceMalloc memory delta: {tm.diff:,d} for "{fn.__name__}()"')
            return result
        return wrapper
    return memory_inner
