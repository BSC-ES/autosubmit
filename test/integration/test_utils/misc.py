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

"""Miscellaneous utilities for integration tests."""

from contextlib import suppress
from pathlib import Path
from time import sleep, time
from typing import TYPE_CHECKING

from portalocker import Lock, AlreadyLocked

if TYPE_CHECKING:
    pass

__all__ = [
    'wait_child',
    'wait_locker'
]


def wait_child(timeout, retry=3):
    """A parametrized fixture that will retry function X amount of times waiting for a child process to be executed.

    In case it still fails after X retries, an exception is thrown."""

    def the_real_decorator(function):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < retry:
                # noinspection PyBroadException
                try:
                    value = function(*args, **kwargs)
                    if value is None:
                        return
                except Exception:
                    sleep(timeout)
                    retries += 1

        return wrapper

    return the_real_decorator


def wait_locker(file_lock: Path, expect_locked: bool, timeout: int, interval=0.05) -> None:
    """Waits until a file is locked or unlocked."""
    start = time()
    while True:
        elapsed = time() - start
        if elapsed > timeout:
            raise TimeoutError(f"File lock {'not ' if expect_locked else ''}acquired at {file_lock} "
                               f"({timeout}s timeout, {elapsed:.2f}s elapsed)")

        if expect_locked:
            # Check if the file is locked by attempting to acquire it non-blocking
            try:
                with Lock(str(file_lock), timeout=0):
                    # Lock acquired, so it's NOT locked — keep waiting
                    pass
            except AlreadyLocked:
                return  # Lock is held
        else:
            # Wait for the lock to be released
            with suppress(AlreadyLocked):
                try:
                    with Lock(str(file_lock), timeout=0.01):
                        return  # Lock released
                except AlreadyLocked:
                    pass

        sleep(interval)
