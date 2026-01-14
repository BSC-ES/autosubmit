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

from time import sleep

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

__all__ = [
    'wait_child',
]


def wait_child(timeout, retry=3):
    """A parametrized fixture that will retry function X amount of times waiting for a child process to be executed.

    In case it still fails after X retries an exception is thrown."""

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
