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

"""Autosubmit exceptions.

TODO: Slowly add more errors.
TODO: Eventually move existing ``Autosubmit*`` exceptions here.

Issues related (remove this once we fixed the TODOs and these issues):

  - https://github.com/BSC-ES/autosubmit/issues/1199 "Improve our error handling and how we log errors"
  - https://github.com/BSC-ES/autosubmit/issues/2902 "Stop using integer error code, replace by good
    error messages and exceptions"
  - https://github.com/BSC-ES/autosubmit/issues/3158 "[Enhancement] - Hierarchy of errors handles"
"""

__all__ = [
    "ExperimentAlreadyLockedException",
    "ExperimentLockException",
    "FailedToReleaseLockException",
]


class ExperimentLockException(Exception):
    """Experiment locking error."""


class ExperimentAlreadyLockedException(ExperimentLockException):
    """Error locking the experiment."""


class FailedToReleaseLockException(ExperimentLockException):
    """Error releasing the experiment lock.."""
