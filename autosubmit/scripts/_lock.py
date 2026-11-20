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

"""Code for CLI experiment locking.

This is only for the command-line interface locking, in sub-commands
and arguments. For the actual locking of experiments, please refer
to :py:mod:`autosubmit.experiment.lock`.
"""

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from enum import Enum, auto
from typing import TYPE_CHECKING

from autosubmit.exceptions import ExperimentAlreadyLockedException
from autosubmit.experiment.lock import experiment_lock

if TYPE_CHECKING:
    from autosubmit.scripts._args import AutosubmitOptions

__all__ = ["LockPolicy", "manage_experiment_lock"]


class LockPolicy(Enum):
    """The sub-command locking policy.

    - NONE: No locking.
    - REQUIRED: The sub-command will work only if the lock is obtained.
    - OPTIONAL: The sub-command will work regardless of the locking policy.

    Note that with ``OPTIONAL``, a sub-command will work even without the lock,
    but its behaviour may differ.
    """

    NONE = auto()
    REQUIRED = auto()
    OPTIONAL = auto()


@contextmanager
def manage_experiment_lock(
    lock_policy: LockPolicy,
    opts: "AutosubmitOptions",
) -> Iterator[None]:
    """Manage the experiment lock according to the specified policy.

    - NONE: Do not acquire the lock.
    - REQUIRED: Acquire the lock or raise an exception.
    - OPTIONAL: Try to acquire the lock, but continue if acquisition fails.

    Sets ``opts._is_locked`` to ``True`` if the lock was acquired,
    ``False`` otherwise.
    """
    opts._is_locked = False

    if lock_policy == LockPolicy.NONE:
        yield
        return

    with ExitStack() as stack:
        try:
            stack.enter_context(experiment_lock(opts.expid))
        except ExperimentAlreadyLockedException:
            if lock_policy == LockPolicy.REQUIRED:
                raise

            yield
            return

        opts._is_locked = True
        yield
