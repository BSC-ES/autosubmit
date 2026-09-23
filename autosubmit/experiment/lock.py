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

"""The experiment lock, held by commands that modify an experiment."""

from contextlib import AbstractContextManager

from portalocker import TemporaryFileLock

from autosubmit.config.basicconfig import BasicConfig

__all__ = ["experiment_lock"]


def experiment_lock(expid: str) -> AbstractContextManager:
    """Return the lock that stops two commands from modifying one experiment at once.

    ``TemporaryFileLock`` deletes ``autosubmit.lock`` on release, including when the
    command raises. From portalocker 4.0 the file is unlinked while the lock is still
    held, so no other process can end up locking a stale copy of it.

    ``timeout=1`` with ``fail_when_locked=False`` retries for one second, then raises
    ``AlreadyLocked``, which ``exit_from_error`` reports to the user.

    A process killed abruptly (``SIGKILL``, a machine crash) leaves the file behind,
    since neither ``release`` nor the ``atexit`` handler runs. That file is harmless:
    the kernel drops the ``flock`` when the process dies, so the next command acquires
    it normally and deletes it on release.

    :param expid: The experiment identifier.
    :return: A context manager that holds the experiment lock.
    """
    return TemporaryFileLock(
        str(BasicConfig.expid_lock_file(expid)),
        timeout=1,
        fail_when_locked=False,
    )

