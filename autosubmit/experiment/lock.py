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

import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING

from portalocker import TemporaryFileLock
from portalocker.exceptions import BaseLockException
from sqlalchemy import text

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.session import DatabaseBackend, get_engine
from autosubmit.exceptions import (
    ExperimentAlreadyLockedException,
    FailedToReleaseLockException,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

__all__ = ["experiment_lock"]


# These default values were based on the values from portalocker,
# and may have to be fine-tuned later if necessary (users can override).

_DEFAULT_LOCK_TIMEOUT: int | float = 1.0
"""Default timeout used when locking an experiment access.

Measured in seconds.

Used when no configuration file is provided.
"""

_DEFAULT_LOCK_RETRY_INTERVAL: int | float = 0.25
"""Default retry interval used when locking an experiment access.

Measured in seconds.

Used when no configuration file is provided.
"""

_QUERY_OBTAIN_LOCK = "SELECT pg_try_advisory_lock(:key)"
"""Query that tries to obtain a Postgres lock.

``pg_try_advisory_lock`` is similar to ``pg_advisory_lock``, but it is non-blocking.
This is used so we have a kind of spin-and-lock behaviour. 
"""

_QUERY_RELEASE_LOCK = "SELECT pg_advisory_unlock(:key)"
"""Query to release a Postgres lock."""


def experiment_lock(expid: str) -> AbstractContextManager:
    """Return a context manager that locks an Autosubmit experiment.

    The context manager may be file- or database-backed.

    :param expid: Autosubmit experiment identifier.
    :return: A context manager that holds the experiment lock.
    """
    db_backend: DatabaseBackend = DatabaseBackend(BasicConfig.DATABASE_BACKEND)

    if db_backend == DatabaseBackend.POSTGRES:
        return postgres_based_experiment_lock(expid)
    return ExperimentFileLock(expid)


class ExperimentFileLock(AbstractContextManager):
    """A lock that stops two commands from modifying one experiment at once.

    This lock relies on the ``portalocker`` library. The ``portalocker`` exceptions
    are captured and one of the children exceptions to ``ExperimentLockException`` is
    returned.

    ``TemporaryFileLock`` deletes ``autosubmit.lock`` on release, including when the
    command raises. From portalocker 4.0 the file is unlinked while the lock is still
    held, so no other process can end up locking a stale copy of it.

    ``timeout=1`` with ``fail_when_locked=False`` retries for one second, then raises
    ``ExperimentAlreadyLockedException``, which ``exit_from_error`` reports to the user.

    A process killed abruptly (``SIGKILL``, a machine crash) leaves the file behind,
    since neither ``release`` nor the ``atexit`` handler runs. That file is harmless:
    the kernel drops the ``flock`` when the process dies, so the next command acquires
    it normally and deletes it on release.

    See:

    - :func:`autosubmit.scripts._args.exit_from_error`
    - :class:`autosubmit.exceptions.ExperimentLockException`
    - :class:`autosubmit.exceptions.ExperimentAlreadyLockedException`
    - :class:`autosubmit.exceptions.FailedToReleaseLockException`
    """

    def __init__(self, expid: str):
        """Create a lock for the given experiment.

        :param expid: The experiment identifier.
        """
        self.expid = expid
        self._lock = TemporaryFileLock(
            str(BasicConfig.expid_lock_file(expid)),
            timeout=_DEFAULT_LOCK_TIMEOUT,
            check_interval=_DEFAULT_LOCK_RETRY_INTERVAL,
            fail_when_locked=False,
        )

    def __enter__(self) -> None:
        try:
            self._lock.__enter__()
        except BaseLockException as e:
            raise ExperimentAlreadyLockedException(
                f"Experiment {self.expid} is locked."
            ) from e

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        try:
            self._lock.__exit__(exc_type, exc_value, traceback)
        except BaseLockException as e:
            raise FailedToReleaseLockException(
                f"Failed to release lock for experiment {self.expid}."
            ) from e


@contextmanager
def postgres_based_experiment_lock(expid: str) -> Iterator[None]:
    """Return a context manager that locks an experiment using PostgreSQL.

    This lock uses SQLAlchemy and Postgres to create a lock in the database.
    It locks the experiment during the execution of the Autosubmit sub-command,
    independent of transaction boundaries.

    The ``expid`` argument is used to obtain an advisory lock on Postgres.

    If the database connection hold here gets closed (normal termination, or
    exception), the lock is released.

    Refs:

    - `Advisory lock (Wikipedia) <https://en.wikipedia.org/w/index.php?title=Advisory_lock&redirect=no>`__

    :param expid: The experiment identifier.
    :return: A context manager that holds the experiment lock.
    """
    engine: "Engine" = get_engine(BasicConfig.DB_PATH)

    # Autosubmit experiment identifiers are base36 encoded integers.
    # Validation occurs in another part of the code, we are sure it is valid here.
    #   - https://autosubmit.readthedocs.io/en/master/userguide/expids.html
    expid_decoded = int(expid, 36)

    # ``AUTOCOMMIT`` avoids keeping a database transaction open throughout
    # the lifetime of the lock. The advisory lock persists independently of
    # transactions, throughout the execution of the Autosubmit sub-command.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        acquired = False
        # Here we copy the behaviour of ``portalocker``.
        deadline = time.monotonic() + _DEFAULT_LOCK_TIMEOUT

        try:
            while True:
                try:
                    acquired = conn.execute(
                        text(_QUERY_OBTAIN_LOCK), {"key": expid_decoded}
                    ).scalar_one()
                except Exception:
                    conn.invalidate()
                    raise

                if acquired:
                    break

                if time.monotonic() >= deadline:
                    raise ExperimentAlreadyLockedException(
                        f"Experiment {expid} is locked."
                    )

                time.sleep(_DEFAULT_LOCK_RETRY_INTERVAL)

            yield
        finally:
            if acquired:
                try:
                    released = conn.execute(
                        text(_QUERY_RELEASE_LOCK), {"key": expid_decoded}
                    ).scalar_one()

                    if not released:
                        raise FailedToReleaseLockException(
                            f"Failed to release lock for experiment {expid}."
                        )
                except Exception:
                    conn.invalidate()
                    raise
