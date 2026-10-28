Experiment locking
==================

Autosubmit uses experiment locks to prevent multiple processes from modifying
the same experiment simultaneously.

For example, a user should not be able to run commands that modify the same
experiment at the same time. Without locking, concurrent operations could
interfere with each other and leave the experiment in an inconsistent state.

Autosubmit supports two locking mechanisms, depending on the configured
database backend.

Locking mechanisms
------------------

File-based locking
~~~~~~~~~~~~~~~~~~

When using SQLite, Autosubmit relies on ``portalocker`` to implement
experiment locking.

A temporary lock file named ``autosubmit.lock`` is created inside the
experiment's temporary directory.

The lock is automatically released when the command finishes, including when
an exception occurs.

If a process terminates unexpectedly, for example following a ``SIGKILL``
signal or a machine crash, the operating system releases the underlying file
lock when the process exits. The remaining lock file does not prevent
subsequent processes from acquiring the lock.

Database-based locking
~~~~~~~~~~~~~~~~~~~~~~

When using a database backend, Autosubmit uses database advisory locks through
SQLAlchemy.

Currently, only PostgreSQL is supported for database-based experiment locking.
However, using SQLAlchemy provides the possibility of supporting other database
backends in the future, provided they offer equivalent advisory locking
functionality.

Each experiment identifier is converted from base36 (i.e., the normal Autosubmit
experiment IDs, like ``a000``) to an integer, which is used as the key for the
advisory lock.

Autosubmit uses PostgreSQL's ``pg_try_advisory_lock`` function to attempt
acquiring the lock without blocking indefinitely.

The database connection uses ``AUTOCOMMIT`` isolation to avoid maintaining a
database transaction throughout the lifetime of the command. The advisory
lock is session-level and therefore remains held independently of
transaction boundaries.

The connection is kept open for the duration of the command. PostgreSQL
automatically releases the advisory lock when the connection is closed.

If the command finishes normally or raises an exception, Autosubmit explicitly
attempts to release the advisory lock.

If a process terminates unexpectedly (for example, following a ``SIGKILL``
signal or a machine crash), the operating system closes its database
connection. PostgreSQL automatically releases all session-level advisory
locks associated with that connection.

.. note::

   Lock release may not be instantaneous, as PostgreSQL needs to detect
   that the connection has been lost.

If the database connection is lost while the Autosubmit Python process is
still running, PostgreSQL eventually releases the advisory lock when it
detects that the session has terminated.

The Python process is not necessarily notified immediately and may continue
executing operations that do not require database access. However, most
Autosubmit operations interact with the database, so the connection loss
will normally be detected on a subsequent database operation, causing the
command to fail.

There is nevertheless a short window in which another process may acquire
the same experiment lock while the original process is still running. For this
reason, if an experiment crashed unexpectedly users are advised to check whether
another Autosubmit process is still running before retrying the command.

Lock configuration
------------------

The experiment locking behaviour can be configured in the ``autosubmitrc``
configuration file.

The following options are available:

.. code-block:: ini

   [experiment_lock]
   timeout = 10.0
   retry_interval = 0.5

``timeout``
~~~~~~~~~~~

Maximum time, in seconds, to wait when acquiring an experiment lock.

If the lock cannot be acquired within this period, Autosubmit raises an
``ExperimentAlreadyLockedException``.

Default value: ``10.0`` seconds.

``retry_interval``
~~~~~~~~~~~~~~~~~~

Time, in seconds, between consecutive attempts to acquire an experiment lock.

This controls how frequently Autosubmit retries when the lock is already held
by another process.

Default value: ``0.5`` seconds.

Both options apply to file-based and PostgreSQL-based locking.

If the options are not specified in ``autosubmitrc``, their default values
are used.

The timeout and retry interval must both be greater than zero. Additionally,
the retry interval must not exceed the timeout.

Choosing appropriate values
---------------------------

The ``timeout`` determines how long a process waits for another process to
release an experiment lock. A longer timeout may be useful in environments
where lock contention is more frequent or database operations are subject to
higher latency. A shorter timeout allows users to receive feedback sooner
when an experiment is already being modified.

The ``retry_interval`` determines how frequently lock acquisition is
attempted. Shorter intervals make lock acquisition more responsive but may
generate additional database requests when using PostgreSQL. Longer intervals
reduce repeated attempts at the cost of potentially delaying lock acquisition
after the lock becomes available.

In general, the default values of `10` seconds and `0.5` seconds provide a
reasonable balance between responsiveness and unnecessary retries. Most users
should not need to change them.

The configured timeout controls the lock acquisition waiting period, but does
not necessarily impose a strict upper bound on the total acquisition time.
For example, a database query experiencing network delays may take longer than
the configured timeout to return.

When using ``portalocker`` consider where the lock file is stored. If it is
located on a shared filesystem, such as an NFS mount or a filesystem backed by
GPFS or Lustre, lock acquisition may be affected by network latency and filesystem
I/O overhead.

In these environments, reducing the timeout and retry_interval values may not
necessarily improve lock acquisition times. Instead, it may lead to more frequent
attempts without reducing the underlying filesystem latency.

Example configuration
---------------------

The following example configures PostgreSQL as the database backend and sets
a thirty-second timeout with a retry interval of one second:

.. code-block:: ini

   [database]
   backend = postgres
   connection_url = postgresql://autosubmit:autosubmit@postgres:5432/autosubmit
   path = /app/database/

   [experiment_lock]
   timeout = 30.0
   retry_interval = 1

With this configuration, Autosubmit waits up to thirty seconds to acquire an
experiment lock, retrying every second.

Exception handling
------------------

Autosubmit translates locking-related errors into specific exceptions:

* ``ExperimentAlreadyLockedException``:
  Raised when an experiment lock cannot be acquired within the configured
  timeout.

* ``FailedToReleaseLockException``:
  Raised when Autosubmit cannot release an acquired experiment lock.

These exceptions are handled by Autosubmit's command-line error handling
mechanism and reported to the user.
