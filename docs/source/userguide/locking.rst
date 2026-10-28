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

The database connection uses ``AUTOCOMMIT`` isolation to ensure that the
advisory lock remains independent of transaction boundaries.

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

If acquiring or releasing the lock fails due to a database error, the
connection is invalidated to ensure that the lock cannot remain associated
with a connection that is returned to the connection pool.

Lock configuration
------------------

The experiment locking behaviour can be configured in the ``autosubmitrc``
configuration file.

The following options are available:

.. code-block:: ini

   [experiment_lock]
   timeout = 5.0
   retry_interval = 0.5

``timeout``
~~~~~~~~~~~

Maximum time, in seconds, to wait when acquiring an experiment lock.

If the lock cannot be acquired within this period, Autosubmit raises an
``ExperimentAlreadyLockedException``.

Default value: ``1.0`` seconds.

``retry_interval``
~~~~~~~~~~~~~~~~~~

Time, in seconds, between consecutive attempts to acquire an experiment lock.

This controls how frequently Autosubmit retries when the lock is already held
by another process.

Default value: ``0.25`` seconds.

Both options apply to file-based and PostgreSQL-based locking.

If the options are not specified in ``autosubmitrc``, their default values
are used.

Example configuration
---------------------

The following example configures PostgreSQL as the database backend and sets
a five-second timeout with a retry interval of half a second:

.. code-block:: ini

   [database]
   backend = postgres
   connection_url = postgresql://autosubmit:autosubmit@postgres:5432/autosubmit
   path = /app/database/

   [experiment_lock]
   timeout = 5.0
   retry_interval = 0.5

With this configuration, Autosubmit waits up to five seconds to acquire an
experiment lock, retrying every half a second.

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
