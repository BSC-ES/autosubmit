.. _job_retries:

Job Retries
===========

When a job fails, Autosubmit can automatically resubmit it up to a configurable
number of times, optionally with a delay between attempts. This page covers
job-level retries; for SSH-connection and remote-command retries, see
:ref:`ssh_retries` below.

Configuration
-------------

Two settings control job retries. Both go under ``CONFIG`` in
``autosubmit_<EXPID>.yml``:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``RETRIALS``
      - Maximum number of retries after the first failure. Default: ``0``
        (no retries). May be overridden per job under ``JOBS.<section>.RETRIALS``
        or per wrapper under ``WRAPPERS.<section>.RETRIALS``.
    * - ``DELAY_RETRY_TIME``
      - Delay in seconds between retries. Accepts three formats — plain, ``+N``,
        and ``*N`` — described below. Default: ``-1`` (no delay; retry
        immediately).

Delay formats
-------------

``DELAY_RETRY_TIME`` accepts three syntaxes. The prefix (``+`` or ``*``, or none)
tells Autosubmit which formula to use for computing the delay before each retry.

.. list-table::
    :widths: 20 30 50
    :header-rows: 1

    * - Format
      - Formula
      - Behavior
    * - ``N``
      - constant ``N``
      - Wait ``N`` seconds before every retry.
    * - ``+N``
      - ``N × fail_count``
      - Linear growth. Wait ``N``, ``2N``, ``3N``, ... seconds.
    * - ``*N``
      - ``N × 10^(fail_count - 1)``
      - Exponential growth. Wait ``N``, ``10N``, ``100N``, ... seconds.

Worked example with ``N = 11``:

.. list-table::
    :widths: 20 20 20 20
    :header-rows: 1

    * - Retry #
      - ``11``
      - ``+11``
      - ``*11``
    * - 1
      - 11 s
      - 11 s
      - 11 s
    * - 2
      - 11 s
      - 22 s
      - 110 s
    * - 3
      - 11 s
      - 33 s
      - 1 100 s
    * - 4
      - 11 s
      - 44 s
      - 11 000 s

Use the plain or ``+N`` format when failures are expected to be short and
transient (e.g. brief HPC hiccups). Use ``*N`` when a longer back-off is safer,
for example when the platform may be under maintenance.

Example
-------

.. code-block:: yaml

    CONFIG:
        RETRIALS: 3
        DELAY_RETRY_TIME: +30    # wait 30, 60, 90 seconds between retries

A per-job override on the retry count:

.. code-block:: yaml

    JOBS:
        POST:
            FILE: templates/post.sh
            RETRIALS: 5

Wrapper retries
---------------

Wrappers accept their own ``RETRIALS`` setting, which overrides the one on the
inner jobs. See :ref:`wrapper_retrials` for details.

Vertical wrappers retry inner jobs **inside the wrapper submission** rather
than by resubmitting the wrapper. Once a vertical wrapper's inner job has
exhausted its inner retries and the wrapper finishes, Autosubmit will not
retry that inner job again externally.

Template variables
------------------

Retry-related values are exposed as template variables that scripts can
reference:

.. list-table::
    :widths: 30 70
    :header-rows: 1

    * - Variable
      - Value
    * - ``%FAIL_COUNT%``
      - Current retry number (0 on first attempt, incremented on each failure).
    * - ``%RETRIALS%``
      - Configured maximum retry count for the job.

Retry log files
---------------

Each retry writes its own log files in ``LOG_<EXPID>/``, suffixed with the
attempt number:

.. code-block:: text

    <job_name>.<timestamp>.out                  # first attempt
    <job_name>.<timestamp>.err
    <job_name>.<timestamp>.out_attempt_1        # first retry
    <job_name>.<timestamp>.err_attempt_1
    <job_name>.<timestamp>.out_attempt_2        # second retry
    ...

Statistics from ``autosubmit stats`` include per-job retry counts
(``retrialCount``, ``completedCount``, ``failedCount``).

.. _ssh_retries:

SSH connection and command retries
----------------------------------

The retries described above are for **job execution**. Autosubmit also retries
at two lower levels when talking to the remote platform: SSH connections and
remote command execution.

For remote platforms, there are at least two parts where retries happen
(if you use wrappers you may have others), when Autosubmit **connects** to
the remote platform, and when Autosubmit **executes** a command.

When Autosubmit **connects** to a remote platform, it will use the ``host``
value of the platform configuration. This value can contain a single
host name, or a list of host names using commas (``,``) as separators.

Right now Autosubmit has a hard-coded number of retries for connecting
to remote platforms. It will try to connect to the platform, without
interval, **retrying connecting twice (``2``)**. It will write to logs in
``INFO`` and ``WARNING`` levels information about the retries, like
whether it is retrying to connect, and what is the current retry number.

When Autosubmit retries connecting to a platform with multiple hosts
separated by comma, the first connection uses the first host name. If it
retries the connection, the next executions will exclude the first host,
and then randomly select one of the remaining host names.

For **executing** commands on remote platforms, Autosubmit uses another
hard-coded value of ``3`` retries, without interval between each retry.
Autosubmit will submit the command to be executed via SSH. If the command
fails on the remote platform, **Autosubmit will not retry** the command.

As an example, if you try to run an executable such as ``Rscript``, but this
executable does not exist on the remote platform, Autosubmit will log the error,
and mark the job as ``FAILED``.

However, if you have a networking issue between Autosubmit and your remote
platform, then Autosubmit will log in ``INFO`` and ``WARNING`` and **will
retry executing the command up to hard-coded ``3`` retries**.

.. note::
   These SSH-layer retry counts are not user-configurable. An issue is open
   to make them configurable by users and site admins.

For ``ecaccess`` platforms specifically, the ``-retry`` flag count on the
``ecaccess`` binary is user-configurable via ``PLATFORMS.<name>.ECACCESS_RETRIES``
(default ``100``).