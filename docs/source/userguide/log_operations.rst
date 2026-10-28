Log operations
=====================

Autosubmit has some utilities to handle the log files that are created during the workflow execution. These utilities can help to save disk space and improve performance when dealing with large log files.


Compressing logs from a remote job execution
------------------------------------------------

You can enable log compression for remote job executions by setting the following options in your platform configuration:

.. code-block:: yaml

    PLATFORMS:
      MN5:
        TYPE: <platform_type>
        HOST: <host_name>
        PROJECT: <project>
        USER: <user>
        SCRATCH: <scratch_dir>
        MAX_WALLCLOCK: <HH:MM>
        QUEUE: <hpc_queue>
        COMPRESS_REMOTE_LOGS: true
        COMPRESSION_TYPE: gzip
        COMPRESSION_LEVEL: 5

In this example, log compression is enabled for the MN5 platform using gzip compression with a compression level of 5.


.. warning:: This compression is applied before transferring the log files from the remote platform to the local machine, helping to reduce transfer times and save bandwidth.
    It uses ``gzip`` or ``xz`` command-line tools, so ensure they are installed on the remote platform. 
    **In case the compression tool is not available or fails, the log files will be transferred without compression.**


The available configuration parameters are as follows:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``COMPRESS_REMOTE_LOGS``
      - Enable or disable log compression for remote job executions. Default is false.
    * - ``COMPRESSION_TYPE``
      - Specify the compression type. Supported types are 'gzip' and 'xz'. Default is 'gzip'.
    * - ``COMPRESSION_LEVEL``
      - Specify the compression level (1-9). Default is 9.


Removing log files after transfer
--------------------------------------

You can configure Autosubmit to remove log files from the remote platform after they have been successfully transferred to the local machine.

.. code-block:: yaml

    PLATFORMS:
      MN5:
        TYPE: <platform_type>
        HOST: <host_name>
        PROJECT: <project>
        USER: <user>
        SCRATCH: <scratch_dir>
        MAX_WALLCLOCK: <HH:MM>
        QUEUE: <hpc_queue>
        REMOVE_LOG_FILES_ON_TRANSFER: true


.. _log-recovery-resiliency:

Log recovery resiliency
-----------------------

Autosubmit recovers the logs and stats of the finished jobs in a separate process.
When that process is enabled, the first start is mandatory: if it cannot be started
from the main process, the run stops instead of silently losing logs and stats.

If the recovery process dies during the run, Autosubmit restarts it and re-queues the
jobs that were still pending. A job that was being processed when the process died may
be recovered again after the restart; that duplicate work is harmless. Once the restart
attempts are exhausted, or when a job cannot be queued, pending logs are recovered in
the main process when ``INLINE_FALLBACK`` is enabled.

If an individual download fails, the error is logged. The recovery process keeps
working on the remaining jobs, but that failed attempt is not retried automatically.

The options below control how the process is restarted when it dies unexpectedly. They
live in the ``RUNTIME`` section of the experiment configuration.

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``CONSECUTIVE_MAX_RESTARTS``
      - Number of consecutive restart attempts before waiting ``RETRY_DELAY`` and trying
        again. Default: ``2``. Set to ``0`` to disable the limit.
    * - ``RETRY_DELAY``
      - Seconds to wait after the consecutive restart attempts are exhausted before trying
        to start the recovery process again. Default: ``600``. Applies with
        ``INLINE_FALLBACK`` both enabled and disabled.
    * - ``MIN_ALIVE_SECONDS``
      - Minimum time the recovery process must stay alive to be considered healthy and
        reset the consecutive-restart counter. Default: ``5``. Set to ``0`` to disable the
        grace period (a crash-looping process is then restarted on every attempt). The
        value is evaluated on the next liveness check, so the effective resolution is the
        main loop interval.
    * - ``INLINE_FALLBACK``
      - If ``true``, while the separate process is not available, pending logs are recovered
        in the main process. Default: ``true``. This option does not apply to the mandatory
        first start of the process.

Example:

.. code-block:: yaml

    RUNTIME:
        LOG_RECOVERY:
            CONSECUTIVE_MAX_RESTARTS: 2
            RETRY_DELAY: 600
            MIN_ALIVE_SECONDS: 5
            INLINE_FALLBACK: true

