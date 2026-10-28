Log operations
==============

Autosubmit has some utilities to handle the log files that are created during the workflow execution. These utilities
can help to save disk space and improve performance when dealing with large log files.


Compressing logs from a remote job execution
--------------------------------------------

You can enable log compression for remote job executions by setting the following options in your platform
configuration:

.. tab-set-code::

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
---------------------------------

You can configure Autosubmit to remove log files from the remote platform after they have been successfully transferred
to the local machine.

.. tab-set-code::

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

If an individual job cannot be recovered, Autosubmit logs the error and leaves the job
pending. The next recovery scan retries it. The run only stops when the recovery process
cannot be started on the first start.

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
    * - ``INLINE_FALLBACK``
      - If ``true``, while the separate process is not available, pending logs are recovered
        in the main process. If an individual job fails, it stays pending for the next
        recovery scan. Default: ``true``. This option does not apply to the mandatory first
        start of the process.

Example:

.. code-block:: yaml

    RUNTIME:
        LOG_RECOVERY:
            CONSECUTIVE_MAX_RESTARTS: 2
            RETRY_DELAY: 600
            INLINE_FALLBACK: true
