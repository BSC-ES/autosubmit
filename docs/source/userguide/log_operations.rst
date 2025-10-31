Log operations
=====================

Autosubmit have some utilities to handle the log files that are created during the workflow execution. These utilities can help to save disk space and improve performance when dealing with large log files.


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
