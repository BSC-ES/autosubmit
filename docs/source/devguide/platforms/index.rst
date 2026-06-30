Platforms
=========
.. |br| raw:: html

    <br />

Extending an Existing Platform
------------------------------

Platforms are defined under Python classes. The source files for such classes are stored inside
``autosubmit/platforms/`` directory. To extend an existing platform we will create a child class from an existing
platform class, for which first we need to identify which existing platform is the most suitable for our project.

.. note::
    Currently the platforms available are:

    - :mod:`Local Platform <autosubmit.platforms.locplatform>` — runs jobs
      directly on the machine where Autosubmit is running. No SSH, no
      scheduler. Used for local preparation steps in a workflow.
    - :mod:`PS Platform <autosubmit.platforms.psplatform>` — a remote host with
      no batch scheduler. Connects via SSH and tracks jobs as remote OS
      processes (using the Unix ``ps`` command).
    - :mod:`EC Platform <autosubmit.platforms.ecplatform>` — ECMWF systems
      reached through the ``ecaccess`` toolchain. The underlying scheduler is
      selected via the platform's ``VERSION`` field (``slurm`` on ECMWF's
      current Atos systems; ``pbs`` is also supported for older
      configurations).
    - :mod:`PJM Platform <autosubmit.platforms.pjmplatform>` — Fujitsu's PJM
      scheduler, used on Fugaku and other large-scale supercomputers.
      Connects via SSH.
    - :mod:`PBS Platform <autosubmit.platforms.pbsplatform>` — Portable Batch
      System (PBS) / PBS Pro / OpenPBS scheduler, used on systems such as
      Miyabi. Connects via SSH.
    - :mod:`Slurm Platform <autosubmit.platforms.slurmplatform>` — the Slurm
      workload manager (MareNostrum, Leonardo, LUMI, etc.). Connects via SSH.
      The tutorial below uses Slurm as its working example.

Composing the Extended Platform Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this page we will be extending the SLURM
platform - source file ``autosubmit/platforms/slurmplatform.py``, see in GitHub `slurmplatform.py <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/platforms/slurmplatform.py#L35>`_ -, but any platform can be extended by following the same steps.

The platform will be transcribing the files and configurations you set manually to allow operations,
and connection to SLURM and its commands, preparing your experiments to be executed transforming configuration
into executable functions.

We will create a new file in ``/autosubmit/platforms/``
and we are going to call it ``slurm_example.py``.

.. code-block:: python
    :linenos:

    from autosubmit.platforms.slurmplatform import SlurmPlatform

    class Slurm_ExamplePlatform(SlurmPlatform):
        """ Class to manage slurm jobs """

This will create a class in which the ``Slurm_ExamplePlatform`` will be associated as its parent class allowing
``Slurm_ExamplePlatform`` inherit all its characteristics.

We create an initialization method with the required parameters.

.. code-block:: python
    :linenos:

    def __init__(self, expid: str, name: str, config: dict):
        """ Initialization of the Class ExamplePlatform

        :param expid: Id of the experiment.
        :type expid: str
        :param name: Name of the platform.
        :type name: str
        :param config: A dictionary containing all the Experiment parameters.
        :type config: dict
        """
        SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)
        self.example_platform_parameter = ... # add any platform specific parameters

As it can be seen, the parent class has an initialization method to invoke all the parent's methods and attributes
into the child (``Slurm_ExamplePlatform``).
In order to override methods from the parent class, we can simply redefine them as shown below, this way we can add
new parameters and/or behaviours, making it possible to add flexibility and restructure a platform for the new needs.

.. code-block:: python
    :linenos:

    def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
        """Submit a job from a given job object."""
        Log.result(f"Job: {job.name}")
        return None

The class ``submit_job`` is a existing class in ``SlurmPlatform`` that was overwritten to have a new behaviour.

After all needed modifications and expansions, the ``Slurm_ExamplePlatform`` class could look similar to the following example code.

.. code-block:: python
    :linenos:

    from typing import Union
    from autosubmit.platforms.slurmplatform import SlurmPlatform

    class Slurm_ExamplePlatform(SlurmPlatform):
        """Class to manage slurm jobs"""
        def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
            """ Initialization of the Class ExamplePlatform

            :param expid: Id of the experiment.
            :type expid: str
            :param name: Name of the platform.
            :type name: str
            :param config: A dictionary containing all the Experiment parameters.
            :type config: dict
            """
            SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)

        def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
            """Submit a job from a given job object."""
            Log.result(f"Job: {job.name}")
            return None


Integrating the Extended Platform into the Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To ensure that the platform will be created as expected, we need to make some changes in 3 different files
|br| ``autosubmit/job/job.py`` - see in GitHub `job.py <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/job/job.py>`_.
|br| ``autosubmit/autosubmit.py`` - see in GitHub `autosubmit.py <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/autosubmit.py>`_.
|br| ``autosubmit/platforms/paramiko_submitter.py`` - see in GitHub `paramiko_submitter.py <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/platforms/paramiko_submitter.py>`_.
|br| ``type`` from ``platform.type`` is defined in the YAML file that configures a platform as it's shown :ref:`here <TargetPlatform>`
to determine the scheduler.

.. warning::
    The numbers noted down to each of the files could become obsolete locally as files get updated so they should be
    seen more as a reference


``autosubmit/autosubmit.py`` in `line 2538 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/autosubmit.py#L2537>`_  add a new ``string`` making sure the new platform type is considered
the same as SLURM platform, as we expect a similar behaviour.

.. code-block:: python
   :emphasize-lines: 1

    if platform.type.lower() in [ "slurm" , "pjm", "example" ] and not inspect and not only_wrappers:
                    # Process the script generated in submit_ready_jobs
                    save_2, valid_packages_to_submit = platform.process_batch_ready_jobs(valid_packages_to_submit,
                                                                                         failed_packages,
                                                                                         error_message="", hold=hold)

``autosubmit/job/job.py`` in `line 2575 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/job/job.py#L2575>`_ ensure each job Job writes
the timestamp to TOTAL_STATS file and jobs_data.db properly.

.. code-block:: python
   :emphasize-lines: 1

    if job_data_dc and type(self.platform) is not str and (self.platform.type in ["slurm", "example"]):
        thread_write_finish = Thread(target=ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR).write_platform_data_after_finish, args=(job_data_dc, self.platform))
        thread_write_finish.name = f"JOB_data_{self.name}"
        thread_write_finish.start()

``autosubmit/job/job.py`` in `line 2817 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/job/job.py#L2817>`_ add a new validation for the validation of the queue
creation with the platform type

.. code-block:: python
    :emphasize-lines: 1

    if self._platform.type in ["slurm", "example"]:
        self._platform.send_command(
            self._platform.get_queue_status_cmd(self.id))
        reason = self._platform.parse_queue_reason(
            self._platform._ssh_output, self.id)


``autosubmit/platforms/paramiko_submitter.py`` in `line 143 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/platforms/paramiko_submitter.py#L143>`_ add a new validation for the header command
creation where the platform type

.. code-block:: python
   :emphasize-lines: 1

    elif platform_type in ["slurm", "example"]:
        remote_platform = SlurmPlatform(
            asconf.expid, section, exp_data, auth_password = auth_password)


How to Configure a Platform
---------------------------

To set up your platform, you first have to create a new experiment by running the following command:
|br| *Change the platform from MARENOSTRUM5 to whichever you will use*

.. parsed-literal::

    autosubmit :ref:`expid <expids>` -H MARENOSTRUM5 -d "platform test" --minimal

This will generate a minimal version of an experiment.

To change the configuration of your experiment to ensure it works properly, you can create a project and customize its parameters. The following instructions are
designed to execute a small job through Autosubmit, explaining how to configure a new platform.

Open the file ``~/autosubmit/<expid>/config/minimal.yml`` and you'll find a file as shown below.

.. code-block:: yaml

    CONFIG:
        AUTOSUBMIT_VERSION: "4.1.12"
        TOTALJOBS: 20
        MAXWAITINGJOBS: 20

    DEFAULT:
        EXPID: <EXPID> # ID of the experiment
        HPCARCH: "MARENOSTRUM5" # This will be the default platform if a job doesn't contain a defined platform
        #hint: use %PROJDIR% to point to the project folder (where the project is cloned)
        CUSTOM_CONFIG: "%PROJDIR%/"

    PROJECT:
        PROJECT_TYPE: local
        PROJECT_DESTINATION: local_project

    GIT:
        PROJECT_ORIGIN: ""
        PROJECT_BRANCH: ""
        PROJECT_COMMIT: ''
        PROJECT_SUBMODULES: ''
        FETCH_SINGLE_BRANCH: true

Now we start configuring the experiment adding the additional ``PARAMETERS`` to create a simple executable experiment

.. code-block:: yaml

    EXPERIMENT:
        DATELIST: 19900101
        MEMBERS: fc0
        CHUNKSIZEUNIT: month
        SPLITSIZEUNIT: day
        CHUNKSIZE: 1
        NUMCHUNKS: 2
        CALENDAR: standard


Add the following PARAMETER which will point towards the folder containing all the scripts and instructions to be
used to execute the experiment in the platform

.. code-block:: yaml

    LOCAL:
        PROJECT_PATH: /home/user/experiment_example # path to your project sources


Autosubmit will copy your sources to the ``$autosubmit_installation/$expid/proj/%PROJECT.PROJECT_DESTINATION%``.

The following settings are used to create a connection with a platform to execute the jobs.
You must input the information suitable for your project (e.g.: user, host, platform).


.. _TargetPlatform:

.. code-block:: yaml

    PLATFORMS:
        MARENOSTRUM5:
            TYPE: <Scheduler> [slurm, ps, example]
            HOST: <Host>
            PROJECT: <Project_Name_Folder>
            USER: <User>
            scratch_dir: <location of project/user>
            QUEUE: gp_debug [dummy, gp_debug, nf, hpc]
            MAX_WALLCLOCK: <HH:MM>
            MAX_PROCESSORS: <N> # This is to enable horizontal_wrappers
            PROCESSORS_PER_NODE: 112 # Each HPC has their own number check the documentation of your platform

.. warning::
    If you cannot connect, it may be because your user doesn't have access to the host, or the PARAMETER SCRATCH_DIR
    might be pointing to a non-existing folder on the host.

    Make sure to create the folder with your USERNAME inside the proper path you pointed to
    (e.g.: <Project_Dir>/<Project_Name_Folder>/<USER>)


.. _platform_connections:

Platform Connections
--------------------

This section describes how Autosubmit interacts with the platforms it has been
configured to use: when it opens connections, when it checks that it has
write access to the remote filesystem, and what it does when a connection has
to be re-established mid-run.

The write-permission check
~~~~~~~~~~~~~~~~~~~~~~~~~~

Before relying on a remote platform, Autosubmit verifies that it can create
and delete entries under the configured ``SCRATCH_DIR``. It does this by
creating a small probe directory under
``<scratch_dir>/<project>/<user>/`` and immediately removing it. The probe is
a directory, not a file; the name it uses depends on the platform type:

.. list-table::
   :header-rows: 1
   :widths: 22 35 43

   * - Platform type
     - Probe directory name
     - How it is created
   * - ``slurm`` / ``pjm`` / ``pbs``
     - ``permission_checker_azxbyc``
     - Over the existing SSH/SFTP session.
   * - ``ps``
     - ``ps_permission_checker_azxbyc``
     - Shell commands over the existing SSH session.
   * - ``ecaccess``
     - ``_permission_checker_azxbyc``
     - Local ``ecaccess-file-*`` commands against the ECMWF gateway. Several parent directories are created first because ``ecaccess-file-mkdir`` has no recursive option.
   * - ``local``
     - (none)
     - The local platform does not perform a write-permission check.

.. note::
    Operators searching log files or the remote scratch filesystem need to
    look for all three directory names, because each platform type uses a
    different one. A search restricted to ``permission_checker_azxbyc`` will
    miss probes from ``ps`` and ``ecaccess`` platforms.

When the write-permission check runs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The check is triggered by a small number of user-facing commands, and the
frequency depends on the command:

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Command
     - When the probe runs
   * - ``autosubmit run``
     - Once per configured platform at the start of the run, before the main loop begins. If the run later encounters a connection error and recovers, the probe runs again once per reconnection attempt against each platform, until reconnection succeeds. The number of reconnection attempts is bounded by ``CONFIG.RECOVERY_RETRIALS`` (default ``3650``), so a prolonged outage can produce many probe entries in the logs.
   * - ``autosubmit setstatus``
     - Once per platform that currently has jobs in ``QUEUING``, ``SUBMITTED`` or ``RUNNING`` state. When ``setstatus`` is run before ``autosubmit create`` (the typical operational case, where all jobs are still ``WAITING`` or ``READY``) no probe is created.
   * - ``autosubmit stop --cancel``
     - Once per configured platform. This applies to any scheduler, not only Slurm; the trigger is the ``--cancel`` flag, not the platform type.

The probe does **not** run during the main loop of ``autosubmit run`` after
startup, during ``autosubmit recovery``, during ``autosubmit create``, or
when Autosubmit reconnects after a transient network error mid-run.

.. _stale_job_data_recovery:

Stale job-data recovery
~~~~~~~~~~~~~~~~~~~~~~~

Autosubmit keeps a history database with one row per job execution. If a job
ran but the row for that execution is incomplete (for example because
Autosubmit was interrupted before the start and end timestamps were
recorded), Autosubmit will try to fetch the missing values from the remote
``STAT`` file for that job. This recovery runs during ``autosubmit run``,
``autosubmit create``, ``autosubmit setstatus`` and ``autosubmit recovery``.

The path opens SSH sessions to the affected platforms when needed, but it
does not run the write-permission probe. ``autosubmit create`` is the case
worth flagging here: it normally appears to be a local operation, but if
there are stale rows in the history database it will open one SSH session
per affected platform.

Connections opened during a run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``autosubmit run`` starts, Autosubmit opens **two SSH sessions per
platform** for every platform of type ``slurm``, ``pjm``, ``pbs`` or
``ps``:

1. A **main session** used by the running workflow for submission, status
   queries and file transfers.
2. A **log-recovery session** owned by a per-platform background process,
   used to download job log files in parallel while the main loop carries
   on with other work.

The log-recovery session is only created during ``autosubmit run``. Other
commands (``setstatus``, ``recovery``, ``stop --cancel``, ``create``)
open at most the main session per platform.

For ``ecaccess`` platforms, no SSH session is opened at all. Every
interaction with ECMWF is a local ``ecaccess-*`` command against the
ECMWF gateway, so the connection cost is gateway traffic rather than
login-node SSH sessions.

For the ``local`` platform, no connection of any kind is opened.

During the main loop, Autosubmit does not open new connections.
Submission, status checks and file transfers all reuse the SSH session
that was opened at startup, and keepalive packets are sent on the
existing channel to keep it open.

Reconnection on error
~~~~~~~~~~~~~~~~~~~~~

If a connection is lost mid-run, Autosubmit closes the existing session
before opening a replacement: the SFTP channel, the SSH transport and the
SSH client itself are all closed, and then a fresh session is opened. This
applies both to the main session and to the per-platform log-recovery
process. Sessions are not silently reused after a failure, and they are
not stacked.

At the end of a successful run, Autosubmit explicitly closes every
session it opened, and waits for each log-recovery process to finish
before exiting.

.. warning::
    The close operations are best-effort. If the underlying transport is
    already broken, the close will fail silently and the next connection
    will be opened anyway. This prevents a stuck close from blocking
    recovery, but it means operators who want to confirm that no SSH
    sessions have leaked should spot-check the login node directly (for
    example with ``who`` or ``ss``) rather than rely on Autosubmit's
    logs alone.

The reconnection behaviour described above applies after the workflow has
started running. If the very first connection attempt at the start of
``autosubmit run`` fails — for example because the HPC login node is
briefly unreachable — Autosubmit makes two internal retries within a
single connection attempt, but does not currently retry the whole
``autosubmit run`` startup. If both internal retries fail, the run
aborts. Adding a workflow-level startup retry is tracked separately.

Filesystem operations during a run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Beyond handling write-permission probes and SSH sessions, Autosubmit performs several file operations against the remote ``<SCRATCH_DIR>`` during each run. The exact number of these operations can vary based on the workflow's complexity, including factors such as the number of jobs, retries, wrapper usage, and recovery events. The table below outlines the types of operations performed at different stages, with descriptions provided in qualitative terms to reflect this variability.

.. list-table::
   :header-rows: 1
   :widths: 32 68

   * - Operation
     - When it happens
   * - Upload of ``.cmd`` job scripts
     - One upload per job script per submission cycle. A submission cycle happens for every main loop iteration that has new ``READY`` jobs to dispatch.
   * - Creation of the remote log directory
     - Once per platform at startup, and then once per upload as part of the upload itself. ``ecaccess`` platforms create one directory per path level because their ``mkdir`` does not have a recursive option.
   * - Listing of ``_COMPLETED`` marker files
     - Once per platform per status-check cycle, to detect newly completed jobs.
   * - Download of ``.out`` / ``.err`` log files
     - Two downloads per completed job (one for standard output, one for standard error). These run in the per-platform log-recovery process, in parallel with the main loop.
   * - Download of job ``STAT`` files
     - One download per job at the end of execution. Additional opportunistic downloads happen when the history database contains incomplete rows.
   * - Reads of remote ``STAT`` files
     - One batched read per status-check cycle, covering all relevant ``STAT`` files in a single round-trip.
   * - Cleanup of previous-run marker files
     - Once per platform at the start of ``autosubmit run``, scoped to the experiment's log directory.
   * - Compression of remote logs
     - One compression command per log file, only when ``COMPRESS_REMOTE_LOGS`` is enabled on the platform.
   * - Write-permission probe
     - See above.

A few patterns worth knowing when reasoning about filesystem load:

- **Submission and log retrieval run in parallel.** The main loop submits
  jobs and queries their status; a separate per-platform process downloads
  the logs of completed jobs. Activity from both runs concurrently against
  the same remote directory.
- **Status queries are batched.** A single scheduler query per platform per
  cycle covers all jobs in flight, regardless of how many there are. Reads
  of remote ``STAT`` files are similarly batched into a single round-trip.
- **``STAT`` files are written by the jobs, not by Autosubmit.** The remote
  job script appends a Unix epoch timestamp at start and finish; Autosubmit
  only reads these files.
- **Wrappers reduce per-job I/O.** A wrapped package submits one script for
  many jobs and produces consolidated log and ``STAT`` output, which cuts
  the per-job upload and download counts roughly in proportion to the
  number of jobs in the wrapper.

.. note::
    The table above describes which operations Autosubmit performs and
    when, but not absolute numbers (bytes transferred, IOPS, syscall
    counts) for a given workflow. Producing those numbers requires either
    instrumenting Autosubmit with an I/O tracer or running a representative
    workflow under ``strace`` / ``iostat`` against a controlled HPC.

How to generate a new experiment
--------------------------------

Now you can add jobs at the end of the file to see the execution
Each job will point to one of the ``Bash`` files that will be created in the next step, meaning that Autosubmit will
look for the instructions of the experiment in the ``~/autosubmit/<expid>/proj/local_project/`` if none is found
inside the folder Autosubmit will look at ``LOCAL.PROJECT_PATH`` set earlier in order to copy to the project folder
if they exist.


.. note::
    The files can also be R, python2, python3. By default it is bash and can be changed by setting the file type.

    .. code-block:: yaml

        JOBS:
            LOCAL_SETUP:
                TYPE: Python # adding this


.. code-block:: yaml

    JOBS:
        LOCAL_SETUP:
            FILE: LOCAL_SETUP.sh # ~/autosubmit/<expid>/proj/local_project/LOCAL_SETUP.sh
            PLATFORM: Local
            RUNNING: once

        SYNCHRONIZE:
            FILE: SYNCHRONIZE.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: LOCAL_SETUP
            RUNNING: once
            WALLCLOCK: 00:05

        REMOTE_SETUP:
            FILE: REMOTE_SETUP.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: SYNCHRONIZE
            WALLCLOCK: 00:05
            RUNNING: once

        INI:
            FILE: INI.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: REMOTE_SETUP
            RUNNING: once
            WALLCLOCK: 00:05

        DATA_NOTIFIER:
            FILE: DATA_NOTIFIER.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: INI
            RUNNING: chunk

        SIM:
            FILE: SIM.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: DATA_NOTIFIER
            RUNNING: chunk

        STATISTICS:
            FILE: STATISTICS.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: SIM
            RUNNING: chunk

        APP:
            FILE: APP.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: STATISTICS
            RUNNING: chunk

        CLEAN:
            FILE: CLEAN.sh
            # PLATFORM: MARENOSTRUM5
            DEPENDENCIES: APP SIM STATISTICS
            RUNNING: once
            WALLCLOCK: 00:05

Once you finish setting up all the new configurations, you can run the following command to generate the experiment
just created; we need to create a new folder to keep all the instructions for the experiment to be executed on the
platform.

``mkdir -p /home/user/experiment_example``

.. hint::
    The name of the folder can be anything as long as it matches the Local Parameter specified in the configuration
    file; the name change needs to take this into account

For the execution of this test, a few files will need to be created within the new folder;
these files will contain proj-associated code that will be executed on the job-specified platform.

.. code-block:: yaml

    LOCAL_SETUP.sh
    SYNCHRONIZE.sh
    REMOTE_SETUP.sh
    INI.sh
    DATA_NOTIFIER.sh
    SIM.sh
    STATISTICS.sh
    APP.sh
    CLEAN.sh

To keep a concise and clear example of how Autosubmit works, a simple instruction can be executed as a test.
So add the following the instruction below to one or more ``Bash`` files created in the previous steps.

.. code-block:: yaml

    sleep 5

How to run the experiment
-------------------------

``autosubmit create -f -v <EXPID>``

Once the experiment is generated, we can execute it and check the experiment by running the command below

    #. Submit the job to the specified platform
    #. monitor their status
    #. transfers logs to $expid/tmp/Log_$expid

``autosubmit run <EXPID>``

.. note::
    For more examples on how to create and share configurations of experiments and platforms,
    you can visit the :ref:`page <create_and_share_config>`.