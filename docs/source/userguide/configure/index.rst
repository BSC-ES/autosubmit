Configure Experiments
=====================

This page covers some of the basics for defining experiment parameters, as well as some references to the files where such information is stored. Locally, you can find them under ``autosubmit/expid/conf`` where ``expid`` is the experiment ID. See :doc:`../expids` for more information regarding experiment IDs.


Configuration files
-------------------

Experiment configuration files are stored under each experiment's configuration directory. You should adjust all parameters to your needs in this files before creating the experiment. 
The files follow the naming schema *type_expid.yml* where *type* is either **expdef**, **jobs**, **platforms** or **autosubmit**.

*expdef_<EXPID>.yml* contains:
    - Start dates, members and chunks (number and length).
    - Experiment project source: origin (version control system or path)
    - Project configuration file path.

*jobs_<EXPID>.yml* contains the workflow to be run:
    - Scripts to execute.
    - Dependencies between tasks.
    - Task requirements (processors, wallclock time...).
    - Platform to use.

For more information on adding jobs see :ref:`add-new-job` and :ref:`add-het-job`.

*platforms_<EXPID>.yml* contains:
    - HPC, fat-nodes and supporting computers configuration.

For more information on adding a new platform to the experiment configuration, see :ref:`add-new-plat-exp`.

.. note:: *platforms_<EXPID>.yml* is usually provided by technicians, users will only have to change login and accounting options for HPCs.

*autosubmit_<EXPID>.yml* contains:
    - Maximum number of jobs to be running at the same time at the HPC.
    - Time (seconds) between connections to the HPC queue scheduler to poll already submitted jobs status.
    - Number of retrials if a job fails.


Once all file parameters have been tuned, an experiment can be created. Refer to the method page :meth:`autosubmit.autosubmit.Autosubmit.create` for syntax details.
In sumamry, ``autosubmit create`` uses the ``expdef_<EXPID>.yml`` file to generate the experiment and related workfow. The experiment workflow, which contains all the jobs and its dependencies, will be saved as a *pkl* file. More info on pickle can be found at http://docs.python.org/library/pickle.html.

In order to understand more the grouping options, which are used for visualization purposes, please check :ref:`grouping`.


.. _add-new-job:

How to add a new job
--------------------

To add a new job from a template file, open the ``jobs_<EXPID>.yml`` file and add this text:

.. code-block:: yaml

    new_job:
        FILE: <new_job_template>

This will create a new job named *new_job* that will be executed once at the default platform. This job will use the template located at ``<new_job_template>``. Note that path is relative to project folder.

This is the minimum job definition and usually is not enough. Typically, you usually will need to add some others parameters:


.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``FILE``
      - File where the job template is stored.
    * - ``PLATFORM``
      - Allows you to execute the job in a platform of your choice. It must be defined in the experiment's
        ``platforms_<EXPID>.yml`` file or to have the value ``LOCAL`` that always refers to the machine running Autosubmit.
    * - ``RUNNING``
      - Defines if jobs runs only once or once per start-date, member or chunk.
        Options are: ``once``, ``date``, ``member``, ``chunk``
    * - ``DEPENDENCIES``
      - Defines dependencies from job as a list of parents jobs separated by spaces.
        If *new_job* has to wait for *old_job* to finish, you must add the line ``DEPENDENCIES: old_job``.

For dependencies to jobs running in previous chunks, members or start-dates, use ``-(DISTANCE)``. For example, for a job *SIM* waiting for the previous *SIM* job to finish, you have to add ``DEPENDENCIES: SIM-1``.

For dependencies that are not mandatory for the normal workflow behaviour, you must add the char ``?`` at the end of the dependency.

For jobs running in HPC platforms, usually you have to provide information about processors, wallclock times and more. To do this, use:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``WALLCLOCK``
      - Wallclock time to be submitted to the HPC queue in format HH:MM.
    * - ``PROCESSORS``
      - Processors number to be submitted to the HPC. (Default: 1)
    * - ``THREADS``
      - Threads number to be submitted to the HPC. (Default: 1)
    * - ``TASKS``
      - Tasks number to be submitted to the HPC. (Default: 1)
    * - ``NODES``
      - Nodes number to be submitted to the HPC. (Default: directive is not added)
    * - ``HYPERTHREADING``
      - Enables Hyper-threading, this will double the max amount of threads. (Default: False)
        # Not available on slurm platforms
    * - ``QUEUE``
      - If given, Autosubmit will add jobs to the given queue instead of platform's default queue
    * - ``RETRIALS``
      - Number of retrials if a job fails. Defaults to the value given on experiment's autosubmit_<EXPID>.yml
    * - ``DELAY_RETRY_TIME``
      - Allows to put a delay between retries. Autosubmit will retry the job as soon as possible.
        Accepted formats are:

        #. plain number (specify a constant delay between retrials),

        #. plus (+) sign followed by a number (the delay will steadily increase by the addition of these number of seconds)

        #. multiplication (*) sign follows by a number (the delay after n retries will be the number multiplied by 10*n).

        Having this in mind, the ideal scenario is to use +(number) or plain(number) in case that the HPC has little
        issues or the experiment will run for a little time. Otherwise, is better to use the \*(number) approach.


.. code-block:: yaml

    #DELAY_RETRY_TIME: 11
    #DELAY_RETRY_TIME: +11 # will wait 11 + number specified
    #DELAY_RETRY_TIME:*11 # will wait 11,110,1110,11110...* by 10 to prevent a too big number


There are also other, less used features that you can use:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``FREQUENCY``
      - A job has only to be run after X dates, members or chunk. A job will always be created for the last one.
        (Default: 1)
    * - ``SYNCHRONIZE``
      - A job with ``RUNNING`` chunk, has to synchronize its dependencies chunks at a 'date' or
        'member' level, which means that the jobs will be unified: one per chunk for all members or dates.
        If not specified, the synchronization is for each chunk of all the experiment.
    * - ``RERUN_ONLY``
      - Determines if a job is only to be executed in reruns. (Default: False)
    * - ``CUSTOM_DIRECTIVES``
      - Custom directives for the HPC resource manager headers of the platform used for that job.
    * - ``SKIPPABLE``
      - In the case of a higher chunk or member ``READY``, ``RUNNING``, ``QUEUING``, or ``COMPLETED``
        The job will be able to be skipped ready.
    * - ``EXPORT``
      - Allows to run an env script or load some modules before running this job.
    * - ``EXECUTABLE``
      - Allows to wrap a job for be launched with a set of env variables.
    * - ``EXTENDED_HEADER_PATH``
      - Autosubmit allows users to customize the header and the tailer by pointing towards the relative path to the
        project folder where the header is located.
    * - ``EXTENDED_TAILER_PATH``
      - Autosubmit allows users to customize the header and the tailer by pointing towards the relative path to the
        project folder where the tailer is located.

.. _add-het-job:

How to add a new heterogeneous job
----------------------------------

.. important::
    This feature is only available for SLURM platforms. It is automatically enabled when the processors or nodes parameter is a yaml list

An heterogeneous job or hetjob is a job for whcih each component has virtually all job options available including partition, account and QOS (Quality Of Service). For example, part of a job might require four cores and 4 GB for each of 128 tasks while another part of the job would require 16 GB of memory and one CPU.



To add a new hetjob, open the ``jobs_<EXPID>.yml``.

.. code-block:: yaml

    JOBS:
        new_hetjob:
            FILE: <new_job_template>
            PROCESSORS: # Determines the amount of components that will be created
                - 4
                - 1
            MEMORY: # Determines the amount of memory that will be used by each component
                - 4096
                - 16384
            WALLCLOCK: 00:30
            PLATFORM: <platform_name> # Determines the platform where the job will be executed
            PARTITION: # Determines the partition where the job will be executed
                - <partition_name>
                - <partition_name>
            TASKS: 128 # Determines the amount of tasks that will be used by each component

This will create a new job named *new_hetjob* with two components that will be executed once.

How to configure email notifications
------------------------------------

**1.** Enable email notifications and set the accounts where you will receive it. For this, edit ``autosubmit_<EXPID>.yml``. More than one address can be defined.

Example:

.. code-block:: yaml

    mail:
        # Enable mail notifications for remote_failures
        # Default:True
        NOTIFY_ON_REMOTE_FAIL: True
        # Enable mail notifications
        # Default: False
        NOTIFICATIONS: True
        # Mail address where notifications will be received
        TO:
            - jsmith@example.com
            - rlewis@example.com


**2.** Define for which jobs you want to be notified. Edit ``jobs_<EXPID>.yml``.  You will be notified every time the job changes its status to one of the statuses defined on the parameter ``NOTIFY_ON``. You can define more than one job status separated by a whitespace, a comma (`,`), or using a list.

Example:

.. code-block:: yaml

    JOBS:
        LOCAL_SETUP:
            FILE: LOCAL_SETUP.sh
            PLATFORM: LOCAL
            NOTIFY_ON: FAILED COMPLETED
        EXAMPLE_JOB:
            FILE: EXAMPLE_JOB.sh
            PLATFORM: LOCAL
            NOTIFY_ON: FAILED, COMPLETED
        EXAMPLE_JOB_2:
            FILE: EXAMPLE_JOB_2.sh
            PLATFORM: LOCAL
            NOTIFY_ON:
                - FAILED
                - COMPLETED

.. _add-new-plat-exp:

How to add a new platform to the experiment configuration
---------------------------------------------------------

.. hint::
    If you are interested in changing the communications library, go to :ref:`request-exclusivity-reservation`.

To add a new platform, open the ``platforms_<EXPID>.yml`` file and add:

.. code-block:: yaml

    PLATFORMS:
        new_platform:
            # MANDATORY
            TYPE: <platform_type>
            HOST: <host_name>
            PROJECT: <project>
            USER: <user>
            SCRATCH: <scratch_dir>
            MAX_WALLCLOCK: <HH:MM>
            QUEUE: <hpc_queue>
            # OPTIONAL
            ADD_PROJECT_TO_HOST: False
            MAX_PROCESSORS: <N>
            EC_QUEUE : <ec_queue> # only when type == ecaccess
            VERSION: <version>
            2FA: False
            2FA_TIMEOUT: <timeout> # default 300
            2FA_METHOD: <method>
            SERIAL_PLATFORM: <platform_name>
            SERIAL_QUEUE: <queue_name>
            BUDGET: <budget>
            TEST_SUITE: False
            MAX_WAITING_JOBS: <N>
            TOTAL_JOBS: <N>
            CUSTOM_DIRECTIVES: "[ 'my_directive' ]"


This will create a platform named *new_platform*. The options specified are all required:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``TYPE``
      - Queue type for the platform. Options supported are PS, ecaccess and SLURM.
    * - ``HOST``
      - Hostname of the platform.
    * - ``PROJECT``
      - Project for the machine scheduler.
    * - ``USER``
      - User for the machine scheduler.
    * - ``SCRATCH_DIR``
      - Path to the scratch directory of the machine.
    * - ``MAX_WALLCLOCK``
      - Maximum wallclock time allowed for a job in the platform.
    * - ``MAX_PROCESSORS``
      - Maximum number of processors allowed for a job in the platform.
    * - ``EC_QUEUE``
      - Queue for the ecaccess platform. (hpc, ecs).

.. warning:: With some platform types, Autosubmit may also need the version, forcing you to add the parameter
    VERSION. For example, ecaccess (options: pbs, loadleveler, slurm).

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``VERSION``
      - Determines de version of the platform type.

.. warning:: With some platforms, 2FA authentication is required. If this is the case, you have to add the parameter
    2FA. These platforms are ecaccess (options: True, False). There may be some autosubmit functions that are not available when using an interactive auth method.

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``2FA``
      - Determines if the platform requires 2FA authentication. (Default: ``False``)
    * - ``2FA_TIMEOUT``
      - Determines the timeout for the 2FA authentication. (Default: ``300``)
    * - ``2FA_METHOD``
      - Determines the method for the 2FA authentication. (Default: ``token``)

Some platforms may require to run serial jobs in a different queue or platform. To avoid changing the job
configuration, you can specify what platform or queue to use to run serial jobs assigned to this platform:

* ``SERIAL_PLATFORM``: if specified, Autosubmit will run jobs with only one processor in the specified platform.

* ``SERIAL_QUEUE``: if specified, Autosubmit will run jobs with only one processor in the specified queue. Autosubmit
  will ignore this configuration if ``SERIAL_PLATFORM`` is provided

There are some other parameters that you may need to specify:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Parameter
      - Description
    * - ``BUDGET``
      - Budget account for the machine scheduler. If omitted, takes the value defined in ``PROJECT``
    * - ``ADD_PROJECT_TO_HOST``
      - Option to add project name to host. This is required for some HPCs
    * - ``TEST_SUITE``
      - If true, autosubmit test command can use this queue as a main queue. (Default: ``False``)
    * - ``MAX_WAITING_JOBS``
      - Maximum number of jobs to be waiting in this platform.
    * - ``TOTAL_JOBS``
      - Maximum number of jobs to be running at the same time in this platform.
    * - ``LOG_RECOVERY_QUEUE_SIZE``
      - A memory-consumption optimization for the recovery of logs.
         Default: ``max(100,TOTAL_JOBS) * 2``, in case of issues with the recovery of logs, you can increase this value.

.. _request-exclusivity-reservation:

How to request exclusivity or reservation
-----------------------------------------

.. important::
    Until now, it is only available for Marenostrum.

To request exclusivity or reservation for your jobs, you can configure two platform variables. Edit ``platforms_<EXPID>.yml``.


.. hint::
    To define some jobs with exclusivity/reservation and some others without it, you can define
    twice a platform, one with this parameters and another one without it.

Example:

.. code-block:: yaml

    PLATFORMS:
        marenostrum5:
            TYPE: slurm
            HOST: mn-bsc32
            PROJECT: bsc32
            ADD_PROJECT_TO_HOST: false
            USER: bsc032XXX
            SCRATCH_DIR: /gpfs/scratch

Of course, you can configure only one or both. For example, for reservation it would be:

Example:

.. code-block:: YAML

    PLATFORMS:
        marenostrum5:
            TYPE: slurm
            ...
            RESERVATION: your-reservation-id


How to set a custom interpreter for your job
--------------------------------------------

If the remote platform does not implement the interpreter you need, you can customize the ``shebang`` of your job script so it points to the relative path of the interpreter you want.

In the file ``jos_<EXPID>.yml``:


.. list-table:: Parameters Description
   :widths: 25 60 15
   :header-rows: 1

   * - Parameters
     - Description
     - Exemple
   * - ``JOBNAME``
     - Job Name
     -
   * - ``FILE``
     - Script to execute. If not specified, job will be omitted from workflow.
       You can also specify additional files separated by a ",".
       Note: The post processed additional_files will be sent to %HPCROOT%/LOG_%EXPID%Path relative to the project
       directory
     -
   * - ``DATA_DEPENDENCIES``
     - Job in which this will be dependent and waiting for the results to start performing.
     -
   * - ``WAIT``
     - Default: True
     - False
   * - ``WCHUNKINC`` (Wallclock chunk increase)
     - Processors number to be submitted to the HPC. (Default: 1)
       WALLCLOCK will be increased according to the formula (WALLCLOCK + WCHUNKINC * (chunk - 1)).
       Ideal for sequences of jobs that change their expected running time according to the current chunk.
     - 00:01
   * - ``PROCESSORS``
     - Number of processors to be used in the Job
     - 1
   * - ``MEMORY``
     - Memory requirements for the job in MB
     - 4096
   * - ``CHECK``
     - Some jobs can not be checked before running previous jobs. Set this option to false if that is the case
     - False
   * - ``TYPE``
     - Select the interpreter that will run the job. Options: bash, python, r. (Default: bash)
     - bash
   * - ``EXECUTABLE``
     - Specify the path to the interpreter. If empty, use system default based on job type. (Default: empty)
     - /my_python_env/python3
   * - Splits
     - Split the job in N jobs. (Default: None)
     - 2
   * - ``SPLITSIZEUNIT``
     - Size unit of the split. Options: hour, day, month, year. (Default: EXPERIMENT.CHUNKSIZEUNIT-1)
     - day
   * - ``SPLITSIZE``
     - Size of the split. (Default: 1)
     - 1


You can give a path to the ``EXECUTABLE`` setting of your job. Autosubmit will replace the ``shebang`` with the path you provided.

Example:

.. code-block:: yaml

    JOBS:
        POST:
            FILE:  POST.sh
            DEPENDENCIES:  SIM
            RUNNING:  chunk
            WALLCLOCK:  00:05
            EXECUTABLE:  /my_python_env/python3

This job will use the python interpreter located in the relative path ``/my_python_env/python3/``

It is also possible to use variables in the ``EXECUTABLE`` path.

Example:

.. code-block:: yaml

    JOBS:
        POST:
            FILE: POST.sh
            DEPENDENCIES: SIM
            RUNNING: chunk
            WALLCLOCK: 00:05
            EXECUTABLE: "%PROJDIR%/my_python_env/python3"

The result is a ``shebang`` line ``#!/esarchive/autosubmit/my_python_env/python3``.

How to create and run only selected members
-------------------------------------------

Your experiment is defined and correctly configured, but you want to create it only considering some selected members, and also to avoid creating the whole experiment to run only the members you want. Then, you can do it by configuring the setting ``RUN_ONLY_MEMBERS`` in the ``expdef_<EXPID>.yml`` file:

.. code-block:: yaml

    DEFAULT:
        # Experiment identifier
        # No need to change
        EXPID: cxxx
        # HPC name.
        # No need to change
        HPCARCH: ithaca

    experiment:
        # Supply the list of start dates. Available formats: YYYYMMDD YYYYMMDDhh YYYYMMDDhhmm
        # Also you can use an abbreviated syntax for multiple dates with common parts:
        # 200001[01 15] <=> 20000101 20000115
        # DATELIST: 19600101 19650101 19700101
        # DATELIST: 1960[0101 0201 0301]
        DATELIST: 19900101
        # Supply the list of members. LIST: fc0 fc1 fc2 fc3 fc4
        MEMBERS: fc0
        # Chunk size unit. STRING: hour, day, month, year
        CHUNKSIZEUNIT: month
        # Chunk size. NUMERIC: 4, 6, 12
        CHUNKSIZE: 1
        # Total number of chunks in experiment. NUMERIC: 30, 15, 10
        NUMCHUNKS: 2
        # Calendar used. LIST: standard, noleap
        CALENDAR: standard
        # List of members that can be included in this run. Optional.
        # RUN_ONLY_MEMBERS: fc0 fc1 fc2 fc3 fc4
        # RUN_ONLY_MEMBERS: fc[0-4]
        RUN_ONLY_MEMBERS:


You can set the ``RUN_ONLY_MEMBERS`` value as shown in the format examples above it. Then, ``Job List`` generation is performed as usual. However, an extra step is performed that will filter the jobs according to ``RUN_ONLY_MEMBERS``. It discards jobs belonging to members not considered in the value provided, and also we discard these jobs from the dependency tree (parents and children). The filtered ``Job List`` is returned.

The necessary changes have been implemented in the API so you can correctly visualize experiments implementing this new setting in **Autosubmit GUI**.

.. important::
    Wrappers are correctly formed considering the resulting jobs.

Remote Dependencies - Presubmission feature
-------------------------------------------

There is also the possibility of setting the option ``PRESUBMISSION`` to True in the config directive. This allows more
than one package containing simple or wrapped jobs to be submitted at the same time, even when the dependencies between
jobs aren't yet satisfied.

This is only useful for cases when the job scheduler considers the time a job has been queuing to determine the job's
priority (and the scheduler understands the dependencies set between the submitted packages). New packages can be
created as long as the total number of jobs are below than the number defined in the ``TOTALJOBS`` variable.

The jobs that are waiting in the remote platform, will be marked as ``HOLD``.
