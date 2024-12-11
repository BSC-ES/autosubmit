Configure Experiments
=====================

How to configure experiments
----------------------------

Edit ``expdef_cxxx.yml``, ``jobs_cxxx.yml`` and ``platforms_cxxx.yml`` in the ``conf`` folder of the experiment.

*expdef_cxxx.yml* contains:
    - Start dates, members and chunks (number and length).
    - Experiment project source: origin (version control system or path)
    - Project configuration file path.

*jobs_cxxx.yml* contains the workflow to be run:
    - Scripts to execute.
    - Dependencies between tasks.
    - Task requirements (processors, wallclock time...).
    - Platform to use.

*platforms_cxxx.yml* contains:
    - HPC, fat-nodes and supporting computers configuration.

.. note:: *platforms_cxxx.yml* is usually provided by technicians, users will only have to change login and accounting options for HPCs.

You may want to configure Autosubmit parameters for the experiment. Just edit ``autosubmit_cxxx.yml``.

*autosubmit_cxxx.yml* contains:
    - Maximum number of jobs to be running at the same time at the HPC.
    - Time (seconds) between connections to the HPC queue scheduler to poll already submitted jobs status.
    - Number of retrials if a job fails.

Then, Autosubmit *create* command uses the ``expdef_cxxx.yml`` and generates the experiment:
After editing the files you can proceed to the experiment workflow creation.
Experiment workflow, which contains all the jobs and its dependencies, will be saved as a *pkl* file:
::

    autosubmit create EXPID

*EXPID* is the experiment identifier.

Options:
::

    usage: autosubmit create [-h] [-np] [--hide] [-d] [-o {pdf,png,ps,svg}] [-group_by {date,member,chunk,split,automatic}]
                         [-expand EXPAND] [-expand_status EXPAND_STATUS] [-nt] [-cw] [-v] [-p] [-f]
                         EXPID

    create specified experiment joblist

    positional arguments:
      EXPID                 experiment identifier

    options:
      -h, --help            show this help message and exit
      -np, --noplot         omit plot
      --hide                hides plot window
      -d, --detail          Show Job List view in terminal
      -o {pdf,png,ps,svg}, --output {pdf,png,ps,svg}
                            chooses type of output for generated plot
      -group_by {date,member,chunk,split,automatic}
                            Groups the jobs automatically or by date, member, chunk or split
      -expand EXPAND        Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". LIST = "[
                            19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"
      -expand_status EXPAND_STATUS
                            Select the statuses to be expanded
      -nt, --notransitive   Disable transitive reduction
      -cw, --check_wrapper  Generate possible wrapper in the current workflow
      -v, --update_version  Update experiment version
      -p, --profile         Prints performance parameters of the execution of this command.
      -f, --force           force regenerate job_list

Example:
::

    autosubmit create cxxx

In order to understand more the grouping options, which are used for visualization purposes, please check :ref:`grouping`.

More info on pickle can be found at http://docs.python.org/library/pickle.html

How to add a new job
--------------------

To add a new job, open the <experiments_directory>/cxxx/conf/jobs_cxxx.yml file where cxxx is the experiment
identifier and add this text:s

.. code-block:: yaml

    new_job:
        FILE: <new_job_template>

This will create a new job named "new_job" that will be executed once at the default platform. This job will use the
template located at <new_job_template> (path is relative to project folder).

This is the minimum job definition and usually is not enough. You usually will need to add some others parameters:

* PLATFORM: allows you to execute the job in a platform of your choice. It must be defined in the experiment's
  platforms.yml file or to have the value 'LOCAL' that always refer to the machine running Autosubmit

* RUNNING: defines if jobs runs only once or once per start-date, member or chunk. Options are: once, date,
  member, chunk

* DEPENDENCIES: defines dependencies from job as a list of parents jobs separated by spaces. For example, if
  'new_job' has to wait for "old_job" to finish, you must add the line "DEPENDENCIES: old_job".

    * For dependencies to jobs running in previous chunks, members or start-dates, use -(DISTANCE). For example, for a job "SIM" waiting for
      the previous "SIM" job to finish, you have to add "DEPENDENCIES: SIM-1".
    * For dependencies that are not mandatory for the normal workflow behaviour, you must add the char '?' at the end of the dependency.


For jobs running in HPC platforms, usually you have to provide information about processors, wallclock times and more.
To do this use:

* WALLCLOCK: wallclock time to be submitted to the HPC queue in format HH:MM

* PROCESSORS: processors number to be submitted to the HPC. If not specified, defaults to 1.

* THREADS:  threads number to be submitted to the HPC. If not specified, defaults to 1.

* TASKS:  tasks number to be submitted to the HPC. If not specified, defaults to 1.

* NODES:  nodes number to be submitted to the HPC. If not specified, the directive is not added.


* HYPERTHREADING: Enables Hyper-threading, this will double the max amount of threads. defaults to false. ( Not available on slurm platforms )
* QUEUE: queue to add the job to. If not specified, uses PLATFORM default.

* RETRIALS: Number of retrials if job fails

* DELAY_RETRY_TIME: Allows to put a delay between retries. Triggered when a job fails. If not specified, Autosubmit will retry the job as soon as possible. Accepted formats are: plain number (there will be a constant delay between retrials, of as many seconds as specified), plus (+) sign followed by a number (the delay will steadily increase by the addition of these number of seconds), or multiplication (*) sign follows by a number (the delay after n retries will be the number multiplied by 10*n). Having this in mind, the ideal scenario is to use +(number) or plain(number) in case that the HPC has little issues or the experiment will run for a little time. Otherwise, is better to use the \*(number) approach.

.. code-block:: yaml

    #DELAY_RETRY_TIME: 11
    #DELAY_RETRY_TIME: +11 # will wait 11 + number specified
    #DELAY_RETRY_TIME:*11 # will wait 11,110,1110,11110...* by 10 to prevent a too big number


There are also other, less used features that you can use:

* FREQUENCY: specifies that a job has only to be run after X dates, members or chunk. A job will always be created for
  the last one. If not specified, defaults to 1

* SYNCHRONIZE: specifies that a job with RUNNING: chunk, has to synchronize its dependencies chunks at a 'date' or
  'member' level, which means that the jobs will be unified: one per chunk for all members or dates.
  If not specified, the synchronization is for each chunk of all the experiment.

* RERUN_ONLY: determines if a job is only to be executed in reruns. If not specified, defaults to false.

* CUSTOM_DIRECTIVES: Custom directives for the HPC resource manager headers of the platform used for that job.

* SKIPPABLE: When this is true, the job will be able to skip it work if there is an higher chunk or member already ready, running, queuing or in complete status.

* EXPORT: Allows to run an env script or load some modules before running this job.

* EXECUTABLE: Allows to wrap a job for be launched with a set of env variables.

* QUEUE: queue to add the job to. If not specified, uses PLATFORM default.

* EXTENDED_HEADER_PATH: specify the path relative to the project folder where the extension to the autosubmit's header is

* EXTENDED_TAILER_PATH: specify the path relative to the project folder where the extension to the autosubmit's tailer is

How to add a new heterogeneous job (hetjob)
-------------------------------------------

A hetjob, is a job in which each component has virtually all job options available including partition, account and QOS (Quality Of Service).For example, part of a job might require four cores and 4 GB for each of 128 tasks while another part of the job would require 16 GB of memory and one CPU.

This feature is only available for SLURM platforms. And it is automatically enabled when the processors or nodes paramater is a yaml list

To add a new hetjob, open the <experiments_directory>/cxxx/conf/jobs_cxxx.yml file where cxxx is the experiment

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

This will create a new job named "new_hetjob" with two components that will be executed once.

* EXTENDED_HEADER_PATH: specify the path relative to the project folder where the extension to the autosubmit's header is

* EXTENDED_TAILER_PATH: specify the path relative to the project folder where the extension to the autosubmit's tailer is

How to configure email notifications
------------------------------------

To configure the email notifications, you have to follow two configuration steps:

1. First you have to enable email notifications and set the accounts where you will receive it.

Edit ``autosubmit_cxxx.yml`` in the ``conf`` folder of the experiment.

.. hint::
    Remember that you can define more than one email address divided by a whitespace.

Example:
::

    vi <experiments_directory>/cxxx/conf/autosubmit_cxxx.yml

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


2. Then you have to define for which jobs you want to be notified.

Edit ``jobs_cxxx.yml`` in the ``conf`` folder of the experiment.

.. hint::
    You will be notified every time the job changes its status to one of the statuses
    defined on the parameter ``NOTIFY_ON``

.. hint::
Remember that you can define more than one job status separated by a whitespace, a comma (`,`), or using a list.

Example:
::

    vi <experiments_directory>/cxxx/conf/jobs_cxxx.yml

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

How to add a new platform
-------------------------

.. hint::
    If you are interested in changing the communications library, go to the section below.

To add a new platform, open the <experiments_directory>/cxxx/conf/platforms_cxxx.yml file where cxxx is the experiment
identifier and add this text:

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


This will create a platform named "new_platform". The options specified are all mandatory:

* TYPE: queue type for the platform. Options supported are PBS, SGE, PS, ecaccess and SLURM.

* HOST: hostname of the platform

* PROJECT: project for the machine scheduler

* USER: user for the machine scheduler

* SCRATCH_DIR: path to the scratch directory of the machine

* MAX_WALLCLOCK: maximum wallclock time allowed for a job in the platform

* MAX_PROCESSORS: maximum number of processors allowed for a job in the platform

* EC_QUEUE: queue for the ecaccess platform. ( hpc, ecs )

.. warning:: With some platform types, Autosubmit may also need the version, forcing you to add the parameter
    VERSION. These platforms are PBS (options: 10, 11, 12) and ecaccess (options: pbs, loadleveler, slurm).

* VERSION: determines de version of the platform type

.. warning:: With some platforms, 2FA authentication is required. If this is the case, you have to add the parameter
    2FA. These platforms are ecaccess (options: True, False). There may be some autosubmit functions that are not avaliable when using an interactive auth method.

* 2FA: determines if the platform requires 2FA authentication. ( default: False)

* 2FA_TIMEOUT: determines the timeout for the 2FA authentication. ( default: 300 )

* 2FA_METHOD: determines the method for the 2FA authentication. ( default: token )

Some platforms may require to run serial jobs in a different queue or platform. To avoid changing the job
configuration, you can specify what platform or queue to use to run serial jobs assigned to this platform:

* SERIAL_PLATFORM: if specified, Autosubmit will run jobs with only one processor in the specified platform.

* SERIAL_QUEUE: if specified, Autosubmit will run jobs with only one processor in the specified queue. Autosubmit
  will ignore this configuration if SERIAL_PLATFORM is provided

There are some other parameters that you may need to specify:

* BUDGET: budget account for the machine scheduler. If omitted, takes the value defined in PROJECT

* ADD_PROJECT_TO_HOST: option to add project name to host. This is required for some HPCs

* QUEUE: if given, Autosubmit will add jobs to the given queue instead of platform's default queue

* TEST_SUITE: if true, autosubmit test command can use this queue as a main queue. Defaults to false

* MAX_WAITING_JOBS: maximum number of jobs to be waiting in this platform.

* TOTAL_JOBS: maximum number of jobs to be running at the same time in this platform.

* CUSTOM_DIRECTIVES: Custom directives for the resource manager of this platform.


How to request exclusivity or reservation
-----------------------------------------

To request exclusivity or reservation for your jobs, you can configure two platform variables:

Edit ``platforms_cxxx.yml`` in the ``conf`` folder of the experiment.

.. hint::
    Until now, it is only available for Marenostrum.

.. hint::
    To define some jobs with exclusivity/reservation and some others without it, you can define
    twice a platform, one with this parameters and another one without it.

Example:
::

    vi <experiments_directory>/cxxx/conf/platforms_cxxx.yml

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
::

    vi <experiments_directory>/cxxx/conf/platforms_cxxx.yml

.. code-block:: YAML

    PLATFORMS:
        marenostrum5:
            TYPE: slurm
            ...
            RESERVATION: your-reservation-id


How to set a custom interpreter for your job
--------------------------------------------

If the remote platform does not implement the interpreter you need, you can customize the ``shebang`` of your job script so it points to the relative path of the interpreter you want.

In the file:

::

    vi <experiments_directory>/cxxx/conf/jobs_cxxx.yml

.. code-block:: yaml

    JOBS:
        # Example job with all options specified

        ## Job name
        # JOBNAME:
        ## Script to execute. If not specified, job will be omitted from workflow. You can also specify additional files separated by a ",".
        # Note: The post processed additional_files will be sent to %HPCROOT%/LOG_%EXPID%
        ## Path relative to the project directory
        # FILE :
        ## Platform to execute the job. If not specified, defaults to HPCARCH in expdef file.
        ## LOCAL is always defined and refers to current machine
        # PLATFORM :
        ## Queue to add the job to. If not specified, uses PLATFORM default.
        # QUEUE :
        ## Defines dependencies from job as a list of parents jobs separated by spaces.
        ## Dependencies to jobs in previous chunk, member o startdate, use -(DISTANCE)
        # DEPENDENCIES:  INI SIM-1 CLEAN-2
        ## Define if jobs runs once, once per stardate, once per member or once per chunk. Options: once, date, member, chunk.
        ## If not specified, defaults to once
        # RUNNING:  once
        ## Specifies that job has only to be run after X dates, members or chunk. A job will always be created for the last
        ## If not specified, defaults to 1
        # FREQUENCY:  3
        ## On a job with FREQUENCY > 1, if True, the dependencies are evaluated against all
        ## jobs in the frequency interval, otherwise only evaluate dependencies against current
        ## iteration.
        ## If not specified, defaults to True
        # WAIT:  False
        ## Defines if job is only to be executed in reruns. If not specified, defaults to false.
        # RERUN_ONLY:  False
        ## Wallclock to be submitted to the HPC queue in format HH:MM
        # WALLCLOCK:  00:05
        ## Processors number to be submitted to the HPC. If not specified, defaults to 1.
        ## Wallclock chunk increase (WALLCLOCK will be increased according to the formula WALLCLOCK + WCHUNKINC * (chunk - 1)).
        ## Ideal for sequences of jobs that change their expected running time according to the current chunk.
        # WCHUNKINC:  00:01
        # PROCESSORS:  1
        ## Threads number to be submitted to the HPC. If not specified, defaults to 1.
        # THREADS:  1
        ## Tasks number to be submitted to the HPC. If not specified, defaults to 1.
        # Tasks:  1
        ## Enables hyper-threading. If not specified, defaults to false.
        # HYPERTHREADING:  false
        ## Memory requirements for the job in MB
        # MEMORY:  4096
        ##  Number of retrials if a job fails. If not specified, defaults to the value given on experiment's autosubmit.yml
        # RETRIALS:  4
        ##  Allows to put a delay between retries, of retrials if a job fails. If not specified, it will be static
        # The ideal is to use the +(number) approach or plain(number) in case that the hpc platform has little issues or the experiment will run for a short period of time
        # And *(10) in case that the filesystem is having large  delays or the experiment will run for a lot of time.
        # DELAY_RETRY_TIME:  11
        # DELAY_RETRY_TIME:  +11 # will wait 11 + number specified
        # DELAY_RETRY_TIME:  *11 # will wait 11,110,1110,11110...* by 10 to prevent a too big number
        ## Some jobs can not be checked before running previous jobs. Set this option to false if that is the case
        # CHECK:  False
        ## Select the interpreter that will run the job. Options: bash, python, r Default: bash
        # TYPE:  bash
        ## Specify the path to the interpreter. If empty, use system default based on job type  . Default: empty
        # EXECUTABLE:  /my_python_env/python3

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

Your experiment is defined and correctly configured, but you want to create it only considering some selected members, and also to avoid creating the whole experiment to run only the members you want. Then, you can do it by configuring the setting **RUN_ONLY_MEMBERS** in the file:

::

    vi <experiments_directory>/cxxx/conf/expdef_cxxx.yml

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



You can set the **RUN_ONLY_MEMBERS** value as shown in the format examples above it. Then, ``Job List`` generation is performed as usual. However, an extra step is performed that will filter the jobs according to **RUN_ONLY_MEMBERS**. It discards jobs belonging to members not considered in the value provided, and also we discard these jobs from the dependency tree (parents and children). The filtered ``Job List`` is returned.

The necessary changes have been implemented in the API so you can correctly visualize experiments implementing this new setting in **Autosubmit GUI**.

.. important::
    Wrappers are correctly formed considering the resulting jobs.

Remote Dependencies - Presubmission feature
-------------------------------------------

There is also the possibility of setting the option **PRESUBMISSION** to True in the config directive. This allows more
than one package containing simple or wrapped jobs to be submitted at the same time, even when the dependencies between
jobs aren't yet satisfied.

This is only useful for cases when the job scheduler considers the time a job has been queuing to determine the job's
priority (and the scheduler understands the dependencies set between the submitted packages). New packages can be
created as long as the total number of jobs are below than the number defined in the **TOTALJOBS** variable.

The jobs that are waiting in the remote platform, will be marked as HOLD.

How to configure
~~~~~~~~~~~~~~~~

In ``autosubmit_cxxx.yml``, regardless of the how your workflow is configured.

For example:

.. code-block:: yaml

    config:
        EXPID: ....
        AUTOSUBMIT_VERSION: 4.0.0
        ...
        MAXWAITINGJOBS: 100
        TOTALJOBS: 100
        ...
