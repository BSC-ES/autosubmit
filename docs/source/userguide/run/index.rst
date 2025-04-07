Running Experiments
===================

Run an experiment
-------------------

Launch Autosubmit with the command:

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID>

In the previous command output ``<EXPID>`` is the experiment identifier. The command
exits with ``0`` when the workflow finishes with no failed jobs, and with ``1``
otherwise.

Options:

.. runcmd:: autosubmit run -h


Example:

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID>

.. important:: If the autosubmit version is set on ``autosubmit_<EXPID>.yml`` it must match the actual autosubmit version
.. hint:: It is recommended to launch it in background and with ``nohup`` (continue running although the user who launched the process logs out).

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    nohup autosubmit run <EXPID> &

.. important:: Before launching Autosubmit check password-less ssh is feasible (*HPCName* is the hostname):
.. important:: Add encryption key to ssh agent for each session (if your ssh key is encrypted)

.. important:: The host machine has to be able to access HPC's/Clusters via password-less ssh. Make sure that the ssh key is in PEM format `ssh-keygen -t rsa -b 4096 -C "email@email.com" -m PEM`.

    ``ssh HPCName``

More info on password-less ssh can be found at: http://www.linuxproblem.org/art_9.html

.. caution:: After launching Autosubmit, one must be aware of login expiry limit and policy (if applicable for any HPC) and renew the login access accordingly (by using token/key etc) before expiry.

How to run an experiment that was created with another version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. important:: First of all you have to stop your Autosubmit instance related with the experiment

Once you've already loaded / installed the Autosubmit version do you want:

.. code-block:: bash

    autosubmit create <EXPID> -np
    autosubmit recovery <EXPID> -s --all -f -np
    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -v
    or
    autosubmit updateversion <EXPID>
    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -v

*EXPID* is the experiment identifier.
The most common problem when you change your Autosubmit version is the apparition of several Python errors.
This is due to how Autosubmit saves internally the data, which can be incompatible between versions.
The steps above represent the process to re-create (1) these internal data structures and to recover (2) the previous status of your experiment.

How to run an experiment that was created with version <= 4.0.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. important:: First of all you have to stop your Autosubmit instance related with the experiment.

Once you've already loaded / installed the Autosubmit version do you want:

.. code-block:: bash

    autosubmit upgrade <EXPID>
    autosubmit create <EXPID> -np
    autosubmit recovery <EXPID> -s --all -f -np
    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -v
    or
    autosubmit updateversion <EXPID>
    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -v

*<EXPID>* is the experiment identifier.
The most common problem when you upgrade an experiment with INI configuration to YAML is that some variables may be not automatically translated.
Ensure that all your <EXPID>/conf/\*.yml files are correct and also revise the templates in <EXPID>/proj/$proj_name.


How to run only selected members
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run only a subset of selected members you can execute the command:

    .. code-block:: bash

        # Add your key to ssh agent ( if encrypted )
        ssh-add ~/.ssh/id_rsa
        autosubmit run <EXPID> -rom MEMBERS

*<EXPID>* is the experiment identifier, the experiment you want to run.

*MEMBERS* is the selected subset of members. Format `"member1 member2 member2"`, example: `"fc0 fc1 fc2"`.

Then, your experiment will start running jobs belonging to those members only. If the experiment was previously running and autosubmit was stopped when some jobs belonging to other members (not the ones from your input) where running, those jobs will be tracked and finished in the new exclusive run.

Furthermore, if you wish to run a sequence of only members execution; then, instead of running `autosubmit run -rom "member_1"` ... `autosubmit run -rom "member_n"`, you can make a bash file with that sequence and run the bash file. Example:

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -rom MEMBER_1
    autosubmit run <EXPID> -rom MEMBER_2
    autosubmit run <EXPID> -rom MEMBER_3
    ...
    autosubmit run <EXPID> -rom MEMBER_N

How to start an experiment at a given time
------------------------------------------

To start an experiment at a given time, use the command:

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -st INPUT

*<EXPID>* is the experiment identifier

*INPUT* is the time when your experiment will start. You can provide two formats:
  * `H:M:S`: For example `15:30:00` will start your experiment at 15:30 in the afternoon of the present day.
  * `yyyy-mm-dd H:M:S`: For example `2021-02-15 15:30:00` will start your experiment at 15:30 in the afternoon on February 15th.

Then, your terminal will show a countdown for your experiment start.

This functionality can be used together with other options supplied by the `run` command.

The `-st` command has a long version `--start_time`.


How to start an experiment after another experiment is finished
---------------------------------------------------------------

To start an experiment after another experiment is finished, use the command:

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    autosubmit run <EXPID> -sa <EXPIDB>

*<EXPID>* is the experiment identifier, the experiment you want to start.

*<EXPIDB>* is the experiment identifier of the experiment you are waiting for before your experiment starts.

.. warning:: Both experiments must be using Autosubmit version `3.13.0` or later.

Then, your terminal will show the current status of the experiment you are waiting for. The status format is `COMPLETED/QUEUING/RUNNING/SUSPENDED/FAILED`.

This functionality can be used together with other options supplied by the `run` command.

The `-sa` command has a long version `--start_after`.

.. _run_profiling:

How to profile Autosubmit while running an experiment
-----------------------------------------------------

Autosubmit offers the possibility to profile an experiment execution. To enable the profiler, just
add the ``--profile`` (or ``-p``) flag to your ``autosubmit run`` command, as in the following example:

.. code-block:: bash

    autosubmit run --profile <EXPID>

.. include:: ../../_include/profiler_common.rst

.. _run_modes:

How to prepare an experiment to run in two independent job_list. (Priority jobs, Two-step-run) (OLD METHOD)
-----------------------------------------------------------------------------------------------------------

This feature allows to run an experiment in two separated steps without the need of do anything manually.

To achieve this, you will have to use an special parameter called TWO_STEP_START in which you will put the list of the jobs that you want to run in an exclusive mode. These jobs will run until all of them finishes and once it finishes, the rest of the jobs will begun the execution.

It can be activated through TWO_STEP_START and it is set on expdef_<EXPID>.yml, under the experiment: section.

.. code-block:: ini

    experiment:
        DATELIST: 20120101 20120201
        MEMBERS: fc00[0-3]
        CHUNKSIZEUNIT: day
        CHUNKSIZE: 1
        NUMCHUNKS: 10
        CHUNKINI :
        CALENDAR: standard
        # To run before the rest of experiment:
        TWO_STEP_START: <job_names&section,dates,member_or_chunk(M/C),chunk_or_member(C/M)>

In order to be easier to use, there are Three  modes for use this feature: job_names and section,dates,member_or_chunk(M/C),chunk_or_member(C/M).

* By using job_names alone, you will need to put all jobs names one by one divided by the char , .
* By using section,dates,member_or_chunk(M/C),chunk_or_member(C/M). You will be able to select multiple jobs at once combining these filters.
* Use both options, job_names and section,dates,member_or_chunk(M/C),chunk_or_member(C/M). You will have to put & between the two modes.

There are 5 fields on TWO_STEP_START, all of them are optional but there are certain limitations:

* **Job_name**: [Independent] List of job names, separated by ',' char. Optional, doesn't depend on any field. Separated from the rest of fields by '&' must be the first field if specified
* **Section**:  [Independent] List of sections, separated by  ',' char. Optional, can be used alone. Separated from the rest of fields by ';'
* **Dates**: [Depends on section] List of dates, separated by ',' char. Optional, but depends on Section field. Separated from the rest of fields by ';'
* **member_or_chunk**: [Depends on Dates(OR)]  List of chunk or member, must start with C or M to indicate the filter type. Jobs are selected by [1,2,3..] or by a range [0-9] Optional, but depends on Dates field. Separated from the rest of fields by ';'
* **chunk_or_member**: [Depends on Dates(OR)]  List of member or chunk, must start with M or C to indicate the filter type. Jobs are selected by [1,2,3..] or by a range [0-9] Optional, but depends on Dates field. Separated from the rest of fields by ';'

Example using the old method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Guess the expdef configuration as follow:

.. code-block:: yaml

    experiment:
        DATELIST: 20120101
        MEMBERS: 00[0-1]
        CHUNKSIZEUNIT: day
        CHUNKSIZE: 1
        NUMCHUNKS: 2
        TWO_STEP_START: a02n_20120101_000_1_REDUCE&COMPILE_DA,SIM;20120101;c[1]

Given this job_list ( jobs_conf has REMOTE_COMPILE(once),DA,SIM,REDUCE)

['a02n_REMOTE_COMPILE', 'a02n_20120101_000_1_SIM', 'a02n_20120101_000_2_SIM', 'a02n_20120101_001_1_SIM', 'a02n_20120101_001_2_SIM', 'a02n_COMPILE_DA', 'a02n_20120101_1_DA', 'a02n_20120101_2_DA', 'a02n_20120101_000_1_REDUCE', 'a02n_20120101_000_2_REDUCE', 'a02n_20120101_001_1_REDUCE', 'a02n_20120101_001_2_REDUCE']

The priority jobs will be ( check TWO_STEP_START from expdef conf):

['a02n_20120101_000_1_SIM', 'a02n_20120101_001_1_SIM', 'a02n_COMPILE_DA', 'a02n_20120101_000_1_REDUCE']

How to prepare an experiment to run in two independent job_list. (New method)
-----------------------------------------------------------------------------

From AS4, TWO_STEP_START is not longer needed since the users can now specify exactly which tasks of a job are needed to run the current task in the DEPENDENCIES parameter.


Simplified example using the new method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example is based on the previous one, but using the new method and without the reduce job.

.. code-block:: yaml

    experiment:
        DATELIST: 20120101
        MEMBERS: "00[0-1]"
        CHUNKSIZEUNIT: day
        CHUNKSIZE: 1
        NUMCHUNKS: 2
    JOBS:
        REMOTE_COMPILE:
            FILE: remote_compile.sh
            RUNNING: once
        DA:
            FILE: da.sh
            DEPENDENCIES:
                SIM:
                DA:
                    DATES_FROM:
                     "20120201":
                       CHUNKS_FROM:
                        1:
                         DATES_TO: "20120101"
                         CHUNKS_TO: "1"
        SIM:
            FILE: sim.sh
            DEPENDENCIES:
                LOCAL_SEND_STATIC:
                REMOTE_COMPILE:
                SIM-1:
                DA-1:

Example 2: Crossdate wrappers using the the new dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

    experiment:
      DATELIST: 20120101 20120201
      MEMBERS: "000 001"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '3'
    wrappers:
        wrapper_simda:
            TYPE: "horizontal-vertical"
            JOBS_IN_WRAPPER: "SIM DA"

    JOBS:
      LOCAL_SETUP:
        FILE: templates/local_setup.sh
        PLATFORM: marenostrum_archive
        RUNNING: once
        NOTIFY_ON: COMPLETED
      LOCAL_SEND_SOURCE:
        FILE: templates/01_local_send_source.sh
        PLATFORM: marenostrum_archive
        DEPENDENCIES: LOCAL_SETUP
        RUNNING: once
        NOTIFY_ON: FAILED
      LOCAL_SEND_STATIC:
        FILE: templates/01b_local_send_static.sh
        PLATFORM: marenostrum_archive
        DEPENDENCIES: LOCAL_SETUP
        RUNNING: once
        NOTIFY_ON: FAILED
      REMOTE_COMPILE:
        FILE: templates/02_compile.sh
        DEPENDENCIES: LOCAL_SEND_SOURCE
        RUNNING: once
        PROCESSORS: '4'
        WALLCLOCK: 00:50
        NOTIFY_ON: COMPLETED
      SIM:
        FILE: templates/05b_sim.sh
        DEPENDENCIES:
          LOCAL_SEND_STATIC:
          REMOTE_COMPILE:
          SIM-1:
          DA-1:
        RUNNING: chunk
        PROCESSORS: '68'
        WALLCLOCK: 00:12
        NOTIFY_ON: FAILED
      LOCAL_SEND_INITIAL_DA:
        FILE: templates/00b_local_send_initial_DA.sh
        PLATFORM: marenostrum_archive
        DEPENDENCIES: LOCAL_SETUP LOCAL_SEND_INITIAL_DA-1
        RUNNING: chunk
        SYNCHRONIZE: member
        DELAY: '0'
      COMPILE_DA:
        FILE: templates/02b_compile_da.sh
        DEPENDENCIES: LOCAL_SEND_SOURCE
        RUNNING: once
        WALLCLOCK: 00:20
        NOTIFY_ON: FAILED
      DA:
        FILE: templates/05c_da.sh
        DEPENDENCIES:
          SIM:
          LOCAL_SEND_INITIAL_DA:
            CHUNKS_TO: "all"
            DATES_TO: "all"
            MEMBERS_TO: "all"
          COMPILE_DA:
          DA:
            DATES_FROM:
             "20120201":
               CHUNKS_FROM:
                1:
                 DATES_TO: "20120101"
                 CHUNKS_TO: "1"
        RUNNING: chunk
        SYNCHRONIZE: member
        DELAY: '0'
        WALLCLOCK: 00:12
        PROCESSORS: '256'
        NOTIFY_ON: FAILED

.. figure:: fig/monarch-da.png
   :name: crossdate-example
   :align: center
   :alt: crossdate-example



Finally, you can launch Autosubmit *run* in background and with ``nohup`` (continue running although the user who launched the process logs out).

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    nohup autosubmit run <EXPID> &

How to stop the experiment
--------------------------

From Autosubmit 4.1.6, you can stop an experiment using the command `autosubmit stop`

Options:

.. runcmd:: autosubmit stop -h

Examples:
~~~~~~~~~

.. code-block:: bash

    autosubmit stop <EXPID>
    autosubmit stop <EXPID>, <EXPID>
    autosubmit stop -a
    autosubmit stop -a -f
    autosubmit stop -a -c
    autosubmit stop -fa --cancel -fs "SUBMITTED, QUEUING, RUNNING" -t "FAILED"


You can stop Autosubmit by sending a signal to the process.
To get the process identifier (PID) you can use the ps command on a shell interpreter/terminal.
::

    ps -ef | grep autosubmit
    dbeltran  22835     1  1 May04 ?        00:45:35 autosubmit run <EXPID>
    dbeltran  25783     1  1 May04 ?        00:42:25 autosubmit run <EXPID>

To send a signal to a process you can use kill also on a terminal.

To stop immediately experiment <EXPID>:
::

    kill -9 22835

.. important:: In case you want to restart the experiment, you must follow the
    :ref:`workflow_recovery` procedure, explained below, in order to properly resynchronize all completed jobs.
