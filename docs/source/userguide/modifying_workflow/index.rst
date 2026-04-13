.. _workflow_recovery:

How to restart the experiment
=============================

How to recover an experiment
----------------------------

We use the ``recovery`` command when an experiment was interrupted in an ungraceful way (e.g. scheduler failure, network failure, etc.) and Autosubmit job states are no loger consistent with the actual state of the jobs on the platform.

The ``recovery`` command checks which jobs are already finished and updates their status to ``COMPLETED``.

- By default, it checks for the completion files for active jobs (i.e. jobs in ``SUBMITTED``, ``QUEUING`` or ``RUNNING`` status).

- With the ``--all`` flag, it checks for the completion files for all jobs, regardless of their status.

- If a platform is unreachable, we can use the ``--offline`` flag to force the recovery without checking for completion files in remote. In this case, Autosubmit will only check for the completion files in the local file system. This option is useful when some platforms are not reachable (e.g. due to network failure) but we want to recover the experiment based on the completion files that are available locally.

.. warning:: Without the -s flag, Autosubmit will only perform a dry-run (i.e. it will not take effect) of the command.  


Typical workflow of recovery
----------------------------

1. Run a dry-run first and inspect the generated report. 
We will check for the completion files of all jobs in the experiment, so we will use the ``--all`` flag.
::

    autosubmit recovery <EXPID> --all

2. If needed, apply filters to limit which jobs are checked for completion files. 
For example, we can filter by job names, chunk/section/split, job statuses, or job types.
::
    
    # check for completion files of all jobs filtered by a space-separated list of job names
    autosubmit recovery <EXPID> --all -fl "<EXPID>_20101101_fc3_21_SIM <EXPID>_20111101_fc4_26_SIM"

    # check for completion files of all jobs filtered by chunk/section/split
    autosubmit recovery <EXPID> --all -fc "[20100101 [ fc0 [1] ] ]"

    # check for completion files of all jobs filtered by job statuses
    autosubmit recovery <EXPID> --all -fs "WAITING"

    # check for completion files of all jobs filtered by job types
    autosubmit recovery <EXPID> --all -ft "LOCALJOB, PSJOB"

    # check for completion files of all jobs filtered by multiple job types and status
    autosubmit recovery <EXPID> --all -ft "LOCALJOB, PSJOB" -fs "WAITING"

3. Apply changes with ``-s``
::

    autosubmit recovery <EXPID> --all -s

4. Resume the workflow with the ``run`` command.
::
    
    autosubmit run <EXPID>


Important options for recovery
------------------------------

.. list-table::
   :header-rows: 1

   * - Command
     - Explanation
   * - ``-s``, ``--save``
     - Apply and persist state changes. Without this option, nothing is saved.
   * - ``-f``, ``--force``
     - Cancel active remote jobs before resetting their state. Use when jobs are still running remotely but we want to reset their state in Autosubmit.
   * - ``--offline``
     - Complete recovery without remote platform checks, using local history instead.
   * - ``--all``
     - Check completion files for all jobs, not only active jobs.
   * - ``-fl``
     - Filter by job names. Example: ``-fl "job1 job2 job3"``.
   * - ``-fc``
     - Filter by chunk/section/split. Example: ``-fc "[20100101 [ fc0 [1] ] ]"``.
   * - ``-fs``
     - Filter by job statuses. Example: ``-fs "WAITING RUNNING"``.
   * - ``-ft``
     - Filter by job types. Example: ``-ft "LOCALJOB, PSJOB"`` or ``LOCALJOB[1,2]``.
   * - ``-np``, ``--noplot``
     - Do not generate plots during recovery (default).
   * - ``-plt``, ``--plot``
     - Generate plots during recovery.

.. warning::

   Keep in mind that the filter ``--all`` is applied before the other job filters.
   If ``--all`` is selected, the job filters will be applied to all jobs.
   If ``--all`` is not selected, the job filters will be applied only to active jobs
   (i.e. jobs in SUBMITTED, QUEUING or RUNNING status).

Examples:
----------------------------

::

    # Dry-run: check active jobs for completion files only (no changes saved)
    autosubmit recovery <EXPID>

    # Dry-run: check all jobs for completion files
    autosubmit recovery <EXPID> --all

    # Dry-run with filters over all jobs
    autosubmit recovery <EXPID> --all -fs "WAITING" -ft "LOCALJOB, PSJOB"

    # Apply recovery changes over all jobs
    autosubmit recovery <EXPID> --all -s

    # Apply recovery changes over all jobs, canceling remote jobs first
    autosubmit recovery <EXPID> --all -f -s

    # Apply recovery when some platforms are not reachable, using local history instead
    autosubmit recovery <EXPID> --offline -f --all -s

    # Resume the experiment after recovery
    autosubmit create <EXPID>
    autosubmit recovery <EXPID> --all -s [--offline] [-f]
    autosubmit run <EXPID>

Options:

.. runcmd:: autosubmit recovery -h

How to rerun a part of the experiment
-------------------------------------

This procedure allows you to create automatically a new pickle with a list of jobs of the experiment to rerun.

The ``create`` command will use the ``expdef_<EXPID>.yml`` file to generate the rerun if the variable RERUN is set to TRUE and a RERUN_JOBLIST is provided.

Additionally, you can have re-run only jobs that won't be included in the default job_list. In order to do that, you have to set RERUN_ONLY in the jobs conf of the corresponding job.

By default, ``create`` does **not** generate plots. To generate plots of the new job list, use the ``-plt`` or ``--plot`` option.

::

    autosubmit create <EXPID>

It will read the list of jobs specified in the RERUN_JOBLIST. To also generate a plot, use:

::

    autosubmit create <EXPID> -plt

Example:
::

    vi <experiments_directory>/<EXPID>/conf/expdef_<EXPID>.yml

.. code-block:: yaml

    ...

    rerun:
        RERUN: TRUE
        RERUN_JOBLIST: RERUN_TEST_INI;SIM[19600101[C:3]],RERUN_TEST_INI_chunks[19600101[C:3]]
    ...

    vi <experiments_directory>/<EXPID>/conf/jobs_<EXPID>.yml

.. code-block:: yaml

    PREPROCVAR:
        FILE: templates/04_preproc_var.sh
        RUNNING: chunk
        PROCESSORS: 8

    RERUN_TEST_INI_chunks:
        FILE: templates/05b_sim.sh
        RUNNING: chunk
        RERUN_ONLY: true

    RERUN_TEST_INI:
        FILE: templates/05b_sim.sh
        RUNNING: once
        RERUN_ONLY: true

    SIM:
        DEPENDENCIES: RERUN_TEST_INI RERUN_TEST_INI_chunks PREPROCVAR SIM-1
        RUNNING: chunk
        PROCESSORS: 10

    .. figure:: fig/rerun.png
       :name: rerun_result
       :align: center
       :alt: rerun_result

Run the command:

.. code-block:: bash

    # Add your key to ssh agent ( if encrypted )
    ssh-add ~/.ssh/id_rsa
    nohup autosubmit run <EXPID> &
