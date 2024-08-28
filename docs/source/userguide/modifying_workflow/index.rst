.. _workflow_recovery:

How to restart the experiment
=============================

This procedure allows you to restart an experiment. Autosubmit looks for the COMPLETED file for jobs that are considered active (SUBMITTED, QUEUING, RUNNING), UNKNOWN or READY.

.. warning:: You can only restart the experiment if there are not active jobs. You can use -f flag to cancel running jobs automatically.

You must execute:
::

    autosubmit recovery EXPID

*EXPID* is the experiment identifier.

Options:
::

    usage: autosubmit recovery [-h] [-np] [--all] [-s] [--hide] [-group_by {date,member,chunk,split,automatic}] [-expand EXPAND]
                           [-expand_status EXPAND_STATUS] [-nt] [-nl] [-d] [-f] [-v]
                           EXPID

    recover specified experiment

    positional arguments:
      EXPID                 experiment identifier

    options:
      -h, --help            show this help message and exit
      -np, --noplot         omit plot
      --all                 Get completed files to synchronize pkl
      -s, --save            Save changes to disk
      --hide                hides plot window
      -group_by {date,member,chunk,split,automatic}
                        Groups the jobs automatically or by date, member, chunk or split
      -expand EXPAND        Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". LIST = "[
                        19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"
      -expand_status EXPAND_STATUS
                            Select the statuses to be expanded
      -nt, --notransitive   Disable transitive reduction
      -nl, --no_recover_logs
                            Disable logs recovery
      -d, --detail          Show Job List view in terminal
      -f, --force           Cancel active jobs
      -v, --update_version  Update experiment version

Example:
::

    autosubmit recovery cxxx -s

In order to understand more the grouping options, which are used for visualization purposes, please check :ref:`grouping`.


.. hint:: When we are satisfied with the results we can use the parameter -s, which will save the change to the pkl file and rename the update file.

The --all flag is used to synchronize all jobs of our experiment locally with the information available on the remote platform
(i.e.: download the COMPLETED files we may not have). In case new files are found, the ``pkl`` will be updated.

Example:
::

    autosubmit recovery cxxx --all -s

How to rerun a part of the experiment
-------------------------------------

This procedure allows you to create automatically a new pickle with a list of jobs of the experiment to rerun.

Using the ``expdef_<expid>.yml`` the ``create`` command will generate the rerun if the variable RERUN is set to TRUE and a RERUN_JOBLIST is provided.

Additionally, you can have re-run only jobs that won't be include in the default job_list. In order to do that, you have to set RERUN_ONLY in the jobs conf of the corresponding job.

::

    autosubmit create cxxx

It will read the list of jobs specified in the RERUN_JOBLIST and will generate a new plot.

Example:
::

    vi <experiments_directory>/cxxx/conf/expdef_cxxx.yml

.. code-block:: yaml

    ...

    rerun:
        RERUN: TRUE
        RERUN_JOBLIST: RERUN_TEST_INI;SIM[19600101[C:3]],RERUN_TEST_INI_chunks[19600101[C:3]]
    ...

    vi <experiments_directory>/cxxx/conf/jobs_cxxx.yml

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
    nohup autosubmit run cxxx &

