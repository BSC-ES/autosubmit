Manage Experiments
===================

.. _clean:

How to clean the experiment
---------------------------

This procedure allows you to save space after finalising an experiment.
You must execute:
::

    autosubmit clean <EXPID>


Options:

.. runcmd:: autosubmit clean -h

* The -p and -s flag are used to clean our experiment ``plot`` folder to save disk space. Only the two latest plots will be kept. Older plots will be removed.

Example:
::

    autosubmit clean <EXPID> -p

* The -pr flag is used to clean our experiment ``proj`` locally in order to save space (it could be particularly big).

.. caution:: Bear in mind that if you have not synchronized your experiment project folder with the information available on the remote repository (i.e.: commit and push any changes we may have), or in case new files are found, the clean procedure will be failing although you specify the -pr option.

Example:
::

    autosubmit clean <EXPID> -pr

A bare copy (which occupies less space on disk) will be automatically made.

.. hint:: That bare clone can be always reconverted in a working clone if we want to run again the experiment by using ``git clone bare_clone original_clone``.

.. note:: In addition, every time you run this command with -pr option, it will check the commit unique identifier for local working tree existing on the ``proj`` directory.
    In case that commit identifier exists, clean will register it to the ``expdef_<EXPID>.yml`` file.

.. _archive:

How to archive an experiment
----------------------------

When you archive an experiment in Autosubmit, it automatically :ref:`cleans <clean>`
the experiment as well. This means the experiment will not be available for
use, unless it is unarchived.

.. code-block::

    autosubmit archive EXPID

Options:

.. runcmd:: autosubmit archive -h

The archived experiment will be stored as a ``tar.gz` file, under
a directory named after the year of the last ``_COMPLETED`` file
date or, if no ``_COMPLETED`` job is present, it will use the year of
the date the ``autosubmit archive`` was run (e.g. for the selected
year ``2023``, the location will be ``$HOME/autosubmit/2023/<EXPID>.tar.gz``).

How to unarchive an experiment
------------------------------

To unarchive an experiment, use the command:

.. code-block::

    autosubmit unarchive <EXPID>

Options:

.. runcmd:: autosubmit unarchive -h

How to delete the experiment
----------------------------

To delete the experiment, use the command:
::

    autosubmit delete <EXPID>

*<EXPID>* is the experiment identifier.

.. warning:: DO NOT USE THIS COMMAND IF YOU ARE NOT SURE !
    It deletes the experiment from database and experiment’s folder.

Options:

.. runcmd:: autosubmit delete -h

Example:
::

    autosubmit delete <EXPID>

.. warning:: Be careful ! force option does not ask for your confirmation.

How to migrate an experiment
----------------------------

The Autosubmit Migrate command is used to migrate data from one user to another.
::

   autosubmit migrate EXPID

Options:

.. runcmd:: autosubmit migrate -h

To migrate it, you need to generate a new file inside <EXPID>/conf/ with the **new user** information for each platform that you want to migrate.


Platform file example: $expid/conf/platforms_<EXPID>.yml
::

    PLATFORMS:
        test-local:
            type: ps
            host: 127.0.0.1
            user: "original_owner"
            project: "original_project"
            scratch_dir: "/tmp/scratch"
        no-migrated-platform:
            ...

Migrate file example: $expid/conf/migrate_<EXPID>.yml
::

    AS_MISC: True # Important to set this flag to True
    PLATFORMS:
        test-local: # must match the one in platforms file
            type: ps
            host: 127.0.0.1 # can change
            user: new_user # can change
            project: new_project  # can change
            scratch_dir: "/tmp/scratch"
            temp_dir: "/tmp/scratch/migrate_tmp_dir" # must be in the same filesystem
            same_user: False # If the user is the same in the new platform, set this flag to True


.. warning:: The USER in the migrate file must be a different user, in case you want to maintain the same user, put SAME_USER: True.

.. warning:: The temporary directory(%PLATFORMS.TEST-LOCAL.TEMP_DIR%) must be set in the <EXPID>/conf/migrate_<EXPID>.yml file.

.. warning:: The temporary directory(%PLATFORMS.TEST-LOCAL.TEMP_DIR%) must be readable by both users (old owner and new owner)
    Example for a RES account to BSC account the tmp folder must have rwx|rwx|--- permissions.
    The temporary directory must be in the same filesystem.

User A, To offer the experiment:
::

    autosubmit migrate --offer <EXPID>

Local files will be archived and remote files put in the HPC temporary directory.

User A To only offer the remote files
::

    autosubmit migrate <EXPID> --offer --onlyremote

Only remote files will be put in the HPC temporary directory.

.. warning:: Be sure that there is no folder named as the expid before do the pick.
    The old owner might need to remove temporal files and archive.
    To Run the experiment the queue may need to be change.

.. warning:: If onlyremote option is selected, the pickup must maintain the flag otherwise the command will fail.

Now to pick the experiment, the user B, must do
::

    autosubmit migrate --pickup <EXPID>

Local files will be unarchived and remote files copied from the temporal location.

To only pick the remote files, the user B, must do
::

    autosubmit migrate --pickup <EXPID> --onlyremote

How to synchronize with the project's latest changes
----------------------------------------------------

Autosubmit supports directly fetching files from the repository, which could be local or remote.

In order to synchronize with remote, use the command:

::

    autosubmit refresh <EXPID>

where *<EXPID>* is the experiment identifier.

It checks the experiment configuration and copies code from the original repository to project directory.

.. warning:: THIS WILL OVERWRITE LOCAL CHANGES!
    Project directory ( <EXPID>/proj will be overwritten and you may loose local changes.


Options:
.. runcmd:: autosubmit refresh -h

Example:
::

    autosubmit refresh <EXPID>

.. _updateDescrip:

How to update the description of your experiment
------------------------------------------------

Use the command:
::

    autosubmit updatedescrip <EXPID> "DESCRIPTION"

*EXPID* is the experiment identifier.

*DESCRIPTION* is the new description of your experiment.

Options:

.. runcmd:: autosubmit updatedescrip -h

Autosubmit will validate the provided data and print the results in the command line.

Example:
::

    autosubmit a29z "Updated using Autosubmit updatedescrip"

.. _setstatus:

How to change the job status
----------------------------

This procedure allows you to modify the status of your jobs.

.. warning:: Beware that Autosubmit must be stopped to use ``setstatus``.
    Otherwise a running instance of Autosubmit, at some point, will overwrite any change you may have done.

You must execute:
::

    autosubmit setstatus <EXPID> -fs STATUS_ORIGINAL -t STATUS_FINAL -s

*<EXPID>* is the experiment identifier.
*STATUS_ORIGINAL* is the original status to filter by the list of jobs.
*STATUS_FINAL* the desired target status.

Options:

.. runcmd:: autosubmit setstatus -h

Examples:
::

    autosubmit setstatus <EXPID> -fl "<EXPID>_20101101_fc3_21_sim <EXPID>_20111101_fc4_26_sim" -t READY -s
    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc1 [1] ] ]" -t READY -s
    autosubmit setstatus <EXPID> -fs FAILED -t READY -s
    autosubmit setstatus <EXPID> -ft TRANSFER -t SUSPENDED -s
    autosubmit setstatus <EXPID> -ftc "[ 19601101 [ fc1 [1] ] ], SIM" -t SUSPENDED -s

Date (month) range example:
::

    autosubmit setstatus <EXPID> -ftc "[ 1960(1101-1201) [ fc1 [1] ] ], SIM" -t SUSPENDED -s

This example will result changing the following jobs:
::

    <EXPID>_19601101_fc1_1_SIM
    <EXPID>_19601201_fc1_1_SIM

Date (day) range example:
::

    autosubmit setstatus <EXPID> -ftc "[ 1960(1101-1105) [ fc1 [1] ] ], SIM" -t SUSPENDED -s

Result:
::

    <EXPID>_19601101_fc1_1_SIM
    <EXPID>_19601102_fc1_1_SIM
    <EXPID>_19601103_fc1_1_SIM
    <EXPID>_19601104_fc1_1_SIM
    <EXPID>_19601105_fc1_1_SIM

This script has two mandatory arguments.

The -t where you must specify the target status of the jobs you want to change to:
::

    {READY,COMPLETED,WAITING,SUSPENDED,FAILED,UNKNOWN}


The second argument has four alternatives, the -fl, -fc, -fs and -ft; with those we can apply a filter for the jobs we want to change:

* The -fl variable receives a list of job names separated by blank spaces: e.g.:
    ::

     "<EXPID>_20101101_fc3_21_sim <EXPID>_20111101_fc4_26_sim"

If we supply the key word "Any", all jobs will be changed to the target status.

* The variable -fc should be a list of individual chunks or ranges of chunks in the following format:
    ::

        [ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]

* The variable -fs can be the following status for job:
    ::

        {Any,READY,COMPLETED,WAITING,SUSPENDED,FAILED,UNKNOWN}

* The variable -ft can be one of the defined types of job.

The variable -ftc acts similar to -fc but also accepts the job types. It does not accept chunk ranges e.g. "1-10", but accepts the wildcard "Any" for members and job types. Let's look at some examples.

* Using -ftc to change the chunks "1 2 3 4" of member "fc0" and chunk "1" of member "fc1" for the starting date "19601101", where these changes apply only for the "SIM" jobs:
    ::

        [ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] ],SIM

* Using -ftc to change the chunks "1 2 3 4" of all members for the starting date "19601101", where these changes apply only for the "SIM" jobs:
    ::

        [ 19601101 [ Any [1 2 3 4] ] ],SIM

* Using -ftc to change the chunks "1 2 3 4" of "fc0" members for the starting date "19601101", where these changes apply to all jobs:
    ::

        [ 19601101 [ fc0 [1 2 3 4] ] ],Any

Try the combinations you come up with. Autosubmit will supply with proper feedback when a wrong combination is supplied.

.. hint:: When we are satisfied with the results we can use the parameter -s, which will save the change to the pkl file. In order to understand more the grouping options, which are used for visualization purposes, please check :ref:`grouping`.

.. _setstatusno:

How to change the job status without stopping autosubmit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This procedure allows you to modify the status of your jobs without having to stop Autosubmit.

You must create a file in ``<experiments_directory>/<EXPID>/pkl/`` named:
::

    updated_list_<EXPID>.txt

Format:

This file should have two columns: the first one has to be the job_name and the second one the status.

Options:
::

    READY,COMPLETED,WAITING,SUSPENDED,FAILED,UNKNOWN

Example:
::

    vi updated_list_<EXPID>.txt

.. code-block:: ini

    <EXPID>_20101101_fc3_21_sim    READY
    <EXPID>_20111101_fc4_26_sim    READY

If Autosubmit finds the above file, it will process it. You can check that the processing was OK at a given date and time,
if you see that the file name has changed to:
::

    update_list_<EXPID>_<DATE>_<TIME>.txt

.. note:: A running instance of Autosubmit will check the existence of adobe file after checking already submitted jobs.
    It may take some time, depending on the setting ``SAFETYSLEEPTIME``.



.. warning:: Keep in mind that autosubmit reads the file automatically so it is suggested to create the file in another location like ``/tmp`` or ``/var/tmp`` and then copy/move it to the ``pkl`` folder. Alternatively you can create the file with a different name an rename it when you have finished.
