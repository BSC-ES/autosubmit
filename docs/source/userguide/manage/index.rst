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

* The -p and -s flags are used to clean our experiment ``plot`` folder to save disk space. Only the two latest plots will be kept. Older plots will be removed.

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

.. note:: In addition, every time you run this command with the -pr option, it will check the commit unique identifier for the local working tree existing in the ``proj`` directory.
    If that commit identifier exists, clean will register it in the ``expdef_<EXPID>.yml`` file.

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

The archived experiment will be stored as a ``tar.gz`` file, under
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

*<EXPID>* is the experiment identifier. You can pass a single
identifier, or a comma-separated list of identifiers.

.. warning:: DO NOT USE THIS COMMAND IF YOU ARE NOT SURE !
    It deletes the experiment from database and experiment’s folder.

Options:

.. runcmd:: autosubmit delete -h

Example:
::

    autosubmit delete <EXPID>

.. warning:: Be careful ! force option does not ask for your confirmation.

How to synchronize with the project's latest changes
----------------------------------------------------

Autosubmit supports directly fetching files from the repository, which could be local or remote.

In order to synchronize with remote, use the command:

::

    autosubmit refresh <EXPID>

where *<EXPID>* is the experiment identifier.

It checks the experiment configuration and copies code from the original repository to project directory.

.. warning:: THIS WILL OVERWRITE LOCAL CHANGES!
    Project directory ``<EXPID>/proj`` will be overwritten and you may loose local changes.


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
    Otherwise a running instance of Autosubmit, at some point, will overwrite any changes you may have done.

You must execute:
::

    autosubmit setstatus <EXPID> <FILTER> <VALUE_TO_FILTER> -t <STATUS_FINAL> -s

By default, plots are **not** generated when changing status. To generate plots showing the updated job statuses, use the ``-plt`` or ``--plot`` option.

::

    autosubmit setstatus <EXPID> <FILTER> <VALUE_TO_FILTER> -t <STATUS_FINAL> -s -plt

Where:

+--------+----------------------------------------------+----------------------------------------------+
| FILTER | Meaning                                      | Example of VALUE_TO_FILTER                   |
+========+==============================================+==============================================+
| -fl    | filter by job name                           | ``-fl "a000_20101101_fc3_21_SIM"``           |
+--------+----------------------------------------------+----------------------------------------------+
| -fs    | filter by job status                         | ``-fs FAILED``                               |
+--------+----------------------------------------------+----------------------------------------------+
| -ft    | filter by job type  (and optionally split)   | ``-ft TRANSFER``                             |
+--------+----------------------------------------------+----------------------------------------------+
| -fc    | filter by chunk/section/split                | ``-fc "[ 19601101 [ fc1 [1] ] ]"``           |
+--------+----------------------------------------------+----------------------------------------------+

If multiple filters are provided (``-fl, fs, ft, fc``), they will be combined as logical AND, meaning that only jobs matching ALL specified filters will have their status changed.

Mandatory arguments:

* ``<EXPID>``: experiment identifier
* ``<FILTER> <VALUE_TO_FILTER>``: at least one filter is required to select jobs (see below for filter options)
* ``-t STATUS_FINAL``: target status (``READY``, ``COMPLETED``, ``WAITING``, ``SUSPENDED``, ``HELD``, ``UNKNOWN``)

Optional filter arguments (combine multiple for granular selection):

* ``-fl``: space-separated list of job names

::

    autosubmit setstatus <EXPID> -fl "<EXPID>_20101101_fc3_21_SIM <EXPID>_20111101_fc4_26_SIM" -t READY -s

* ``-fc``: chunk/section/split filter (JSON-like format, takes precedence over legacy filters)

::

    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc1 [1] ] ]" -t READY -s

* ``-fs``: space-separated job statuses. The available statuses are: ``READY``, ``COMPLETED``, ``WAITING``, ``SUSPENDED``, ``HELD``, and ``UNKNOWN``.

::

    autosubmit setstatus <EXPID> -fs FAILED -t READY -s

* ``-ft``: space-separated job types with optional split selection

::

    autosubmit setstatus <EXPID> -ft TRANSFER -t SUSPENDED -s
    autosubmit setstatus <EXPID> -ft "TRANSFER[1 2]" -t SUSPENDED -s

* ``-ftc``: **[DEPRECATED]** legacy chunk/type filter (use a combination of ``-fc`` and ``-ft`` instead).

Command:
::

    autosubmit setstatus <EXPID> -ftc "[ 19601101 [ fc0 [1 2 3 4] ] ],SIM" -t READY -s

Can be replaced with:
::

    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc0 [1 2 3 4] ] ]" -ft SIM -t READY -s

* ``-ftcs``: **[DEPRECATED]** legacy chunk/type/split filter (use a combination of ``-fc`` and ``-ft`` instead).

Command:
::

    autosubmit setstatus <EXPID> -ftcs "[ 19601101 [ fc0 [1 2 3 4] ] ],SIM,1" -t READY -s

Can be replaced with:
::

    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc0 [1 2 3 4] ] ]" -ft SIM -t READY -s

Options:

.. runcmd:: autosubmit setstatus -h

Filter precedence (when multiple chunk filters are specified):

When ``-fc``, ``-ftc``, and ``-ftcs`` are combined, precedence is: ``-fc`` → ``-ftc`` → ``-ftcs``. A warning will be logged if multiple chunk filters are detected.

Filter Examples
~~~~~~~~~~~~~~~

Single filter, by job list:
::

    autosubmit setstatus <EXPID> -fl "<EXPID>_20101101_fc3_21_SIM <EXPID>_20111101_fc4_26_SIM" -t READY -s

Single filter, by chunk/section/split:
::

    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc1 [1] ] ]" -t READY -s

Single filter, by current status:
::

    autosubmit setstatus <EXPID> -fs FAILED -t READY -s

Single filter, by job type:
::

    autosubmit setstatus <EXPID> -ft TRANSFER -t SUSPENDED -s

Single filter, by job type and split:
::

    autosubmit setstatus <EXPID> -ft "TRANSFER[1:5]" -t SUSPENDED -s

Multiple filters combined (AND logic, selects jobs matching ALL filters):
::

    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc1 [1] ] ]" -ft "SIM" -t SUSPENDED -s

This selects jobs that are in both the chunk filter AND have type "SIM".

Chunk/Section/Split Filter Details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Format:
::

    [ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]

Date (month) range example:
::

    autosubmit setstatus <EXPID> -fc "[ 1960(1101-1201) [ fc1 [1] ] ]" -ft "SIM" -t SUSPENDED -s

Result:
::

    <EXPID>_19601101_fc1_1_SIM
    <EXPID>_19601201_fc1_1_SIM


Date (day) range example:
::

    autosubmit setstatus <EXPID> -fc "[ 1960(1101-1105) [ fc1 [1] ] ]" -ft "SIM" -t SUSPENDED -s
    
Result:
::

    <EXPID>_19601101_fc1_1_SIM
    <EXPID>_19601102_fc1_1_SIM
    <EXPID>_19601103_fc1_1_SIM
    <EXPID>_19601104_fc1_1_SIM
    <EXPID>_19601105_fc1_1_SIM

Using the "Any" Keyword
~~~~~~~~~~~~~~~~~~~~~~~~

The keyword ``Any`` (case-insensitive) means "no restriction" in that filter:

* ``-fl Any``: all jobs (no job list restriction)
* ``-fs Any``: all job statuses (no status restriction)
* ``-ft Any``: all job types (no type restriction)

Example:
::

    autosubmit setstatus <EXPID> -fs Any -fc "[ 19601101 [ fc1 [1] ] ]" -t READY -s

This changes all jobs in the chunk filter to ``READY``, regardless of their current status.

Deprecated Filters
~~~~~~~~~~~~~~~~~~~

.. warning:: The ``-ftc`` and ``-ftcs`` filters are deprecated and will be removed in future versions. Use ``-fc`` instead.

``-ftc`` (deprecated) is similar to ``-fc`` but also accepts job types without separate ``-ft``:
::

    autosubmit setstatus <EXPID> -ftc "[ 19601101 [ fc0 [1 2 3 4] ] ],SIM" -t READY -s

Use ``-fc`` with ``-ft`` instead:
::

    autosubmit setstatus <EXPID> -fc "[ 19601101 [ fc0 [1 2 3 4] ] ]" -ft SIM -t READY -s

.. hint:: When satisfied with your filter selections, use the parameter ``-s`` to save changes to the pkl file. In order to understand more the grouping options, which are used for visualization purposes, please check :ref:`grouping`.
.. _setstatusno:

How to change the job status without stopping autosubmit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This procedure allows you to modify the status of your jobs without having to stop Autosubmit.

You must create a file in ``<experiments_directory>/<EXPID>/status/`` named:
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

    <EXPID>_20101101_fc3_21_SIM    READY
    <EXPID>_20111101_fc4_26_SIM    READY

If Autosubmit finds the above file, it will process it. You can check that the processing was OK at a given date and time,
if you see that the file name has changed to:
::

    update_list_<EXPID>_<DATE>_<TIME>.txt

.. note:: A running instance of Autosubmit will check the existence of the above file after checking already submitted jobs.
    It may take some time, depending on the setting ``SAFETYSLEEPTIME``.



.. warning:: Keep in mind that autosubmit reads the file automatically, so it is suggested to create the file in another location like ``/tmp`` or ``/var/tmp`` and then copy/move it to the ``pkl`` folder. Alternatively, you can create the file with a different name and rename it when you have finished.
