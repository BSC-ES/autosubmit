.. toctree::
   :caption: Troubleshooting
   :maxdepth: 1

   /troubleshooting/error-codes

###############
Troubleshooting
###############

Changing the job status with Autosubmit stopped
===============================================

Review :ref:`setstatus`.

Changing the job status while Autosubmit runs
=============================================

Review :ref:`setstatusno`.

My project parameters are not being substituted in the templates
================================================================

*Explanation*: If there is a duplicated section or option in any other side of autosubmit, including proj files, it
won't be able to recognize which option pertains to what section in which file.

*Solution*: Don't repeat section names and parameters names until Autosubmit 4.0 release.

Unable to recover remote logs files
===================================

*Explanation*: If there are limitations on the remote platform regarding multiple connections.
*Solution*: You can try DISABLE_RECOVERY_THREADS: TRUE under the platform_name: section in the platforms_<EXPID>.yml.

Error on create caused by a configuration parsing error
=======================================================

When running create, you can come across an error similar to:
::

    [ERROR] Trace: '%' must be followed by '%' or '(', found: u'%HPCROOTDIR%/remoteconfig/%CURRENT_ARCH%_launcher.sh'

The important part of this error is the message ``'%' must be followed by '%'``. It indicates that the source of the
error is the ``configparser`` library.
This library is included in the python common libraries, so you shouldn't have any other version of it installed in your
environment. Execute ``pip list``, if you see
``configparser`` in the list, then run ``pip uninstall configparser``. Then, try to create your experiment again.

Error when using jobs.<job>.VALIDATE: True
==========================================


When using the ``VALIDATE: True`` option in your job configuration, you might encounter errors related to syntax issues
in the generated Python script for that job.
This validation checks if your script is compatible with autosubmit and will print any errors it finds.
To get rid of this error, you need to fix the syntax issues in your job script.
The error message will indicate the specific line and type of syntax error that needs to be addressed.

Example of how to enable validation for a job in your configuration:

.. tab-set-code::

    .. code-block:: yaml

        JOBS:
            JOB:
                FILE: <path_to_your_script>
                VALIDATE: True

Example output of the command:

.. code-block:: none

    [CRITICAL] Syntax error in generated Python script for job t006_LOCAL_SETUP: unexpected indent (<string>, line 45) [eCode=7014]




The Autosubmit process uses more and more memory during a long run
==================================================================

*Explanation*: Python and the system allocator keep memory that has already been freed, so a long-running Autosubmit process can keep a high memory footprint even after its jobs have finished. By default Autosubmit does not force returning that free memory to the operating system.

*Solution*: If your long-running experiment (or the machine it runs on) shows high memory usage, you can opt in to returning free memory to the OS with ``CONFIG.MEMORY_RELEASE_MODE``:

* ``off`` (default): never force a release.
* ``on_unload``: compact the heap whenever finished jobs are unloaded.
* ``interval``: compact the heap every ``CONFIG.MEMORY_RELEASE_INTERVAL`` iterations of the run loop.

To apply it to every experiment on the machine, set it in the ``[runtime]`` section of the autosubmitrc (see :ref:`configure-autosubmit`):

.. code-block:: ini

    [runtime]
    memory_release_mode = interval
    memory_release_interval = 10

To override it for a single experiment, put it in ``$EXPID/conf/asruntime.yml`` (the experiment configuration wins over the autosubmitrc default):

.. code-block:: yaml

    RUNTIME:
        MEMORY_RELEASE_MODE: "on_unload"

The release calls ``gc.collect()`` followed by glibc's ``malloc_trim``. It is a safe no-op on platforms without ``malloc_trim``.

.. note:: Today ``asruntime.yml`` is created manually. A future change will add a command to generate it with the commented defaults.

Other possible errors
=====================

**I see the ``database malformed`` error on my experiment log.**

*Explanation*: The latest version of autosubmit uses a database to efficiently track changes in the jobs of your
experiment. It could have happened that this small database got corrupted.

*Solution*: run ``autosubmit dbfix <EXPID>`` where ``<EXPID>`` is the identifier of your experiment. This function will
rebuild the database saving as much information as possible (usually all of it).

Error codes
===========

The latest version of **Autosubmit** implements a code system that guides you through the process of fixing some of the
common problems you might find. Check :doc:`error-codes`, where you will find the list of error codes, their
descriptions, and solutions.

Changelog
=========

For the changes in each release, see the `CHANGELOG on GitHub`_, which is
always up to date.

The :doc:`changelog page <changelog>` in these docs also covers migrating
configuration from Autosubmit 3 to 4.

.. _CHANGELOG on GitHub: https://github.com/BSC-ES/autosubmit/blob/master/CHANGELOG.md
