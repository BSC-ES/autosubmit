.. toctree::
   :caption: Troubleshooting
   :maxdepth: 1

   /troubleshooting/error-codes
   /troubleshooting/changelog

###############
Troubleshooting
###############

How to change the job status stopping autosubmit
================================================

Review :ref:`setstatus`.

How to change the job status without stopping autosubmit
========================================================

Review :ref:`setstatusno`.

My project parameters are not being substituted in the templates
================================================================

*Explanation*: If there is a duplicated section or option in any other side of autosubmit, including proj files It won't be able to recognize which option pertains to what section in which file.

*Solution*: Don't repeat section names and parameters names until Autosubmit 4.0 release.

Unable to recover remote logs files.
========================================================

*Explanation*: If there are limitations on the remote platform regarding multiple connections,
*Solution*:  You can try DISABLE_RECOVERY_THREADS: TRUE under the platform_name: section in the platform_<EXPID>.yml.

Error on create caused by a configuration parsing error
=======================================================

When running create you can come across an error similar to:
::

    [ERROR] Trace: '%' must be followed by '%' or '(', found: u'%HPCROOTDIR%/remoteconfig/%CURRENT_ARCH%_launcher.sh'

The important part of this error is the message ``'%' must be followed by '%'``. It indicated that the source of the error is the ``configparser`` library.
This library is included in the python common libraries, so you shouldn't have any other version of it installed in your environment. Execute ``pip list``, if you see
``configparser`` in the list, then run ``pip uninstall configparser``. Then, try to create your experiment again.

Other possible errors
=====================

**I see the `database malformed` error on my experiment log.**

*Explanation*: The latest version of autosubmit uses a database to efficiently track changes in the jobs of your experiment. It could have happened that this small database got corrupted.

*Solution*: run `autosubmit dbfix <EXPID>` where `<EXPID>` is the identifier of your experiment. This function will rebuild the database saving as much information as possible (usually all of it).

**The pkl file of my experiment is empty but there is a job_list_%<EXPID>%_backup.pkl file that seems to be the real one.**

*Solution*: run `autosubmit pklfix <EXPID>`, it will restore the `backup` file if possible.

Error codes
===========

The latest version of **Autosubmit** implements a code system that guides you through the process of fixing some of the common problems you might find. Check :doc:`error-codes`, where you will find the list of error codes, their descriptions, and solutions.

Changelog
=========

review :doc:`changelog`.