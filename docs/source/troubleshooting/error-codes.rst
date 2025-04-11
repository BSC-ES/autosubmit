#########################
Error codes and solutions
#########################

.. note::
  Increasing the logging level gives you more detailed information
  about your error, e.g. ``autosubmit -lc DEBUG -lf DEBUG <CMD>``,
  where ``<CMD>`` could be ``create``, ``run``, etc.

Every error in Autosubmit contains a numeric error code, to help users and developers
to identify the category of the error. These errors are organized as follows:

+---------------+-------------+
| Level         | Starts from |
+===============+=============+
| EVERYTHING    | 0           |
+---------------+-------------+
| STATUS_FAILED | 500         |
+---------------+-------------+
| STATUS        | 1000        |
+---------------+-------------+
| DEBUG         | 2000        |
+---------------+-------------+
| WARNING       | 3000        |
+---------------+-------------+
| INFO          | 4000        |
+---------------+-------------+
| RESULT        | 5000        |
+---------------+-------------+
| ERROR         | 6000        |
+---------------+-------------+
| CRITICAL      | 7000        |
+---------------+-------------+
| NO_LOG        | 8000        |
+---------------+-------------+

Levels such as ``DEBUG``, ``WARNING``, ``INFO``, and ``RESULT`` are commonly
used when writing log messages. You may find it in the output of commands in
case there is a minor issue with your configuration such as a deprecated call.

The two levels that normally appear with traceback and important log messages
are either ``ERROR`` or ``CRITICAL``.

Autosubmit has two error types. ``AutosubmitError`` uses the ``ERRORR`` level,
and is raised for minor errors where the program execution may be able to
recover. ``AutosubmitCritical`` uses the ``CRITICAL`` level and is for errors
that abort the program execution.

The detailed error codes along with details and possible workarounds are
listed below.

Minor errors  - Error codes [6000+]
===================================

+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| Code | Details                                  | Solution                                                                                       |
+======+==========================================+================================================================================================+
| 6001 | Failed to retrieve log files             | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6002 | Failed to reconnect                      | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6003 | Failed connection, wrong configuration   | Check your platform configuration                                                              |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6004 | Input output issues                      | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6005 | Unable to execute the command            | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6006 | Failed command                           | Check err output for more info, command worked but some issue was detected                     |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6007 | Broken sFTP connection                   | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6008 | Inconsistent/unexpected, job status      | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6009 | Failed job checker                       | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6010 | Corrupted ``job_list`` using backup      | Automatically, if it fails try ``mv <EXPID>/pkl/job_list_backup.pkl <EXPID>/pkl/job_list.pkl`` |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6011 | Incorrect mail notifier configuration    | Double check your mail configuration on your job configuration (job status) and                |
|      |                                          | the experiment configuration (email)                                                           |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6012 | Migrate, archive/unarchive I/O issues    | Check the migrate configuration                                                                |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6013 | Configuration issues                     | Check log output for more info                                                                 |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6014 | Git can not clone repository submodule   | Check submodule url, perform a ``refresh``                                                     |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6015 | Submission failed                        | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+
| 6016 | Temporary connection issues              | Automatically, if there are no major issues                                                    |
+------+------------------------------------------+------------------------------------------------------------------------------------------------+

Experiment Locked - Critical Error 7000
=======================================

+-------+-------------------------------------------------------------------+------------------------------------------------------------------------------+
| Code  | Details                                                           | Solution                                                                     |
+=======+===================================================================+==============================================================================+
| 7000  | Experiment is locked due another instance of Autosubmit using it  | Halt other experiment instances, then ``rm <EXPID>/tmp/autosubmit.lock``     |
+-------+-------------------------------------------------------------------+------------------------------------------------------------------------------+

Database Issues  - Critical Error codes [7001-7009]
===================================================

These issues occur due to server side issues. Check your site settings, and
report an issue to the Autosubmit team in Git if the issue persists.

+------+-----------------------------------------------+-----------------------------------------------------------------+
| Code | Details                                       | Solution                                                        |
+======+===============================================+=================================================================+
| 7001 | Connection to the db could not be established | Check if database exists                                        |
+------+-----------------------------------------------+-----------------------------------------------------------------+
| 7002 | Wrong version                                 | Check system sqlite version                                     |
+------+-----------------------------------------------+-----------------------------------------------------------------+
| 7003 | Database doesn't exist                        | Check if database exists                                        |
+------+-----------------------------------------------+-----------------------------------------------------------------+
| 7004 | Can't create a new database                   | Check your user permissions                                     |
+------+-----------------------------------------------+-----------------------------------------------------------------+
| 7005 | AS database is corrupted or locked            | Report the issue to the Autosubmit team in Git                  |
+------+-----------------------------------------------+-----------------------------------------------------------------+
| 7006 | Experiment database not found                 | Ask the site administrator to run ``autosubmit install``        |
+------+-----------------------------------------------+-----------------------------------------------------------------+
| 7007 | Experiment database permissions               | Invalid permissions, ask your administrator to add ``R/W``      |
+------+-----------------------------------------------+-----------------------------------------------------------------+

Wrong User Input  - Critical Error codes [7010-7030]
====================================================

These issues are caused by the user input. Check the logs and also the
existing issues in git for possible workarounds. Report an issue to the
Autosubmit team in Git if the issue persists.

+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+
| Code | Details                                              | Solution                                                                                       |
+======+======================================================+================================================================================================+
| 7010 | Experiment has been halted manually                  |                                                                                                |
+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+
| 7011 | Wrong arguments for a specific command               | Check the command section for more information                                                 |
+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+
| 7012 | Insufficient permissions for an specific experiment  | Check if you have enough permissions, and that the experiment exists                           |
+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+
| 7013 | Pending commits                                      | You must commit pending changes in the experiment ``proj`` folder                              |
+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+
| 7014 | Wrong configuration                                  | Check your experiment configuration files, and at the ``<EXPID>/tmp/ASLOG/<CMD>.log`` output   |
+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+
| 7015 | Job list is empty                                    | Check your experiment configuration files                                                      |
+------+------------------------------------------------------+------------------------------------------------------------------------------------------------+

Platform issues  - Critical Error codes. Local [7040-7050] and remote [7050-7060]
=================================================================================

The Autosubmit logs should contain more detailed information about the error.
Check your platform configuration and general status (connectivity, permissions,
etc.).

+------+-----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| Code | Details                                                         | Solution                                                                                                                                |
+======+=================================================================+=========================================================================================================================================+
| 7040 | Invalid experiment ``pkl`` or ``db`` files                      | Should be recovered automatically, if not check if there is a backup file and do it manually                                            |
+------+-----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| 7041 | Unexpected job status                                           | Try to run ``autosubmit recovery <EXPID>``, report the issue to the Autosubmit team if it persists                                      |
+------+-----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| 7050 | Connection can not be established                               | Check your experiment platform configuration                                                                                            |
+------+-----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| 7051 | Invalid SSH configuration                                       | Check ``.ssh/config`` file. Additionally, check if you can perform a password-less connection to that platform                          |
+------+-----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
| 7052 | Scheduler is not installed or not correctly configured          | Check if there is a scheduler installed in the remote machine                                                                           |
+------+-----------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------+

Uncatalogued codes  - Critical Error codes [7060+]
==================================================

The Autosubmit logs should contain more detailed information about the error.
If you believe you found a bug, feel free to report an issue to the Autosubmit
team in Git.

+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Code | Details                                       | Solution                                                                                                                                                                         |
+======+===============================================+==================================================================================================================================================================================+
| 7060 | Display issues during monitoring              | Use a different output or use plain text (``txt``)                                                                                                                               |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7061 | Stat command failed                           | Check the command output in ``ASLOGS`` for a possible bug, report it to the Autosubmit team in Git                                                                               |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7062 | Svn issues                                    | Check if URL was configured in the experiment configuration                                                                                                                      |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7063 | cp/rsync issues                               | Check if destination path exists                                                                                                                                                 |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7064 | Git issues                                    | Check ``GIT:`` experiment configuration. If issue persists, check if ``proj`` folder is a valid Git repository                                                                   |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7065 | Wrong git configuration                       | Invalid Git url. Check ``GIT:`` experiment configuration. If issue persists, check if ``proj`` folder is a valid Git repository                                                  |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7066 | Pre-submission feature issues                 | New feature, this message should not be issued, Please report it in Git                                                                                                          |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7067 | Historical Database not found                 | Configure ``historicdb: PATH:<file_path>``                                                                                                                                       |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7068 | Monitor output can't be loaded                | Try another output method, check if the experiment exists and is readable                                                                                                        |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7069 | Monitor output format invalid                 | Try another output method                                                                                                                                                        |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7070 | Bug in the code                               | Please submit an issue to the Autosubmit team in Git                                                                                                                             |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7071 | AS can't run in this host                     | If you think that this is an error, check the ``.autosubmitrc`` and modify the allowed and forbidden directives                                                                  |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7072 | Basic configuration not found                 | Administrator: run ``autosubmit configure --advanced`` or create a common file in ``/etc/autosubmitrc``.                                                                         |
|      |                                               | User: run ``autosubmit configure`` or create a ``$HOME/.autosubmitrc`` (consult the installation documentation)                                                                  |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7073 | Private key is encrypted                      | Add your key to your ssh agent, e.g. ``ssh-add $HOME/.ssh/id_rsa``, then try running Autosubmit again.                                                                           |
|      |                                               | You can also use a non-encrypted key (make sure nobody else has access to the file)                                                                                              |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7074 | Profiling process failed                      | You can find more detailed information in the logs, as well as hints to solve the problem                                                                                        |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 7075 | Local changes have not been pushed to remote Git or SVN repo   | Commit and push local changes before running the experiment                                                                           
  |
+------+-----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

.. note::
  Please submit an issue to the Autosubmit team if you have not found your error
  code listed here.
