Create an Experiment
====================

Create new experiment
-------------------------

To create a new experiment, just run the command:
::

    autosubmit expid -H HPCname -d Description

*HPCname* is the name of the main HPC platform for the experiment: it will be the default platform for the tasks.
*Description* is a brief experiment description.

Options:
::

    usage: autosubmit expid [-h] [-y COPY] [-dm] [-min] [-repo GIT_REPO]
                            [-b GIT_BRANCH] [-conf GIT_AS_CONF] [-local] [-op]
                            [-H HPC] -d DESCRIPTION [-t]
                            [-fs {Any,READY,COMPLETED,WAITING,SUSPENDED,FAILED,UNKNOWN}]
    
    Creates a new experiment
    
    optional arguments:
      -h, --help            show this help message and exit
      -y COPY, --copy COPY  makes a copy of the specified experiment
      -dm, --dummy          creates a new experiment with default values, usually
                            for testing
      -min, --minimal_configuration
                            creates a new experiment with minimal configuration,
                            usually combined with -repo
      -repo GIT_REPO, --git_repo GIT_REPO
                            sets a git repository for the experiment
      -b GIT_BRANCH, --git_branch GIT_BRANCH
                            sets a git branch for the experiment
      -conf GIT_AS_CONF, --git_as_conf GIT_AS_CONF
                            sets the git path to as_conf
      -local, --use_local_minimal
                            uses local minimal file instead of git
      -op, --operational    creates a new experiment with operational experiment
                            id
      -H HPC, --HPC HPC     specifies the HPC to use for the experiment
      -d DESCRIPTION, --description DESCRIPTION
                            sets a description for the experiment to store in the
                            database.
      -t, --testcase        creates a new experiment with testcase experiment id
      -fs {Any,READY,COMPLETED,WAITING,SUSPENDED,FAILED,UNKNOWN}, --filter_status {Any,READY,COMPLETED,WAITING,SUSPENDED,FAILED,UNKNOWN}
                            Select the original status to filter the list of jobs

Example:
::

    autosubmit expid --HPC marenostrum4 --description "experiment is about..."
    autosubmit expid -min -repo https://earth.bsc.es/gitlab/ces/auto-advanced_config_example -b main -conf as_conf -d "minimal config example"
    autosubmit expid -dm -d "dummy test"


If there is an autosubmitrc or .autosubmitrc file in your home directory (cd ~), you can setup a default file from where the contents of platforms_expid.yml should be copied.

In this autosubmitrc or .autosubmitrc file, include the configuration setting custom_platforms:

Example:
::

    conf:
        custom_platforms: /home/Earth/user/custom.yml

Where the specified path should be complete, as something you would get when executing pwd, and also include the filename of your custom platforms content.

Copy another experiment
--------------------------

This option makes a copy of an existing experiment.
It registers a new unique identifier and copies all configuration files in the new experiment folder:
::

    autosubmit expid -y COPY -H HPCname -d Description
    autosubmit expid -y COPY -c PATH -H HPCname -d Description

*HPCname* is the name of the main HPC platform for the experiment: it will be the default platform for the tasks.
*COPY* is the experiment identifier to copy from.
*Description* is a brief experiment description.
*CONFIG* is a folder that exists.

Example:
::

    autosubmit expid -y cxxx -H ithaca -d "experiment is about..."
    autosubmit expid -y cxxx -p "/esarchive/autosubmit/genericFiles/conf" -H marenostrum4 -d "experiment is about..."

.. warning:: You can only copy experiments created with Autosubmit 3.11 or above.

If there is an autosubmitrc or .autosubmitrc file in your home directory (cd ~), you can setup a default file from where the contents of platforms_expid.yml should be copied.

In this autosubmitrc or .autosubmitrc file, include the configuration setting custom_platforms:

Example:
::

    conf:
    custom_platforms: /home/Earth/user/custom.yml

Where the specified path should be complete, as something you would get when executing pwd, and also include the filename of your custom platforms content.

Create a dummy experiment
--------------------------------

It is useful to test if Autosubmit is properly configured with a inexpensive experiment. A Dummy experiment will check,
test, and submit to the HPC platform, as any other experiment would.

The job submitted are only sleeps.

This command creates a new experiment with default values, useful for testing:
::

    autosubmit expid -H HPCname -dm -d Description

*HPCname* is the name of the main HPC platform for the experiment: it will be the default platform for the tasks.
*Description* is a brief experiment description.

Example:
::

    autosubmit expid -H ithaca -dm -d "experiment is about..."

Create a test case experiment
------------------------------------

Test case experiments are special experiments which have a reserved first letter "t" at the expid. They are meant to
help differentiate testing suits of the automodels from normal runs.

This method is to create a test case experiment. It creates a new experiment for a test case with a
given number of chunks, start date, member and HPC.

To create a test case experiment, use the command:
::

    autosubmit testcase

Options:
::

    usage: autosubmit testcase [-h] [-y COPY | -min] -d DESCRIPTION [-c CHUNKS]
                               [-m MEMBER] [-s STARDATE] -H HPC [-repo GIT_REPO]
                               [-b GIT_BRANCH] [-conf GIT_AS_CONF] [-local]
    
    create test case experiment
    
    optional arguments:
      -h, --help            show this help message and exit
      -y COPY, --copy COPY  makes a copy of the specified experiment
      -min, --minimal_configuration
                            creates a new experiment with minimal configuration,
                            usually combined with -repo
      -d DESCRIPTION, --description DESCRIPTION
                            description of the test case
      -c CHUNKS, --chunks CHUNKS
                            chunks to run
      -m MEMBER, --member MEMBER
                            member to run
      -s STARDATE, --stardate STARDATE
                            stardate to run
      -H HPC, --HPC HPC     HPC to run experiment on it
      -repo GIT_REPO, --git_repo GIT_REPO
                            sets a git repository for the experiment
      -b GIT_BRANCH, --git_branch GIT_BRANCH
                            sets a git branch for the experiment
      -conf GIT_AS_CONF, --git_as_conf GIT_AS_CONF
                            sets the git path to as_conf
      -local, --use_local_minimal
                            uses local minimal file instead of git

Example:
::

    autosubmit testcase -d "TEST CASE cca-intel auto-ecearth3 layer 0: T511L91-ORCA025L75-LIM3 (cold restart) (a092-a09n)" -H cca-intel -b 3.2.0b_develop -y a09n

Test the experiment
-------------------

This method is to conduct a test for a given experiment. It creates a new experiment for a given experiment with a
given number of chunks with a random start date and a random member to be run on a random HPC.

To test the experiment, use the command:
::

    autosubmit test CHUNKS EXPID

*EXPID* is the experiment identifier.
*CHUNKS* is the number of chunks to run in the test.



Options:
::

    usage: autosubmit test [-h] -c CHUNKS [-m MEMBER] [-s STARDATE] [-H HPC] [-b BRANCH] [-v] EXPID

     test experiment

     positional arguments:
        EXPID                 experiment identifier

     options:
         -h, --help            show this help message and exit
         -c CHUNKS, --chunks CHUNKS
                               chunks to run
         -m MEMBER, --member MEMBER
                               member to run
         -s STARDATE, --stardate STARDATE
                               stardate to run
         -H HPC, --HPC HPC     HPC to run experiment on it
         -b BRANCH, --branch BRANCH
                               branch of git to run (or revision from subversion)
         -v, --update_version  Update experiment version

Example:
::

    autosubmit test -c 1 -s 19801101 -m fc0 -H ithaca -b develop cxxx

.. _create_profiling:

How to profile Autosubmit while creating an experiment
------------------------------------------------------

Autosubmit offers the possibility to profile the experiment creation process. To enable the profiler, just 
add the ``--profile`` (or ``-p``) flag to your ``autosubmit create`` command, as in the following example:

.. code-block:: bash

    autosubmit create --profile EXPID

.. include:: ../../_include/profiler_common.rst
