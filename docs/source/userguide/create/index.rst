Create an Experiment
====================

Create new experiment
-------------------------

To create a new experiment, just run the command:
::

    autosubmit expid -H HPCname -d "Description"


*HPCname* is the name of the main HPC platform for the experiment: it will be the default platform for the tasks.
*Description* is a brief experiment description.

Options:

.. runcmd:: autosubmit expid -h

Example:
::

    autosubmit expid --HPC marenostrum4 --description "experiment is about..."
    autosubmit expid -min -repo https://earth.bsc.es/gitlab/ces/auto-advanced_config_example -b main -conf as_conf -d "minimal config example"
    autosubmit expid -dm -d "dummy test"


If there is an autosubmitrc or .autosubmitrc file in your home directory (cd ~), you can setup a default file from where the contents of platforms_<EXPID>.yml should be copied.

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

    autosubmit expid -y <EXPID> -H HPCname -d "Description"
    autosubmit expid -y <EXPID> -c PATH -H HPCname -d "Description"


*HPCname* is the name of the main HPC platform for the experiment: it will be the default platform for the tasks.
*COPY* is the experiment identifier to copy from.
*Description* is a brief experiment description.
*CONFIG* is a folder that exists.

Example:
::

    autosubmit expid -y <EXPID> -H ithaca -d "experiment is about..."
    autosubmit expid -y <EXPID> -p "/esarchive/autosubmit/genericFiles/conf" -H marenostrum4 -d "experiment is about..."

.. warning:: You can only copy experiments created with Autosubmit 3.11 or above.

If there is an autosubmitrc or .autosubmitrc file in your home directory (cd ~), you can setup a default file from where the contents of platforms_<EXPID>.yml should be copied.

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

    autosubmit expid -H HPCname -dm -d "Description"

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

.. runcmd:: autosubmit testcase -h

Example:
::

    autosubmit testcase -d "TEST CASE cca-intel auto-ecearth3 layer 0: T511L91-ORCA025L75-LIM3 (cold restart) (a092-a09n)" -H cca-intel -b 3.2.0b_develop -y a09n

.. _create_profiling:

How to profile Autosubmit while creating an experiment
------------------------------------------------------

Autosubmit offers the possibility to profile the experiment creation process. To enable the profiler, just
add the ``--profile`` (or ``-p``) flag to your ``autosubmit create`` command, as in the following example:

.. code-block:: bash

    autosubmit create --profile <EXPID>

.. include:: ../../_include/profiler_common.rst
