###################
Variables reference
###################

Autosubmit uses a variable substitution system to facilitate the
development of the templates. These variables can be used on templates
with the syntax ``%VARIABLE_NAME%``.

All configuration variables that are not related to the current job
or platform are available by accessing first their parents, e.g.
``%PROJECT.PROJECT_TYPE% or %DEFAULT.EXPID%``.

You can review all variables at any given time by using the
:ref:`report <report>` command, as illustrated below.


.. code-block:: console
    :caption: Example usage of ``autosubmit report``

    $ autosubmit report <EXPID> -all

The command will save the list of variables available to a file
in the experiment area. The groups of variables of Autosubmit are
detailed in the next sections on this page.

.. note:: All the variable tables are displayed in alphabetical order.


.. note::

    Custom configuration files (e.g. ``my-file.yml``) may contain
    configuration like this example:

    .. code-block:: yaml

        MYAPP:
          MYPARAMETER: 42
          ANOTHER_PARAMETER: 1984

    If you configure Autosubmit to include this file with the
    rest of your configuration, then those variables will be
    available to each job as ``%MYAPP.MYPARAMETER%`` and
    ``%MYAPP.ANOTHER_PARAMETER%``.


Job variables
=============

These variables are relatives to the current job. These variables
appear in the output of the :ref:`report <report>` command with the
pattern ``JOBS.${JOB_ID}.${JOB_VARIABLE}=${VALUE}``. They can be used in
templates with ``%JOB_VARIABLE%``.

.. autosubmit-variables:: job


The following variables are present only in jobs that contain a date
(e.g. ``RUNNING=date``).


.. autosubmit-variables:: chunk

Custom directives
-----------------

There are job variables that Autosubmit automatically converts into
directives for your batch server. For example, ``NUMTHREADS`` will
be set in a Slurm platform as ``--SBATCH --cpus-per-task=$NUMTHREADS``.

However, the variables in Autosubmit do not contain all the directives
available in each platform like Slurm. For values that do not have a
direct variable, you can use ``CUSTOM_DIRECTIVES`` to define them in
your target platform. For instance, to set the number of GPU's in a Slurm
job, you can use ``CUSTOM_DIRECTIVES=--gpus-per-node=10``.


Platform variables
==================

These variables are relative to the platforms defined in each
job configuration. The table below shows the complete set of variables
available in the current platform. These variables appear in the
output of the :ref:`report <report>` command with the pattern
``JOBS.${JOB_ID}.${PLATFORM_VARIABLE}=${VALUE}``. They can be used in
templates with ``%PLATFORM_VARIABLE%``.

A series of variables is also available in each platform, and appear
in the output of the :ref:`report <report>` command with the pattern
``JOBS.${JOB_ID}.PLATFORMS.${PLATFORM_ID}.${PLATFORM_VARIABLE}=${VALUE}``.
They can be used in templates with ``PLATFORMS.%PLATFORM_ID%.%PLATFORM_VARIABLE%``.

.. autosubmit-variables:: platform


.. note::
    The variables ``_USER``, ``_PROJ`` and ``_BUDG``
    have no value on the LOCAL platform.

    Certain variables (e.g. ``_RESERVATION``,
    ``_EXCLUSIVITY``) are only available for certain
    platforms (e.g. MareNostrum).

A set of variables for the experiment's default platform are
also available.

.. TODO: Some variables do not exist anymore, like HPCHOST, HPCUSER, HPCDUG, etc.

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Variable
      - Description
    * - **HPCARCH**
      - Default HPC platform name.
    * - **HPCHOST**
      - Default HPC platform url.
    * - **HPCUSER**
      - Default HPC platform user.
    * - **HPCPROJ**
      - Default HPC platform project.
    * - **HPCBUDG**
      - Default HPC platform budget.
    * - **HPCTYPE**
      - Default HPC platform scheduler type.
    * - **HPCVERSION**
      - Default HPC platform scheduler version.
    * - **SCRATCH_DIR**
      - Default HPC platform scratch folder path.
    * - **HPCROOTDIR**
      - Default HPC platform experiment's folder path.

Other variables
=================

.. autosubmit-variables:: config


.. autosubmit-variables:: default


.. autosubmit-variables:: experiment


.. autosubmit-variables:: project


.. note::

    Depending on your project type other variables may
    be available. For example, if you choose Git, then
    you should have ``%PROJECT_ORIGIN%``. If you choose
    Subversion, then you will have ``%PROJECT_URL%``.


Performance Metrics variables
=============================

These variables apply only to the :ref:`report <report>` subcommand.

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Variable
      - Description
    * - **ASYPD**
      - Actual simulated years per day.
    * - **CHSY**
      - Core hours per simulated year.
    * - **JPSY**
      - Joules per simulated year.
    * - **Parallelization**
      - Number of cores requested for the simulation job.
    * - **RSYPD**
      - Raw simulated years per day.
    * - **SYPD**
      - Simulated years per day.


.. FIXME: this link is broken, and should probably not be under wuruchi's
..        gitlab account.
.. For more information about these metrics please visit
.. https://earth.bsc.es/gitlab/wuruchi/autosubmitreact/-/wikis/Performance-Metrics.

