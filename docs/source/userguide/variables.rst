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
directives for your batch server. For example, ``THREADS`` will
be set in a Slurm platform as ``--SBATCH --cpus-per-task=$THREADS``.

However, the variables in Autosubmit do not contain all the directives
available in each platform like Slurm. For values that do not have a
direct variable, you can use ``CUSTOM_DIRECTIVES`` to define them in
your target platform. For instance, to set the number of GPU's in a Slurm
job, you can use ``CUSTOM_DIRECTIVES=--gpus-per-node=10``.


Platform variables
==================

Platform variables come from the ``PLATFORMS``section of the configuration
files. Each ``JOBS`` entry can reference any of these platforms via its
``PLATFORM`` key. If no platform is specified, the job uses the experiment's
default platform (``DEFAULT.HPCARCH``).

There are three ways platform variables reach your job templates: raw keys, 
``HPC``-prefixed keys, and ``CURRENT_``-prefixed keys.

Example configuration
---------------------

Consider the following experiment configuration:

.. code-block:: yaml

    DEFAULT:
      EXPID: a000
      HPCARCH: PS

    PLATFORMS:
      MARENOSTRUM5:
        TYPE: slurm
        HOST: glogin1.bsc.es, glogin2.bsc.es
        USER: root
        PROJECT: bsc32
        SCRATCH_DIR: /gpfs/scratch
      SLURM:
        TYPE: slurm
        HOST: slurm-test
        USER: root
        PROJECT: group
        SCRATCH_DIR: /tmp/scratch
      PS:
        TYPE: pbs
        HOST: ps-test
        USER: root
        PROJECT: bsc-es
        SCRATCH_DIR: /tmp/scratch


    JOBS:
      SIM:
        FILE: sim.sh
        RUNNING: once
        PLATFORM: MARENOSTRUM5
      POST:
        FILE: post.sh
        RUNNING: once
        PLATFORM: SLURM
      LOCAL_JOB:
        FILE: local_test.sh
        RUNNING: once
         # No PLATFORM key, uses DEFAULT.HPCARCH (PS)

In this example:

* ``SIM``uses ``MARENOSTRUM5`` as its platform, so it will have access to all the keys under
  ``PLATFORMS.MARENOSTRUM5``.
* ``POST`` uses ``SLURM`` as its platform, so it will have access to all the keys under
  ``PLATFORMS.SLURM``.
* ``LOCAL_JOB`` does not specify a platform, so it will use the default platform
  ``DEFAULT.HPCARCH`` (``PS``). It will have access to all the keys under ``PLATFORMS.PS``.

---

**1. Raw platform keys (global)**

Every key under a platform is available globally as ``PLATFORMS.<PLATFORM_ID>.<KEY>``.
You can reference them in templates as ``%PLATFORMS.MARENOSTRUM5.HOST%`` for example.

In the output of :ref:`report <report>` you will find them with the pattern 
``PLATFORMS.<PLATFORM_ID>.<KEY>=<VALUE>``.

.. code-block:: text
    :caption: Excerpt from ``autosubmit report a000 -all``

    PLATFORMS.MARENOSTRUM5.TYPE=slurm
    PLATFORMS.MARENOSTRUM5.HOST=glogin1.bsc.es, glogin2.bsc.es
    PLATFORMS.MARENOSTRUM5.USER=root
    PLATFORMS.MARENOSTRUM5.PROJECT=bsc32
    PLATFORMS.MARENOSTRUM5.SCRATCH_DIR=/gpfs/scratch
    PLATFORMS.SLURM.TYPE=slurm
    ...

---

**2. HPC prefixed keys (default platform, global)**

Every key under the **default** platform (``DEFAULT.HPCARCH``) is available
with an ``HPC`` prefix. These are also global, as they have the same value for every job.

From the example above (default platform is ``PS``):

.. list-table::
    :widths: 25 25 50
    :header-rows: 1

    * - Template variable
      - Source key
      - Value
    * - ``%HPCHOST%``
      - ``HOST``
      - ``ps-test``
    * - ``%HPCUSER%``
      - ``USER``
      - ``root``
    * - ``%HPCPROJECT%``
      - ``PROJECT``
      - ``bsc-es``
    * - ``%HPCSCRATCH_DIR%``
      - ``SCRATCH_DIR``
      - ``/tmp/scratch``

.. note::

    The key name is used as-is. E.g.``HPCPROJECT`` (not ``HPCPROJ``).
    If you add a custom key to your platform (e.g. ``GPU_MODEL: A100``),
    it becomes available as ``%HPCGPU_MODEL%``.

In addition, Autosubmit automatically computes the following derived variables:

.. list-table::
    :widths: 25 75
    :header-rows: 1

    * - Variable
      - Description
    * - **HPCARCH**
      - Name of the default platform (``PS``).
    * - **HPCROOTDIR**
      - ``<SCRATCH_DIR>/<PROJECT>/<USER>/<EXPID>``
        e.g. ``/tmp/scratch/bsc-es/root/a000``.
    * - **HPCLOGDIR**
      - ``<HPCROOTDIR>/LOG_<EXPID>``
        e.g. ``/tmp/scratch/bsc-es/root/a000/LOG_a000``.

.. code-block:: text
    :caption: Excerpt from ``autosubmit report a000 -all``

    HPCARCH=PS
    HPCHOST=ps-test
    HPCLOGDIR=/tmp/scratch/bsc-es/root/a000/LOG_a000
    HPCPROJECT=bsc-es
    HPCROOTDIR=/tmp/scratch/bsc-es/root/a000
    HPCSCRATCH_DIR=/tmp/scratch
    HPCUSER=root

---

**3. ``CURRENT_`` prefixed keys (per-job)**

Each job has access to the keys of its own platform (the one it runs on)
with a ``CURRENT_`` prefix. This means different jobs may have different
values for the same ``CURRENT_<KEY>`` variable.

Continuing with the example above:

.. list-table::
    :widths: 25 25 25 25
    :header-rows: 1

    * - Template variable
      - In ``SIM``
      - In ``POST``
      - In ``LOCAL_JOB``
    * - ``%CURRENT_HOST%``
      - ``glogin1.bsc.es, glogin2.bsc.es``
      - ``slurm-test``
      - ``ps-test``
    * - ``%CURRENT_USER%``
      - ``root``
      - ``root``
      - ``root``
    * - ``%CURRENT_PROJECT%``
      - ``bsc32``
      - ``group``
      - ``bsc-es``
    * - ``%CURRENT_SCRATCH_DIR%``
      - ``/gpfs/scratch``
      - ``/tmp/scratch``
      - ``/tmp/scratch``

.. code-block:: text
    :caption: Excerpt from ``autosubmit report a000 -all``

    JOBS.LOCAL_JOB.CURRENT_HOST=ps-test
    JOBS.LOCAL_JOB.CURRENT_PROJECT=bsc-es
    JOBS.POST.CURRENT_HOST=slurm-test
    JOBS.POST.CURRENT_PROJECT=group
    JOBS.SIM.CURRENT_HOST=glogin1.bsc.es, glogin2.bsc.es
    JOBS.SIM.CURRENT_PROJECT=bsc32

The ``CURRENT_`` prefix is also populated from the job's section keys 
(``JOBS.<section>``). This allows you to inject job-specific information 
into your templates.

**Auto-generated ``CURRENT_`` variables**

Below is a list of the auto-generated ``CURRENT_`` variables that are available in each job.
These are always available regardless of the raw YAML keys defined.

.. autosubmit-variables:: platform


.. note::
    The variables ``CURRENT_USER``, ``CURRENT_PROJ`` and ``CURRENT_BUDG``
    have no value on the LOCAL platform.

    Certain variables (e.g. ``CURRENT_RESERVATION``,
    ``CURRENT_EXCLUSIVITY``) are only available for certain
    platforms (e.g. MareNostrum).

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

