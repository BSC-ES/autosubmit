Wrappers
========

Job packages, or "wrappers", are jobs created as bundles of different tasks (submitted at once in a single script to the platform) assembled by Autosubmit to maximize the usage of platforms managed by a scheduler (by minimizing the queuing time between consecutive or concurrent tasks). Autosubmit supports four wrapper types that can be used depending on the experiment’s workflow.

* Horizontal_
* Vertical_
* Horizontal-vertical_
* Vertical-horizontal_

.. note:: To have a preview of wrappers, you must use the parameter `-cw` available on inspect, monitor, and create.

.. code-block:: bash

	autosubmit create  EXPID -cw  # Unstarted experiment
	autosubmit monitor EXPID -cw # Ongoing experiment
	autosubmit inspect EXPID -cw -f # Visualize wrapper cmds

Basic configuration
-------------------

To configure a new wrapper, the user has to define a `WRAPPERS` section in any configuration file. When using the standard configuration, this one is autosubmit_<EXPID>.yml.

.. code-block:: YAML

    WRAPPERS:
        WRAPPER_0:
            TYPE: "horizontal"

By default, Autosubmit will try to bundle jobs of the same type. The user can alter this behavior by setting the `JOBS_IN_WRAPPER` parameter directive in the wrapper section.

When using multiple wrappers or 2-dim wrappers is essential to define the `JOBS_IN_WRAPPER` parameter.

.. code-block:: YAML

    WRAPPERS:
      WRAPPER_H:
        TYPE: "horizontal"
        JOBS_IN_WRAPPER: "SIM"
      WRAPPER_V:
        TYPE: "vertical"
        JOBS_IN_WRAPPER: "SIM2"
      WRAPPER_VH:
        TYPE: "vertical-horizontal"
        JOBS_IN_WRAPPER: "SIM3 SIM4"
      WRAPPER_HV:
        TYPE: "horizontal-vertical"
        JOBS_IN_WRAPPER: "SIM5 SIM6"

    experiment:
      DATELIST: 20220101
      MEMBERS: "fc0 fc1"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '4'
      CALENDAR: standard
    JOBS:
        SIM:
          FILE: sim.sh
          RUNNING: chunk
          QUEUE: debug
          DEPENDENCIES: SIM-1
          WALLCLOCK: 00:15
        SIM2:
          FILE: sim.sh
          RUNNING: chunk
          QUEUE: debug
          DEPENDENCIES: SIM2-1
          WALLCLOCK: 00:15
        SIM3:
          FILE: sim.sh
          RUNNING: chunk
          QUEUE: debug
          DEPENDENCIES: SIM3-1
          WALLCLOCK: 00:15
        SIM4:
          FILE: sim.sh
          RUNNING: chunk
          QUEUE: debug
          DEPENDENCIES: SIM4-1
          WALLCLOCK: 00:15
        SIM5:
          FILE: sim.sh
          RUNNING: chunk
          QUEUE: debug
          DEPENDENCIES: SIM5-1
          WALLCLOCK: 00:15
        SIM6:
          FILE: sim.sh
          RUNNING: chunk
          QUEUE: debug
          DEPENDENCIES: SIM6-1
          WALLCLOCK: 00:15

.. figure:: fig/wrapper_all.png
   :name: wrapper all
   :align: center
   :alt: wrapper all

.. important:: Autosubmit will not wrap tasks with external and non-fulfilled dependencies.

Wrapper parameters description
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Type
^^^^

The type parameter allow the user to determine the wrapper algorithm. 

It affects tasks in wrapper order executions, and in hybrid cases, it adds some internal logic. 

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"

Jobs_in_wrapper
^^^^^^^^^^^^^^^

The jobs_in_wrapper parameter allow the user to determine the tasks inside a wrapper by giving the job_section name. It can group multiple tasks by providing more than one job_section name. 

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      

Method
^^^^^^

The method parameter allow the user to determine if the wrapper will use machine files or threads. 

This allows to form a wrapper with that relies on machinefiles to work.

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      METHOD: ASTHREAD

or 

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"

This allows to form a wrapper with shared-memory paradigm instead of rely in machinefiles to work in parallel.


.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      METHOD: SRUN

Extend_wallclock
^^^^^^^^^^^^^^^^

The extend_wallclock parameter allow the users to provide extra headroom for the wrapper. The accepted value is an integer. Autosubmit will translate this value automatically to the max_wallclock of the sum of wrapper inner-tasks wallclock at the horizontal level. 

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      extend_wallclock: 1

Retrials
^^^^^^^^

The retrials parameter allows the users to enable or disable the wrapper's retrial mechanism. This value overrides the general tasks defined. 

Vertical wrappers will retry the jobs without resubmitting the wrapper. 

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      RETRIALS: 2

Queue
^^^^^

The queue parameter allows the users to define a different queue for the wrapper. This value overrides the platform queue and job queue.

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      QUEUE: BSC_ES

Export
^^^^^^

The queue parameter allows the users to define a path to a script that will load environment scripts before running the wrapper tasks. This value overrides the job queue.

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
      EXPORT: "%CURRENT_ROOTDIR%/envmodules.sh"



Check_time_wrapper
^^^^^^^^^^^^^^^^^^

The CHECK_TIME_WRAPPER parameter defines the frequency, in seconds, on which Autosubmit will check the remote platform status of all the wrapper tasks. This affects all wrappers.

.. code-block:: YAML

  WRAPPERS:
    CHECK_TIME_WRAPPER: 10
    WRAPPER_0:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
    WRAPPER_1:
      TYPE: "vertical"
      JOBS_IN_WRAPPER: "SIM1"

Number of jobs in a wrapper({MIN/MAX}_WRAPPED{_H/_V}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Users can configure the maximum and the minimum number of jobs in each wrapper by configuring MAX_WRAPPED and MIN_WRAPPED inside the wrapper section. If the user doesn't set them, Autosubmit will default to MAX_WRAPPED: “infinite” and MIN_WRAPPED: 2.

.. code-block:: YAML

  WRAPPERS:
    MIN_WRAPPED: 2
    MAX_WRAPPED: 999999
    WRAPPER_0:
      MAX_WRAPPED: 2
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"
    WRAPPER_1:
      TYPE: "vertical"
      JOBS_IN_WRAPPER: "SIM1"

For 2-dim wrappers, {MAX_MIN}_WRAPPED_{V/H} must be used instead of the general one.

.. code-block:: YAML

  WRAPPERS:
   MIN_WRAPPED: 2
   MAX_WRAPPED: 999999
   WRAPPER_0:
    MAX_WRAPPED_H: 2
    MAX_WRAPPED_V: 4
    MIN_WRAPPED_H: 2
    MIN_WRAPPED_V: 2
    TYPE: "horizontal-vertical"
    JOBS_IN_WRAPPER: "SIM SIM1"

Policy
^^^^^^


Autosubmit will wrap as many tasks as possible while respecting the limits set in the configuration(MAX_WRAPPED, MAX_WRAPPED_H, MAX_WRAPPED_V, MIN_WRAPPED, MIN_WRAPPED_V, and MIN_WRAPPED_H parameters). However, users have three different policies available to tune the behavior in situations where there aren’t enough tasks in general, or there are uncompleted tasks remaining from a failed wrapper job:

* Flexible: if there aren’t at least MIN_WRAPPED tasks to be grouped, Autosubmit will submit them as individual jobs.
* Mixed: will wait for MIN_WRAPPED jobs to be available to create a wrapper, except if one of the wrapped tasks had failed beforehand. In this case, Autosubmit will submit them individually.
* Strict: will always wait for MIN_WRAPPED tasks to be ready to create a wrapper.


.. warning: Mixed and strict policies can cause deadlocks.

.. code-block:: YAML

  WRAPPERS:
    POLICY: "flexible"
    WRAPPER_0:
      TYPE: "vertical"
      JOBS_IN_WRAPPER: "SIM SIM1"

.. _Vertical:

Vertical wrapper
----------------

Vertical wrappers are suited for sequential dependent jobs (e.x. chunks of SIM tasks that depend on the previous chunk). Defining the platform’s  `MAX_WALLCLOCK` is essential since the wrapper's total wallclock time will be the sum of each job and will be a limiting factor for the creation of the wrapper, which will not bundle more jobs than the ones fitting in the wallclock time.

Autosubmit supports wrapping together vertically jobs of different types.

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_V:
      TYPE: "vertical"
      JOBS_IN_WRAPPER: "SIM"

.. figure:: fig/wrapper_v.png
   :name: wrapper vertical
   :align: center
   :alt: wrapper vertical

.. _Horizontal:

Horizontal wrapper
------------------

Horizontal wrappers are suited for jobs that must run parallel (e.x. members of SIM tasks). Defining the platform’s  `MAX_PROCESSORS` is essential since the wrapper processor amount will be the sum of each job and will be a limiting factor for the creation of the wrapper, which will not bundle more jobs than the ones fitting in the `MAX_PROCESSORS` of the platform.

.. code-block:: YAML

  WRAPPERS:
    WRAPPER_H:
      TYPE: "horizontal"
      JOBS_IN_WRAPPER: "SIM"


.. figure:: fig/wrapper_h.png
   :name: wrapper horizontal
   :align: center
   :alt: wrapper horizontal


.. _Vertical-horizontal:

Vertical-horizontal wrapper
---------------------------

The vertical-horizontal wrapper allows bundling together a vertical sequence of tasks independent of the horizontal ones. Therefore, all horizontal tasks do not need to finish to progress to the next horizontal level.

.. figure:: fig/wrapper_vh.png
   :name: wrapper vertical-horizontal
   :align: center
   :alt: wrapper vertical-horizontal


.. _Horizontal-vertical:

Horizontal-vertical wrapper
---------------------------

The horizontal-vertical wrapper allows bundling together tasks that could run simultaneously but need to communicate before progressing to the next horizontal level.


.. figure:: fig/wrapper_hv.png
   :name: wrapper horizontal-vertical
   :align: center
   :alt: wrapper horizontal-vertical



Advanced example: Set-up an crossdate wrapper
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Considering the following configuration:

.. code-block:: yaml

    experiment:
      DATELIST: 20120101 20120201
      MEMBERS: "000 001"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '3'

    JOBS:
      LOCAL_SETUP:
        FILE: templates/local_setup.sh
        PLATFORM: marenostrum_archive
        RUNNING: once
        NOTIFY_ON: COMPLETED
      LOCAL_SEND_SOURCE:
        FILE: templates/01_local_send_source.sh
        PLATFORM: marenostrum_archive
        DEPENDENCIES: LOCAL_SETUP
        RUNNING: once
        NOTIFY_ON: FAILED
      LOCAL_SEND_STATIC:
        FILE: templates/01b_local_send_static.sh
        PLATFORM: marenostrum_archive
        DEPENDENCIES: LOCAL_SETUP
        RUNNING: once
        NOTIFY_ON: FAILED
      REMOTE_COMPILE:
        FILE: templates/02_compile.sh
        DEPENDENCIES: LOCAL_SEND_SOURCE
        RUNNING: once
        PROCESSORS: '4'
        WALLCLOCK: 00:50
        NOTIFY_ON: COMPLETED
      SIM:
        FILE: templates/05b_sim.sh
        DEPENDENCIES:
          LOCAL_SEND_STATIC:
          REMOTE_COMPILE:
          SIM-1:
          DA-1:
        RUNNING: chunk
        PROCESSORS: '68'
        WALLCLOCK: 00:12
        NOTIFY_ON: FAILED
      LOCAL_SEND_INITIAL_DA:
        FILE: templates/00b_local_send_initial_DA.sh
        PLATFORM: marenostrum_archive
        DEPENDENCIES: LOCAL_SETUP LOCAL_SEND_INITIAL_DA-1
        RUNNING: chunk
        SYNCHRONIZE: member
        DELAY: '0'
      COMPILE_DA:
        FILE: templates/02b_compile_da.sh
        DEPENDENCIES: LOCAL_SEND_SOURCE
        RUNNING: once
        WALLCLOCK: 00:20
        NOTIFY_ON: FAILED
      DA:
        FILE: templates/05c_da.sh
        DEPENDENCIES:
          SIM:
          LOCAL_SEND_INITIAL_DA:
            CHUNKS_TO: "all"
            DATES_TO: "all"
            MEMBERS_TO: "all"
          COMPILE_DA:
          DA:
            DATES_FROM:
              "20120201":
                CHUNKS_FROM:
                  1:
                    DATES_TO: "20120101"
                    CHUNKS_TO: "1"
        RUNNING: chunk
        SYNCHRONIZE: member
        DELAY: '0'
        WALLCLOCK: 00:12
        PROCESSORS: '256'
        NOTIFY_ON: FAILED


.. code-block:: yaml

    wrappers:
      wrapper_simda:
        TYPE: "horizontal-vertical"
        JOBS_IN_WRAPPER: "SIM DA"

.. figure:: fig/monarch-da.png
   :name: crossdate-example
   :align: center
   :alt: crossdate-example
