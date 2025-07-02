#########
Changelog
#########

This page shows the main changes from AS3 to AS4.

Mayor mentions:

- Python version has changed to 3.7.3 instead of 2.7.
- Configuration language has changed to YAML.

 - All parameters are now unified into a single dictionary.
 - All sections are now uppercase.
 - All parameters, except for job related ones, have now an hierarchy.
 - An special key, FOR:, has been added. This key allows to create multiple jobs with almost the same configuration.
 - The configuration of autosubmit is now more flexible.

- New command added, upgrade. This command will update all the scripts and autosubmit configuration.
- Wrapper definition has changed.
- Tasks dependencies system has changed.
- Added the parameter DELETE_WHEN_EDGELESS ( boolean ) to the section JOBS. This parameter allows to delete a job when it has no edges. ( default TRUE)

.. warning::
    The configuration language has changed. Please, check the new configuration file format.

.. warning::
    The wrapper definition has changed. Please, check the new wrapper definition.

.. warning::
    The tasks dependencies system has changed. Please, check the new tasks dependencies system.

.. warning::
    Edgeless jobs are now deleted by default. Please, check the new parameter DELETE_WHEN_EDGELESS.

.. warning:: upgrade may not translate all the scripts, we recommend to revise your scripts before run AS.

Configuration changes
=====================

Now autosubmit is composed by two kind of YAML configurations, the default ones, which are the same as always, and the custom ones.

The custom ones, allows to define custom configurations that will override the default ones, in order to do this, you only have to put the key in the custom configuration file.
These custom ones, can be anywhere and have any name, by default they're inside `<EXPID>/conf` but you can change this path in the expdef_<EXPID>.yml file. `DEFAULT.CUSTOM_CONFIG`

Additionally, you must be aware of the following changes:

 - All sections **keys** are normalized to **UPPERCASE**, while values remain as the user put. Beware of the scripts that relies on %CURRENT_HPCARCH% and variables that refer to a platform because they will be always in UPPERCASE. Normalize the script.
 - To define a job, you must put them under the key `jobs` in any custom configuration file.
 - To define a platform, you must put them under the key `platforms` in any custom configuration file.
 - To define a loop, you must put the key "FOR" as the first key of the section.
 - You can put any %placeholder% in the proj_<EXPID>.yml and custom files, and also you can put %ROOTDIR% in the expdef_<EXPID>.yml.
 - All configuration is now based in an hierarchical structure, so to export a var, you must use the following syntax: `%KEY.SUBKEY.SUBSUBKEY%`. The same goes for override them.
 - YAML has into account the type.

Examples
========

List of example with the new configuration and the structure as follows

.. code-block:: bash

    $/autosubmit/a00q/conf$ ls
    autosubmit_a00q.yml  custom_conf  expdef_a00q.yml  jobs_a00q.yml  platforms_a00q.yml
    $/autosubmit/a00q/conf/custom_conf ls
    more_jobs.yml

Configuration
=============

    `autosubmit_<EXPID>.yml`

    .. code-block:: yaml

        config:
          AUTOSUBMIT_VERSION: 4.0.0b
          MAXWAITINGJOBS: '3000'
          TOTALJOBS: '3000'
          SAFETYSLEEPTIME: 0
          RETRIALS: '10'
        mail:
          NOTIFICATIONS: 'False'
          TO: daniel.beltran@bsc.es

    `expdef_<EXPID>.yml`

    .. code-block:: yaml

        DEFAULT:
          EXPID: a02u
          HPCARCH: local
          CUSTOM_CONFIG: "%ROOTDIR%/conf/custom_conf"
        experiment:
          DATELIST: '20210811'
          MEMBERS: CompilationEfficiency HardwareBenchmarks WeakScaling StrongScaling
          CHUNKSIZEUNIT: hour
          CHUNKSIZE: '6'
          NUMCHUNKS: '2'
          CALENDAR: standard
        rerun:
          RERUN: 'FALSE'
          CHUNKLIST: ''
        project:
          PROJECT_TYPE: local
          PROJECT_DESTINATION: r_test
        git:
          PROJECT_ORIGIN: https://earth.bsc.es/gitlab/ces/automatic_performance_profiling.git
          PROJECT_BRANCH: autosubmit-makefile1
          PROJECT_COMMIT: ''
        svn:
          PROJECT_URL: ''
          PROJECT_REVISION: ''
        local:
          PROJECT_PATH: /home/dbeltran/r_test
        project_files:
          FILE_PROJECT_CONF: ''
          FILE_JOBS_CONF: ''

    `jobs_<EXPID>.yml`

    .. code-block:: yaml

        JOBS:
          LOCAL_SETUP:
            FILE: LOCAL_SETUP.sh
            PLATFORM: LOCAL
            RUNNING: "once"
          REMOTE_SETUP:
            FILE: REMOTE_SETUP.sh
            DEPENDENCIES: LOCAL_SETUP
            WALLCLOCK: '00:05'
            RUNNING: once
            NOTIFY_ON: READY SUBMITTED QUEUING COMPLETED
          INI:
            FILE: INI.sh
            DEPENDENCIES: REMOTE_SETUP
            RUNNING: member
            WALLCLOCK: '00:05'
            NOTIFY_ON: READY SUBMITTED QUEUING COMPLETED

          SIM:
            FOR:
              NAME: [20,40,80]
              PROCESSORS: [2,4,8]
              THREADS: [1,1,1]
              DEPENDENCIES: [INI SIM_20-1 CLEAN-2, INI SIM_40-1 CLEAN-2, INI SIM_80-1 CLEAN-2]
              NOTIFY_ON: READY SUBMITTED QUEUING COMPLETED

            FILE: SIM.sh
            DEPENDENCIES: INI SIM_20-1 CLEAN-2
            RUNNING: chunk
            WALLCLOCK: '00:05'
            TASKS: '1'
            NOTIFY_ON: READY SUBMITTED QUEUING COMPLETED

          POST:
            FOR:
              NAME: [ 20,40,80 ]
              PROCESSORS: [ 20,40,80 ]
              THREADS: [ 1,1,1 ]
              DEPENDENCIES: [ SIM_20 POST_20-1,SIM_40 POST_40-1,SIM_80 POST_80-1 ]
            FILE: POST.sh
            RUNNING: chunk
            WALLCLOCK: '00:05'
          CLEAN:
            FILE: CLEAN.sh
            DEPENDENCIES: POST_20 POST_40 POST_80
            RUNNING: chunk
            WALLCLOCK: '00:05'
          TRANSFER:
            FILE: TRANSFER.sh
            PLATFORM: LOCAL
            DEPENDENCIES: CLEAN
            RUNNING: member

    `platforms_<EXPID>.yml`

    .. code-block:: yaml

        Platforms:
          MaReNoStRuM4:
            TYPE: slurm
            HOST: bsc
            PROJECT: bsc32
            USER: bsc32070
            QUEUE: debug
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: False
            MAX_WALLCLOCK: '48:00'
            USER_TO: pr1enx13
            TEMP_DIR: ''
            SAME_USER: False
            PROJECT_TO: pr1enx00
            HOST_TO: bscprace
          marenostrum_archive:
            TYPE: ps
            HOST: dt02.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: 'False'
            TEST_SUITE: 'False'
            USER_TO: pr1enx13
            TEMP_DIR: /gpfs/scratch/bsc32/bsc32070/test_migrate
            SAME_USER: false
            PROJECT_TO: pr1enx00
            HOST_TO: transferprace
          transfer_node:
            TYPE: ps
            HOST: dt01.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            ADD_PROJECT_TO_HOST: false
            SCRATCH_DIR: /gpfs/scratch
            USER_TO: pr1enx13
            TEMP_DIR: /gpfs/scratch/bsc32/bsc32070/test_migrate
            SAME_USER: false
            PROJECT_TO: pr1enx00
            HOST_TO: transferprace
          transfer_node_bscearth000:
            TYPE: ps
            HOST: bscearth000
            USER: dbeltran
            PROJECT: Earth
            ADD_PROJECT_TO_HOST: false
            QUEUE: serial
            SCRATCH_DIR: /esarchive/scratch
            USER_TO: dbeltran
            TEMP_DIR: ''
            SAME_USER: true
            PROJECT_TO: Earth
            HOST_TO: bscpraceearth000
          bscearth000:
            TYPE: ps
            HOST: bscearth000
            USER: dbeltran
            PROJECT: Earth
            ADD_PROJECT_TO_HOST: false
            QUEUE: serial
            SCRATCH_DIR: /esarchive/scratch
          nord3:
            TYPE: SLURM
            HOST: nord1.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            QUEUE: debug
            SCRATCH_DIR: /gpfs/scratch
            MAX_WALLCLOCK: '48:00'
            USER_TO: pr1enx13
            TEMP_DIR: ''
            SAME_USER: true
            PROJECT_TO: pr1enx00
          ecmwf-xc40:
            TYPE: ecaccess
            VERSION: pbs
            HOST: cca
            USER: c3d
            PROJECT: spesiccf
            ADD_PROJECT_TO_HOST: false
            SCRATCH_DIR: /scratch/ms
            QUEUE: np
            SERIAL_QUEUE: ns
            MAX_WALLCLOCK: '48:00'

    `custom_conf/more_jobs.yml`

    .. code-block:: yaml

        jobs:
          Additional_job_1:
            FILE: extrajob.sh
            DEPENDENCIES: POST_20
            RUNNING: once
          additional_job_2:
            FILE: extrajob.sh
            RUNNING: once


Wrappers definition
===================

To define a the wrappers:

.. code-block:: yaml

    wrappers:
      wrapper_sim20:
        TYPE: "vertical"
        JOBS_IN_WRAPPER: "SIM_20"
      wrapper_sim40:
        TYPE: "vertical"
        JOBS_IN_WRAPPER: "SIM_40"

Loops definition
================

To define a loop, you need to use the FOR key and also the NAME key.

In order to generate the following jobs:

.. code-block:: yaml

    experiment:
      DATELIST: 19600101
      MEMBERS: "00"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '2'
      CALENDAR: standard
    JOBS:
      POST_20:

        DEPENDENCIES:
          POST_20:
          SIM_20:
        FILE: POST.sh
        PROCESSORS: '20'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05
      POST_40:

        DEPENDENCIES:
          POST_40:
          SIM_40:
        FILE: POST.sh
        PROCESSORS: '40'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05
      POST_80:

        DEPENDENCIES:
          POST_80:
          SIM_80:
        FILE: POST.sh
        PROCESSORS: '80'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05
      SIM_20:

        DEPENDENCIES:
          SIM_20-1:
        FILE: POST.sh
        PROCESSORS: '20'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05
      SIM_40:

        DEPENDENCIES:
          SIM_40-1:
        FILE: POST.sh
        PROCESSORS: '40'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05
      SIM_80:

        DEPENDENCIES:
          SIM_80-1:
        FILE: POST.sh
        PROCESSORS: '80'
        RUNNING: chunk
        THREADS: '1'
        WALLCLOCK: 00:05

One can use now the following configuration:

.. code-block:: yaml

    experiment:
      DATELIST: 19600101
      MEMBERS: "00"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '2'
      CALENDAR: standard
    JOBS:
      SIM:
        FOR:
          NAME: [ 20,40,80 ]
          PROCESSORS: [ 20,40,80 ]
          THREADS: [ 1,1,1 ]
          DEPENDENCIES: [ SIM_20-1,SIM_40-1,SIM_80-1 ]
        FILE: POST.sh
        RUNNING: chunk
        WALLCLOCK: '00:05'
      POST:
          FOR:
            NAME: [ 20,40,80 ]
            PROCESSORS: [ 20,40,80 ]
            THREADS: [ 1,1,1 ]
            DEPENDENCIES: [ SIM_20 POST_20,SIM_40 POST_40,SIM_80 POST_80 ]
          FILE: POST.sh
          RUNNING: chunk
          WALLCLOCK: '00:05'

.. warning:: Only the parameters that changes must be included inside the `FOR` key.

Dependencies rework
===================

The DEPENDENCIES key is used to define the dependencies of a job. It can be used in the following ways:

- Basic: The dependencies are a list of jobs, separated by " ", that runs before the current task is submitted.
- New: The dependencies is a list of YAML sections, separated by "\n", that runs before the current job is submitted.

    - For each dependency section, you can designate the following keywords to control the current job-affected tasks:

        - DATES_FROM: Selects the job dates that you want to alter.
        - MEMBERS_FROM: Selects the job members that you want to alter.
        - CHUNKS_FROM: Selects the job chunks that you want to alter.

    - For each dependency section and \*_FROM keyword, you can designate the following keywords to control the destination of the dependency:

        - DATES_TO: Links current selected tasks to the dependency tasks of the dates specified.
        - MEMBERS_TO: Links current selected tasks to the dependency tasks of the members specified.
        - CHUNKS_TO: Links current selected tasks to the dependency tasks of the chunks specified.

    - Important keywords for [DATES|MEMBERS|CHUNKS]_TO:

        - "natural": Will keep the default linkage. Will link if it would be normally. Example, SIM_FC00_CHUNK_1 -> DA_FC00_CHUNK_1.
        - "all": Will link all selected tasks of the dependency with current selected tasks. Example, SIM_FC00_CHUNK_1 -> DA_FC00_CHUNK_1, DA_FC00_CHUNK_2, DA_FC00_CHUNK_3...
        - "none": Will unlink selected tasks of the dependency with current selected tasks.

For the new format, consider that the priority is hierarchy and goes like this DATES_FROM -(includes)-> MEMBERS_FROM -(includes)-> CHUNKS_FROM.

- You can define a DATES_FROM inside the DEPENDENCY.
- You can define a MEMBERS_FROM inside the DEPENDENCY and DEPENDENCY.DATES_FROM.
- You can define a CHUNKS_FROM inside the DEPENDENCY, DEPENDENCY.DATES_FROM, DEPENDENCY.MEMBERS_FROM, DEPENDENCY.DATES_FROM.MEMBERS_FROM

For the examples, we will consider that our experiment has the following configuration:

.. code-block:: yaml

    EXPERIMENT:
        DATELIST: 20220101
        MEMBERS: FC1 FC2
        NUMCHUNKS: 4

Basic
=====

.. code-block:: yaml

  JOBS:
    JOB_1:
        FILE: job1.sh
        RUNNING: chunk
    JOB_2:
        FILE: job2.sh
        DEPENDENCIES: JOB_1
        RUNNING: chunk
    JOB_3:
        FILE: job3.sh
        DEPENDENCIES: JOB_2
        RUNNING: chunk
    SIM:
        FILE: sim.sh
        DEPENDENCIES: JOB_3 SIM-1
        RUNNING: chunk
    POST:
        FILE: post.sh
        DEPENDENCIES: SIM
        RUNNING: chunk
    TEST:
        FILE: test.sh
        DEPENDENCIES: POST
        RUNNING: chunk

New format
==========

.. code-block:: yaml

  JOBS:
    JOB_1:
        FILE: job1.sh
        RUNNING: chunk
    JOB_2:
        FILE: job2.sh
        DEPENDENCIES:
            JOB_1:
                dates_to: "natural"
                members_to: "natural"
                chunks_to: "natural"
        RUNNING: chunk
    JOB_3:
        FILE: job3.sh
        DEPENDENCIES:
            JOB_2:
                dates_to: "natural"
                members_to: "natural"
                chunks_to: "natural"
        RUNNING: chunk
    SIM:
        FILE: sim.sh
        DEPENDENCIES:
            JOB_3:
                dates_to: "natural"
                members_to: "natural"
                chunks_to: "natural"
            SIM-1:
                dates_to: "natural"
                members_to: "natural"
                chunks_to: "natural"
        RUNNING: chunk
    POST:
        FILE: post.sh
        DEPENDENCIES:
            SIM:
                dates_to: "natural"
                members_to: "natural"
                chunks_to: "natural"
        RUNNING: chunk
    TEST:
        FILE: test.sh
        DEPENDENCIES:
            POST:
                dates_to: "natural"
                members_to: "natural"
                chunks_to: "natural"
        RUNNING: chunk

.. figure:: fig/new_dependencies_0.png
   :name: new_dependencies_0
   :align: center
   :alt: new_dependencies

Example 1: New format with specific dependencies
------------------------------------------------


In the following example, we want to launch the next member SIM after the last SIM chunk of the previous member is finished.


.. code-block:: yaml

    JOBS:
        JOB_1:
            FILE: job1.sh
            RUNNING: chunk
        JOB_2:
            FILE: job2.sh
            DEPENDENCIES:
                JOB_1:
            RUNNING: chunk
        JOB_3:
            FILE: job3.sh
            DEPENDENCIES:
                JOB_2:
            RUNNING: chunk
        SIM:
            FILE: sim.sh
            DEPENDENCIES:
                JOB_3:
                SIM-1:
                SIM:
                    MEMBERS_FROM:
                      FC2:
                        CHUNKS_FROM:
                         1:
                          dates_to: "all"
                          members_to: "FC1"
                          chunks_to: "4"
            RUNNING: chunk
        POST:
            FILE: post.sh
            DEPENDENCIES:
                SIM:
            RUNNING: chunk
        TEST:
            FILE: test.sh
            DEPENDENCIES:
                POST:
                  members_to: "FC2"
                  chunks_to: 4
            RUNNING: once

.. figure:: fig/new_dependencies_1.png
   :name: new_dependencies_1
   :align: center
   :alt: new_dependencies

Example 2: Crossdate wrappers using the the new dependencies
------------------------------------------------------------

.. code-block:: yaml

    experiment:
      DATELIST: 20120101 20120201
      MEMBERS: "000 001"
      CHUNKSIZEUNIT: day
      CHUNKSIZE: '1'
      NUMCHUNKS: '3'
    wrappers:
        wrapper_simda:
            TYPE: "horizontal-vertical"
            JOBS_IN_WRAPPER: "SIM DA"

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

.. figure:: fig/monarch-da.png
   :name: crossdate-example
   :align: center
   :alt: crossdate-example