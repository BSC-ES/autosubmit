Configure Platforms
=====================

The AutoSubmit can connect and make use of different Platforms of different types


Configure Slurm Platforms
----------------------------

You can create a new experiment by running the following command creating a minimal version of of a experiment
and configure the experiment platform to Marenostrum.

``autosubmit expid -H MARENOSTRUM5 -d "SLURM test" --minimal``

You'll have to insert the PARAMETERS to make you experiment work properly as an exemple the following
instruction are thought to execute a small job in MN5 through AS.

First create a new folder at the root called project:

``mkdir -p ~/autosubmit/project``

And create a new file called test.sh

.. code-block:: yaml

    echo "Hello World!"

Once the new folder and file are created, open the file ``config/jobs_<expid>.yml`` inside the experiment folder.

.. code-block:: yaml

    CONFIG:
        AUTOSUBMIT_VERSION: "4.1.12"
        TOTALJOBS: 20
        MAXWAITINGJOBS: 20
    DEFAULT:
        EXPID: <EXPID> # ID of the experiment
        HPCARCH: "MARENOSTRUM5"
        #hint: use %PROJDIR% to point to the project folder (where the project is cloned)
        CUSTOM_CONFIG: "%PROJDIR%/"
    PROJECT:
        PROJECT_TYPE: git
        PROJECT_DESTINATION: git_project
    GIT:
        PROJECT_ORIGIN: ""
        PROJECT_BRANCH: ""
        PROJECT_COMMIT: ''
        PROJECT_SUBMODULES: ''
        FETCH_SINGLE_BRANCH: true

You should add the ``CUSTOM_TAG`` adding the following information at the top of the file

.. code-block:: yaml

    TEST:
        JOB: &job
            SCRIPT: test
        PLATFORM: &platform
            USER: edoria
            PROJECT: bsc32

You can configure the experiment adding the following information under ``DEFAULT``

.. code-block:: yaml

    EXPERIMENT:
      DATELIST: 19900101
      MEMBERS: fc0
      CHUNKSIZEUNIT: month
      SPLITSIZEUNIT: day
      CHUNKSIZE: 1
      NUMCHUNKS: 1
      CALENDAR: standard


You can add the following tag after the TAG ``PROJECT``

.. code-block:: yaml

    LOCAL:
      PROJECT_PATH: ~/autosubmit/project

Adding configuration and adding the platforms will allow you to connect and execute the jobs
.. code-block:: yaml

    PLATFORMS:
      MARENOSTRUM5:
        <<: *platform
        TYPE: slurm
        HOST: glogin1.bsc.es, glogin2.bsc.es
        QUEUE: gp_debug
        SCRATCH_DIR: /tmp/scratch
        ADD_PROJECT_TO_HOST: false
        MAX_WALLCLOCK: 02:00
        TEMP_DIR: ''
        MAX_PROCESSORS: 99999
        PROCESSORS_PER_NODE: 112

      MARENOSTRUM5-login:
        <<: *platform
        TYPE: slurm
        HOST: glogin1.bsc.es, glogin2.bsc.es
        SCRATCH_DIR: /tmp/scratch
        ADD_PROJECT_TO_HOST: false
        MAX_WALLCLOCK: 02:00
        TEMP_DIR: ''
        MAX_PROCESSORS: 99999

Now you can add jobs at the end of the file to see the execution

.. code-block:: yaml

    JOBS:
      LOCAL_SETUP:
        <<: *job
        CHECK: on_submission
        PLATFORM: MARENOSTRUM5
        RUNNING: once

      SYNCHRONIZE:
        <<: *job
        CHECK: on_submission
        DEPENDENCIES:
          LOCAL_SETUP: {}
        PLATFORM: MARENOSTRUM5
        RUNNING: once

After setting up all the new configuration you can run the following command to create the plots

``autosubmit create -np -f -v a043``

At the end you can run the last command to execute the experiment and check its behaviour

``autosubmit run a043``