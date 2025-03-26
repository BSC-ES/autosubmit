Configure Platforms
=====================

How to configure SLURM Platforms
------------------------------------

To set up your SLURM platform you first have to create a new experiment by running the following command creating a
minimal version of a experiment and configure the experiment platform to Marenostrum.

``autosubmit expid -H MARENOSTRUM5 -d "SLURM test" --minimal``

You'll have to insert the **PARAMETERS** to make you experiment work properly as an exemple the following
instruction are thought to execute a small job in **MN5** through Autosubmit.

First create a new folder at the root ``~/Autosubmit`` called project executing the following command:

``mkdir -p ~/autosubmit/project``

.. hint::
    The naming of the folder can be any given as long as the ``Local`` Parameter, explained bellow, change in
    accordance to the name

And create a new file called ``test.sh`` within the new folder, this file will have the SLURM commands to be executed

.. code-block:: yaml

    #! /usr/bin/bash
    #SBATCH --account=bsc32
    #SBATCH --qos=gp_debug
    #SBATCH --nodes=2
    #SBATCH --time=00:30:00

    sleep 5

Once the new folder and file are created, open the file ``<expid>/config/jobs_<expid>.yml`` and you'll have a
file as shown below.

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
        PROJECT_TYPE: local
        PROJECT_DESTINATION: local_project

    GIT:
        PROJECT_ORIGIN: ""
        PROJECT_BRANCH: ""
        PROJECT_COMMIT: ''
        PROJECT_SUBMODULES: ''
        FETCH_SINGLE_BRANCH: true

Once you confirm the file is properly structure try add the ``CUSTOM_TAG`` at the top of the file
The PARAMETER created will later embody the job and platform configuration, simplifying future fixes
and tests in multiple platforms

.. code-block:: yaml

    TEST:
        JOB: &job
            SCRIPT: test
        PLATFORM: &platform
            USER: <user> # User that have access to the platform
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


You can add the following PARAMETER after ``PROJECT``, this will point towards the file with SLURM instructions

.. code-block:: yaml

    LOCAL:
        PROJECT_PATH: ~/autosubmit/project

Adding configuration and adding the platforms will allow you to connect and execute the jobs, be mindful of using
your own user in this step and make sure you have a folder for your user.

.. hint::
    In case of not connecting it can be because your user dont have access to the host
    ``SCRATCH_DIR`` might be pointing to a non existing folder inside the host

.. code-block:: yaml

    PLATFORMS:
        MARENOSTRUM5:
            <<: *platform
            TYPE: slurm
            HOST: glogin1.bsc.es, glogin2.bsc.es
            QUEUE: gp_debug
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: false
            MAX_WALLCLOCK: 02:00
            TEMP_DIR: ''
            MAX_PROCESSORS: 99999

        MARENOSTRUM5-login:
            <<: *platform
            TYPE: slurm
            HOST: glogin1.bsc.es, glogin2.bsc.es
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: false
            MAX_WALLCLOCK: 02:00
            TEMP_DIR: ''

Now you can add jobs at the end of the file to see the execution

.. code-block:: yaml

    JOBS:
        LOCAL_SETUP:
            <<: *job
            CHECK: on_submission
            PLATFORM: MARENOSTRUM5
            RUNNING: once
            WALLCLOCK: 00:05

        SYNCHRONIZE:
            <<: *job
            CHECK: on_submission
            DEPENDENCIES:
                LOCAL_SETUP: {}
            PLATFORM: MARENOSTRUM5
            RUNNING: once

After setting up all the new configuration you can run the following command to create the plots

``autosubmit create -np -f -v <EXPID>``

At the end you can run the last command to execute the experiment and check its behaviour

``autosubmit run <EXPID>``