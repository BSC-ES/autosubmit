Platforms
=====================

Extend an existing platform
------------------------------------

Platforms are defined under python classes. To extend an existing platofrm, ...

How to configure SLURM Platforms
------------------------------------

To set up your SLURM platform you first have to create a new experiment by running the following command creating a
minimal version of a experiment and configure the experiment platform to Marenostrum.

``autosubmit expid -H MARENOSTRUM5 -d "SLURM test" --minimal``

You'll have to insert the **PARAMETERS** to make your experiment work properly as an exemple the following
instruction are thought to execute a small job through Autosubmit explaining how to configure a platform.

First create a new folder at the root ``~/Autosubmit`` called project executing the following command:

``mkdir -p ~/autosubmit/project``

.. hint::
    The given name of the folder can be any as long as it matches the ``Local`` Parameter, the change in name
    needs to take this into account

For the execution of this test a few files will need to be created within the new folder,
this file will have the SLURM commands to be executed

.. code-block:: yaml

    LOCAL_SETUP.sh
    SYNCHRONIZE.sh
    REMOTE_SETUP.sh
    INI.sh
    DATA_NOTIFIER.sh
    SIM.sh
    STATISTICS.sh
    APP.sh
    CLEAN.sh

For sake of keeping and concise and clear exemple of how AutoSubmit works a simple instruction can be executed.
For full developed experiments this will be the instructions used in your experiment.

.. code-block:: yaml

    sleep 5

Once the new folder and files were created, open the file ``<expid>/config/jobs_<expid>.yml`` and you'll have a
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

Now we start configuring the experiment adding the additional ``PARAMETERS`` as shown bellow
to create a simple executable experiment

.. code-block:: yaml

    EXPERIMENT:
        DATELIST: 19900101
        MEMBERS: fc0
        CHUNKSIZEUNIT: month
        SPLITSIZEUNIT: day
        CHUNKSIZE: 1
        NUMCHUNKS: 2
        CALENDAR: standard


Add the following PARAMETER after, this will point towards the folder containing all the Platform instructions

.. code-block:: yaml

    LOCAL:
        PROJECT_PATH: ~/autosubmit/project

The following setting are used towards creating a connection with a platform to execute the jobs,
you have to input the information suitable for your project. (e.g.: user, host, platform)

.. warning::
    In case of not being able to connect it can be either because your user don't have access to the host
    or the PARAMETER ``SCRATCH_DIR`` might be pointing to a non existing folder inside the host.

    Make sure to have created the folder with your USERNAME inside the proper path you pointed to
    (e.g.: <Project_Dir>/<Project_Name_Folder>/<USER>)

.. code-block:: yaml

    PLATFORMS:
        MARENOSTRUM5:
            TYPE: <Platform_Type>
            HOST: <Host>
            PROJECT: <Project_Name_Folder>
            USER: <User>
            QUEUE: gp_debug
            SCRATCH_DIR: <Project_Dir>
            ADD_PROJECT_TO_HOST: false
            MAX_WALLCLOCK: 02:00
            TEMP_DIR: ''

        MARENOSTRUM_ARCHIVE:
            TYPE: <Platform_Type>
            HOST: <Host>
            PROJECT: <Project_Name_Folder>
            USER: <User>
            SCRATCH_DIR: <Project_Dir>
            ADD_PROJECT_TO_HOST: false
            MAX_WALLCLOCK: 02:00
            TEMP_DIR: ''

Now you can add jobs at the end of the file to see the execution

.. code-block:: yaml

    JOBS:
        LOCAL_SETUP:
            FILE: LOCAL_SETUP.sh
            PLATFORM: LOCAL
            RUNNING: once

        SYNCHRONIZE:
            FILE: SYNCHRONIZE.sh
            PLATFORM: LOCAL
            DEPENDENCIES: LOCAL_SETUP
            RUNNING: once
            WALLCLOCK: 00:05

        REMOTE_SETUP:
            FILE: REMOTE_SETUP.sh
            PLATFORM: LOCAL
            DEPENDENCIES: SYNCHRONIZE
            WALLCLOCK: 00:05
            RUNNING: once

        INI:
            FILE: INI.sh
            PLATFORM: LOCAL
            DEPENDENCIES: REMOTE_SETUP
            RUNNING: once
            WALLCLOCK: 00:05

        DATA_NOTIFIER:
            FILE: DATA_NOTIFIER.sh
            PLATFORM: LOCAL
            DEPENDENCIES: INI
            RUNNING: chunk

        SIM:
            FILE: SIM.sh
            PLATFORM: LOCAL
            DEPENDENCIES: DATA_NOTIFIER
            RUNNING: chunk

        STATISTICS:
            FILE: STATISTICS.sh
            PLATFORM: LOCAL
            DEPENDENCIES: SIM
            RUNNING: chunk

        APP:
            FILE: APP.sh
            PLATFORM: LOCAL
            DEPENDENCIES: STATISTICS
            RUNNING: chunk

        CLEAN:
            FILE: CLEAN.sh
            PLATFORM: LOCAL
            DEPENDENCIES: APP SIM STATISTICS
            RUNNING: once
            WALLCLOCK: 00:05

As you finish to set up all the new configuration you can run the following command to generate the experiment
that was just created

``autosubmit create -np -f -v <EXPID>``

Once the experiment is generated we can execute it and check its results by running the command bellow to execute
the experiment and check if its behaviour is as expected

``autosubmit run <EXPID>``
