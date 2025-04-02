Platforms
=========

Extending an existing platform
------------------------------

Platforms are defined under python classes. The source files for such classes are stored inside ``autosubmit/platforms/`` directory. To extend an existing platform we will create a child class from an existing platform class, for which first we need
to identify which existing platform is the most suitable for out project. 

Composing the extended platform class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this page we will be extedning the SLURM
platform - source file ``autosubmit/platforms/slurmplatform.py``, see in GitHub `slurmplatform.py <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/platforms/slurmplatform.py#L35>`_ -, but any platform can be extended by following the same steps.

The platform will be transcribing the files and configurations you set manually to allow operations,
and connection to SLURM and its commands, prepare your experiments to be executed transforming configuration
into executable function.

We will create a new file in ``/autosubmit/platforms/``
and we are going to call it ``example_platform.py``. 

.. code-block:: python
    :linenos:

    from autosubmit.platforms.slurmplatform import SlurmPlatform

    class Slurm_ExamplePlatform(SlurmPlatform):
        """ Class to manage slurm jobs """

This will Create a class in which the ``SlurmPlatform`` will be associated as its parent class allowing
``ExamplePlatform`` inherit all its characteristics.

We create an initialization method with the required parameters.

.. code-block:: python
    :linenos:

    def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
        """ Initialization of the Class ExamplePlatform """
        SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)
        self.example_platform_parameter = ...

As you can see the parent class has an initialization in order invoke all the parent`s methods and attributes into the
child (``ExamplePlatform``).
In oder to override methods from the parent class, we cna simply redefine them.

.. code-block:: python
    :linenos:

    def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
        """Submit a job from a given job object."""
        Log.result(f"Job: {job.name}")
        return None

The class ``submit_job`` is a existing class in ``SlurmPlatform`` that was overwritten to have a new behaviour.

After all needed modifications and expansions, the ``ExamplePlatform`` class could look similar to the following example code.

.. code-block:: python
    :linenos:

    from typing import Union
    from autosubmit.platforms.slurmplatform import SlurmPlatform

    class Slurm_ExamplePlatform(SlurmPlatform):
        """Class to manage slurm jobs"""
        def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
            """Initialization of the Class ExamplePlatform"""
            SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)

        def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
            """Submit a job from a given job object."""
            Log.result(f"Job: {job.name}")
            return None


Integrating the extended platform into the module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to ensure that the platform will be created as expected we need to make some changes in 4 different files ``autosubmit/autosubmit.py``, ``autosubmit/job/job.py``, ``autosubmit/platforms/ecplatform.py`` and ``atuosubmit/platforms/paramiko_submitter.py``:

.. warning::
   Line numbers might differ from the ones listed in this page

``autosubmit/autosubmit.py`` in `line 2537 <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/autosubmit.py#L2537>`_  add a new ``String`` making sure the new platform type is considered
the same as SLURM platform, as we expect a similar behaviour.

.. code-block:: python
   :emphasize-lines: 1 

    if platform.type.lower() in [ "slurm" , "pjm", "example" ] and not inspect and not only_wrappers:
                    # Process the script generated in submit_ready_jobs
                    save_2, valid_packages_to_submit = platform.process_batch_ready_jobs(valid_packages_to_submit,
                                                                                         failed_packages,
                                                                                         error_message="", hold=hold)

``autosubmit/job/job.py`` in `line 2575 <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/job/job.py#L2575>`_ making sure each Job writes
end timestamp to TOTAL_STATS file and jobs_data.db properly.

.. code-block:: python
   :emphasize-lines: 1

    if job_data_dc and type(self.platform) is not str and (self.platform.type == "slurm" or self.platform.type == "example"):
        thread_write_finish = Thread(target=ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR).write_platform_data_after_finish, args=(job_data_dc, self.platform))
            thread_write_finish.name = "JOB_data_{}".format(self.name)
            thread_write_finish.start()

``autosubmit/job/job.py`` in `line 2817 <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/job/job.py#L2817>`_ add a new validation for the validation of the queue
creation where the platform type

.. code-block:: python
    :emphasize-lines: 1

    if self._platform.type == 'slurm' or self._platform.type == 'example':
        self._platform.send_command(
            self._platform.get_queue_status_cmd(self.id))
        reason = self._platform.parse_queue_reason(
            self._platform._ssh_output, self.id)

``autosubmit/platforms/ecplatform.py`` in `line 59 <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/platforms/ecplatform.py#L59>`_ add a new validation for the header command
creation where the platform type

.. code-block:: python
    :emphasize-lines: 1

    elif scheduler == 'slurm' or scheduler == 'example':
        self._header = SlurmHeader()

``autosubmit/platforms/paramiko_submitter.py`` in `line 143 <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/platforms/paramiko_submitter.py#L143>`_ add a new validation for the header command
creation where the platform type

.. code-block:: python
   :emphasize-lines: 1

    elif platform_type == 'slurm' or platform_type == 'example':
        remote_platform = SlurmPlatform(
            asconf.expid, section, exp_data, auth_password = auth_password)


How to configure a Platforms
------------------------------------

To set up your platform you first have to create a new experiment by running the following command creating a
minimal version of a experiment and configure the experiment platform to Marenostrum.

``autosubmit expid -H MARENOSTRUM5 -d "platform test" --minimal``

You'll have to insert the **PARAMETERS** to make your experiment work properly as an example the following
instruction are thought to execute a small job through Autosubmit explaining how to configure a platform.

First create a new folder at the root ``~/Autosubmit`` called project executing the following command:

``mkdir -p ~/autosubmit/project``

.. hint::
    The given name of the folder can be any as long as it matches the ``Local`` Parameter, the change in name
    needs to take this into account

For the execution of this test a few files will need to be created within the new folder,
this file will have the Platform commands to be executed

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

For sake of keeping and concise and clear example of how AutoSubmit works a simple instruction can be executed.
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
