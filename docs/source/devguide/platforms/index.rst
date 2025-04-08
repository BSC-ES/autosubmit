Platforms
=========
.. |br| raw:: html

    <br />


.. note::
    This documentation is based on the v4.1.13 branch, and can only guarantee reproducibility in this context

Extending an Existing Platform
------------------------------

Platforms are defined under Python classes. The source files for such classes are stored inside
``autosubmit/platforms/`` directory. To extend an existing platform we will create a child class from an existing
platform class, for which first we need to identify which existing platform is the most suitable for our project.

.. note::
    Currently the platforms available are:
    |br| :ref:`Local Platform <Local Platform>`
    |br| :mod:`EC Platform <autosubmit.platforms.ecplatform>` :mod:`PBS Platform <autosubmit.platforms.pbsplatform>`
    |br| :mod:`PJM Platform <autosubmit.platforms.pjmplatform>` :mod:`Slurm Platform <autosubmit.platforms.slurmplatform>`

Composing the Extended Platform Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this page we will be extending the SLURM
platform - source file ``autosubmit/platforms/slurmplatform.py``, see in GitHub `slurmplatform.py <https://github.com/BSC-ES/autosubmit/blob/53b2a142fee5c8d8ac169547528c768c93e02a4a/autosubmit/platforms/slurmplatform.py#L35>`_ -, but any platform can be extended by following the same steps.

The platform will be transcribing the files and configurations you set manually to allow operations,
and connection to SLURM and its commands, preparing your experiments to be executed transforming configuration
into executable functions.

We will create a new file in ``/autosubmit/platforms/``
and we are going to call it ``slurm_example.py``.

.. code-block:: python
    :linenos:

    from autosubmit.platforms.slurmplatform import SlurmPlatform

    class Slurm_ExamplePlatform(SlurmPlatform):
        """ Class to manage slurm jobs """

This will create a class in which the ``Slurm_ExamplePlatform`` will be associated as its parent class allowing
``Slurm_ExamplePlatform`` inherit all its characteristics.

We create an initialization method with the required parameters.

.. code-block:: python
    :linenos:

    def __init__(self, expid: str, name: str, config: dict):
        """ Initialization of the Class ExamplePlatform

        :param expid: Id of the experiment.
        :type expid: str
        :param name: Name of the platform.
        :type name: str
        :param config: A dictionary containing all the Experiment parameters.
        :type config: dict
        """
        SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)
        self.example_platform_parameter = ... # add any platform specific parameters

As it can be seen, the parent class has an initialization method to invoke all the parent'`s methods and attributes
into the child (``Slurm_ExamplePlatform``).
In order to override methods from the parent class, we can simply redefine them as shown below, this way we can add
new parameters and/or behaviours, making it possible to add flexibility and restructure a platform for the new needs.

.. code-block:: python
    :linenos:

    def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
        """Submit a job from a given job object."""
        Log.result(f"Job: {job.name}")
        return None

The class ``submit_job`` is a existing class in ``SlurmPlatform`` that was overwritten to have a new behaviour.

After all needed modifications and expansions, the ``Slurm_ExamplePlatform`` class could look similar to the following example code.

.. code-block:: python
    :linenos:

    from typing import Union
    from autosubmit.platforms.slurmplatform import SlurmPlatform

    class Slurm_ExamplePlatform(SlurmPlatform):
        """Class to manage slurm jobs"""
        def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
            """ Initialization of the Class ExamplePlatform

            :param expid: Id of the experiment.
            :type expid: str
            :param name: Name of the platform.
            :type name: str
            :param config: A dictionary containing all the Experiment parameters.
            :type config: dict
            """
            SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)

        def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
            """Submit a job from a given job object."""
            Log.result(f"Job: {job.name}")
            return None


Integrating the Extended Platform into the Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To ensure that the platform will be created as expected, we need to make some changes in 3 different files
|br| ``autosubmit/job/job.py`` - see in GitHub `job.py <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/job/job.py>`_.
|br| ``autosubmit/autosubmit.py`` - see in GitHub `autosubmit.py <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/autosubmit.py>`_.
|br| ``autosubmit/platforms/paramiko_submitter.py`` - see in GitHub `paramiko_submitter.py <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/platforms/paramiko_submitter.py>`_.
|br| ``type`` from ``platform.type`` is defined in the YAML file that configures a platform as it's shown :ref:`here <TargetPlatform>`
to determine the scheduler.

.. warning::
    The numbers noted down to each of the files could become obsolete locally as files get updated so they should be
    seen more as a reference


``autosubmit/autosubmit.py`` in `line 2538 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/autosubmit.py#L2537>`_  add a new ``string`` making sure the new platform type is considered
the same as SLURM platform, as we expect a similar behaviour.

.. code-block:: python
   :emphasize-lines: 1

    if platform.type.lower() in [ "slurm" , "pjm", "example" ] and not inspect and not only_wrappers:
                    # Process the script generated in submit_ready_jobs
                    save_2, valid_packages_to_submit = platform.process_batch_ready_jobs(valid_packages_to_submit,
                                                                                         failed_packages,
                                                                                         error_message="", hold=hold)

``autosubmit/job/job.py`` in `line 2575 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/job/job.py#L2575>`_ ensure each job Job writes
the timestamp to TOTAL_STATS file and jobs_data.db properly.

.. code-block:: python
   :emphasize-lines: 1

    if job_data_dc and type(self.platform) is not str and (self.platform.type in ["slurm", "example"]):
        thread_write_finish = Thread(target=ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR, historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR).write_platform_data_after_finish, args=(job_data_dc, self.platform))
            thread_write_finish.name = "JOB_data_{}".format(self.name)
            thread_write_finish.start()

``autosubmit/job/job.py`` in `line 2817 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/job/job.py#L2817>`_ add a new validation for the validation of the queue
creation with the platform type

.. code-block:: python
    :emphasize-lines: 1

    if self._platform.type in ["slurm", "example"]:
        self._platform.send_command(
            self._platform.get_queue_status_cmd(self.id))
        reason = self._platform.parse_queue_reason(
            self._platform._ssh_output, self.id)


``autosubmit/platforms/paramiko_submitter.py`` in `line 143 <https://github.com/BSC-ES/autosubmit/blob/v4.1.13/autosubmit/platforms/paramiko_submitter.py#L143>`_ add a new validation for the header command
creation where the platform type

.. code-block:: python
   :emphasize-lines: 1

    elif platform_type in ["slurm", "example"]:
        remote_platform = SlurmPlatform(
            asconf.expid, section, exp_data, auth_password = auth_password)


How to Configure a Platform
------------------------------------

To set up your platform, you first have to create a new experiment by running the following command:
|br| *Change the platform from MARENOSTRUM5 to whichever you will use*

.. parsed-literal::

    autosubmit :ref:`expid <expids>` -H MARENOSTRUM5 -d "platform test" --minimal

This will generate a minimal version of an experiment.

To change the configuration of your experiment to ensure it works properly, you can create a project and customize its parameters. The following instructions are
designed to execute a small job through Autosubmit, explaining how to configure a new platform.

Open the file ``~/autosubmit/<expid>/config/minimal.yml`` and you'll find a file as shown below.

.. code-block:: yaml

    CONFIG:
        AUTOSUBMIT_VERSION: "4.1.12"
        TOTALJOBS: 20
        MAXWAITINGJOBS: 20

    DEFAULT:
        EXPID: <EXPID> # ID of the experiment
        HPCARCH: "MARENOSTRUM5" # This will be the default platform if a job doesn't contain a defined platform
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

Now we start configuring the experiment adding the additional ``PARAMETERS`` to create a simple executable experiment

.. code-block:: yaml

    EXPERIMENT:
        DATELIST: 19900101
        MEMBERS: fc0
        CHUNKSIZEUNIT: month
        SPLITSIZEUNIT: day
        CHUNKSIZE: 1
        NUMCHUNKS: 2
        CALENDAR: standard


Add the following PARAMETER will point towards the folder containing all the scripts and instructions to be
used to execute the experiment in the platform

.. code-block:: yaml

    LOCAL:
        PROJECT_PATH: /home/user/experiment_example # path to your project sources


Autosubmit will copy your sources to the ``$autosubmit_installation/$expid/proj/%PROJECT.PROJECT_DESTINATION%``.

The following settings used to create a connection with a platform to execute the jobs,
you must to input the information suitable for your project. (e.g.: user, host, platform)


.. _TargetPlatform:

---------

.. code-block:: yaml

    PLATFORMS:
        MARENOSTRUM5:
            TYPE: <Scheduler> [slurm, ps, example]
            HOST: <Host>
            PROJECT: <Project_Name_Folder>
            USER: <User>
            scratch_dir: <location of project/user>
            QUEUE: gp_debug [dummy, gp_debug, nf, hpc]
            MAX_WALLCLOCK: <HH:MM>
            MAX_PROCESSORS: <N> # This is to enable horizontal_wrappers
            PROCESSORS_PER_NODE: 112 # Each HPC has their own number check the documentation of your platform

.. warning::
    If you cannot connect, it may be because your user doesn't have access to the host, or the PARAMETER SCRATCH_DIR
    might be pointing to a non-existing folder on the host.

    Make sure to create the folder with your USERNAME inside the proper path you pointed to
    (e.g.: <Project_Dir>/<Project_Name_Folder>/<USER>)

How to generate a new experiment
------------------------------------

Now you can add jobs at the end of the file to see the execution
Each job will point to one of the ``Bash`` files that will be created in the next step, meaning that Autosubmit will
look for the instructions of the experiment in the ``~/autosubmit/<expid>/proj/local_project/`` if none is found
inside the folder Autosubmit will look at ``LOCAL.PROJECT_PATH`` set earlier in order to copy to the project folder
if they exist.


.. note::
    The files can also be R, python2, python3. By default it is bash and can be changed by setting the file type.

    .. code-block:: yaml

        JOBS:
            LOCAL_SETUP:
                TYPE: Python # adding this


.. code-block:: yaml

    JOBS:
        LOCAL_SETUP:
            FILE: LOCAL_SETUP.sh # ~/autosubmit/<expid>/proj/local_project/LOCAL_SETUP.sh
            PLATFORM: Local
            RUNNING: once

        SYNCHRONIZE:
            FILE: SYNCHRONIZE.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: LOCAL_SETUP
            RUNNING: once
            WALLCLOCK: 00:05

        REMOTE_SETUP:
            FILE: REMOTE_SETUP.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: SYNCHRONIZE
            WALLCLOCK: 00:05
            RUNNING: once

        INI:
            FILE: INI.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: REMOTE_SETUP
            RUNNING: once
            WALLCLOCK: 00:05

        DATA_NOTIFIER:
            FILE: DATA_NOTIFIER.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: INI
            RUNNING: chunk

        SIM:
            FILE: SIM.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: DATA_NOTIFIER
            RUNNING: chunk

        STATISTICS:
            FILE: STATISTICS.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: SIM
            RUNNING: chunk

        APP:
            FILE: APP.sh
            PLATFORM: MARENOSTRUM5
            DEPENDENCIES: STATISTICS
            RUNNING: chunk

        CLEAN:
            FILE: CLEAN.sh
            # PLATFORM: MARENOSTRUM5
            DEPENDENCIES: APP SIM STATISTICS
            RUNNING: once
            WALLCLOCK: 00:05

Once you finish setting up all the new configurations, you can run the following command to generate the experiment
just created; we need to create a new folder to keep all the instructions for the experiment to be executed on the
platform.

``mkdir -p /home/user/experiment_example``

.. hint::
    The name of the folder can be anything as long as it matches the Local Parameter specified in the configuration
    file; the name change needs to take this into account

For the execution of this test, a few files will need to be created within the new folder;
these files will contain proj-associated code that will be executed on the job-specified platform.

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

To keep and concise and clear example of how Autosubmit works, a simple instruction can be executed as a test.
So add the following the instruction below to one or more ``Bash`` files created in the previous steps.

.. code-block:: yaml

    sleep 5

How to run the experiment
------------------------------------

``autosubmit create -np -f -v <EXPID>``

Once the experiment is generated we can execute it and check if by running the command below the experiment

    #. Submit the job to the specified platform
    #. monitor their status
    #. transfers logs to $expid/tmp/Log_$expid

``autosubmit run <EXPID>``

.. note::
    For more examples on how to create and share configurations of experiments and platforms,
    you can visit the :ref:`page <create_and_share_config>`.