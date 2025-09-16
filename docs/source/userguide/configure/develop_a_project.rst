:orphan:

.. _develproject:

====================
Developing a project
====================

This section contains some examples on how to develop a new project.

All files, with the exception of user-defined scripts, are located in the ``<expid>/conf`` directory.

Configuration files are written in ``yaml`` format. In the other hand, the user-defined scripts are written in ``bash/python or R`` format.

To configure the experiment, edit ``autosubmit_<EXPID>.yml``, ``expdef_<EXPID>.yml``, ``jobs_<EXPID>.yml`` , ``platforms_<EXPID>.yml`` and ``proj_<EXPID>.yml``` in the ``conf`` folder of the experiment.

Expdef configuration
====================

    vi <experiments_directory>/<EXPID>/conf/expdef_<EXPID>.yml

.. code-block:: yaml

    DEFAULT:
        # Experiment identifier
        # No need to change
        EXPID: cxxx
        # HPC name.
        # No need to change
        HPCARCH: ithaca

    experiment:
        # Supply the list of start dates. Available formats: YYYYMMDD YYYYMMDDhh YYYYMMDDhhmm
        # Also you can use an abbreviated syntax for multiple dates with common parts:
        # 200001[01 15] <=> 20000101 20000115
        # DATELIST: 19600101 19650101 19700101
        # DATELIST: 1960[0101 0201 0301]
        DATELIST: 19900101
        # Supply the list of members. LIST: fc0 fc1 fc2 fc3 fc4
        MEMBERS: fc0
        # Chunk size unit. STRING: hour, day, month, year
        CHUNKSIZEUNIT: month
        # Split size unit. STRING: hour, day, month, year and lower than CHUNKSIZEUNIT
        SPLITSIZEUNIT: day # default CHUNKSIZEUNIT-1 (month-1 == day)
        # Chunk size. NUMERIC: 4, 6, 12
        CHUNKSIZE: 1
        # Split size. NUMERIC: 4, 6, 12
        SPLITSIZE: 1
        # Total number of chunks in experiment. NUMERIC: 30, 15, 10
        NUMCHUNKS: 2
        # Calendar used. LIST: standard, noleap
        CALENDAR: standard

        # List of members that can be included in this run. Optional.
        # RUN_ONLY_MEMBERS: fc0 fc1 fc2 fc3 fc4
        # RUN_ONLY_MEMBERS: fc[0-4]
        RUN_ONLY_MEMBERS :

    rerun:
        # Is a rerun or not? [Default: Do set FALSE]. BOOLEAN: TRUE, FALSE
        RERUN: FALSE
        # If RERUN: TRUE then supply the list of jobs to rerun
        RERUN_JOBLIST :

    project:
        # Select project type. STRING: git, svn, local, none
        # If PROJECT_TYPE is set to none, Autosubmit self-contained dummy templates will be used
        PROJECT_TYPE: git
        # Destination folder name for project. type: STRING, default: leave empty,
        PROJECT_DESTINATION: model

    # If PROJECT_TYPE is not git, no need to change
    git:
        # Repository URL  STRING: 'https://github.com/torvalds/linux.git'
        PROJECT_ORIGIN: https://gitlab.cfu.local/cfu/auto-ecearth3.git
        # Select branch or tag, STRING, default: 'master',
        # help: {'master' (default), 'develop', 'v3.1b', ...}
        PROJECT_BRANCH: develop
        # type: STRING, default: leave empty, help: if model branch is a TAG leave empty
        PROJECT_COMMIT :

    # If PROJECT_TYPE is not svn, no need to change
    svn:
        # type: STRING, help: 'https://svn.ec-earth.org/ecearth3'
        PROJECT_URL :
        # Select revision number. NUMERIC: 1778
        PROJECT_REVISION :

    # If PROJECT_TYPE is not local, no need to change
    local:
        # type: STRING, help: /foo/bar/ecearth
        PROJECT_PATH :

        # If PROJECT_TYPE is none, no need to change
    project_files:
        # Where is PROJECT CONFIGURATION file location relative to project root path
        FILE_PROJECT_CONF: templates/ecearth3/ecearth3.yml
        # Where is JOBS CONFIGURATION file location relative to project root path
        FILE_JOBS_CONF: templates/common/jobs.yml

Autosubmit configuration
========================

    vi <experiments_directory>/<EXPID>/conf/autosubmit_<EXPID>.yml

.. code-block:: yaml

    config:
        # Experiment identifier
        # No need to change
        EXPID :
        # No need to change.
        # Autosubmit version identifier
        AUTOSUBMIT_VERSION :
        # Default maximum number of jobs to be waiting in any platform
        # Default: 3
        MAXWAITINGJOBS: 3
        # Default maximum number of jobs to be running at the same time at any platform
        # Can be set at platform level on the platform_<EXPID>.yml file
        # Default: 6
        TOTALJOBS: 6
        # Time (seconds) between connections to the HPC queue scheduler to poll already submitted jobs status
        # Default:10
        SAFETYSLEEPTIME: 10
        # Time (seconds) before ending the run to retrieve the last logs.
        # Default:180
        LAST_LOGS_TIMEOUT: 180
        # Number of retrials if a job fails. Can ve override at job level
        # Default:0
        RETRIALS:0
        ##  Allows to put a delay between retries, of retrials if a job fails. If not specified, it will be static
        # DELAY_RETRY_TIME:11
        # DELAY_RETRY_TIME:+11 # will wait 11,22,33,44...
        # DELAY_RETRY_TIME:*11 # will wait 11,110,1110,11110...
        # Default output type for CREATE, MONITOR, SET STATUS, RECOVERY. Available options: pdf, svg, png, ps, txt
        # Default:pdf
        # This parameter is used to enable the use of threads in autosubmit for the wrappers. # Default False
        ENABLE_WRAPPER_THREADS: False
        OUTPUT:pdf
        WRAPPERS_WALLCLOCK: 48:00  # Default max_wallclock for wrappers before getting killed
        JOB_WALLCLOCK: 24:00  # Default max_wallclock for jobs before getting killed
        LOG_RECOVERY_CONSOLE_LEVEL: "DEBUG"  # Default log level for console output for the log recovery process.
        LOG_RECOVERY_FILE_LEVEL: "EVERYTHING"  # Default log level for file output for the log recovery process.
    # wrapper definition
    wrappers:
        wrapper_1_v_example:
            TYPE: Vertical
            JOBS_IN_WRAPPER: sim
        wrapper_2_h_example:
            TYPE: Horizontal
            JOBS_IN_WRAPPER: da

Jobs configuration
==================

    vi <experiments_directory>/<EXPID>/conf/jobs_<EXPID>.yml

.. code-block:: yaml

    JOBS:
        LOCAL_SETUP:
            FILE: LOCAL_SETUP.sh
            PLATFORM: LOCAL

        REMOTE_SETUP:
            FILE: REMOTE_SETUP.sh
            DEPENDENCIES: LOCAL_SETUP
            WALLCLOCK: 00:05

        INI:
            FILE: INI.sh
            DEPENDENCIES: REMOTE_SETUP
            RUNNING: member
            WALLCLOCK: 00:05

        SIM:
            FILE: SIM.sh
            DEPENDENCIES: INI SIM-1 CLEAN-2
            RUNNING: chunk
            WALLCLOCK: 00:05
            PROCESSORS: 2
            THREADS: 1

        POST:
            FILE: POST.sh
            DEPENDENCIES: SIM
            RUNNING: chunk
            WALLCLOCK: 00:05

        CLEAN:
            FILE: CLEAN.sh
            DEPENDENCIES: POST
            RUNNING: chunk
            WALLCLOCK: 00:05

        TRANSFER:
            FILE: TRANSFER.sh
            PLATFORM: LOCAL
            DEPENDENCIES: CLEAN
            RUNNING: member

Platform configuration
======================

    vi <experiments_directory>/<EXPID>/conf/platforms_<EXPID>.yml

.. code-block:: yaml


    PLATFORMS:
        # Example platform with all options specified
        ## Platform name
        # PLATFORM:
        ## Queue type. Options: PS, ecaccess, SLURM
        # TYPE:
        ## Version of queue manager to use. Needed only in ecaccess (options: pbs, loadleveler)
        # VERSION:
        ## Hostname of the HPC
        # HOST:
        ## Project for the machine scheduler
        # PROJECT:
        ## Budget account for the machine scheduler. If omitted, takes the value defined in PROJECT
        # BUDGET:
        ## Option to add project name to host. This is required for some HPCs
        # ADD_PROJECT_TO_HOST: False
        ## User for the machine scheduler
        # USER:
        ## Path to the scratch directory for the machine
        # SCRATCH_DIR: /scratch
        ## If true, autosubmit test command can use this queue as a main queue. Defaults to false
        # TEST_SUITE: False
        ## If given, autosubmit will add jobs to the given queue
        # QUEUE:
        ## If specified, autosubmit will run jobs with only one processor in the specified platform.
        # SERIAL_PLATFORM: SERIAL_PLATFORM_NAME
        ## If specified, autosubmit will run jobs with only one processor in the specified queue.
        ## Autosubmit will ignore this configuration if SERIAL_PLATFORM is provided
        # SERIAL_QUEUE: SERIAL_QUEUE_NAME
        ## Default number of processors per node to be used in jobs
        # PROCESSORS_PER_NODE:
        ## Default Maximum number of jobs to be waiting in any platform queue
        ## Default: 3
        # MAX_WAITING_JOBS: 3
        ## Default maximum number of jobs to be running at the same time at the platform.
        ## Applies at platform level. Considers QUEUEING + RUNNING jobs.
        ## Ideal for configurations where some remote platform has a low upper limit of allowed jobs per user at the same time.
        ## Default: 6
        # TOTAL_JOBS: 6


Proj configuration
==================

After filling the experiment configuration and prompt ``autosubmit create <EXPID> -np`` create, user can go into ``proj`` which has a copy of the model.

The experiment project contains the scripts specified in ``jobs_<EXPID>.yml`` and a copy of model source code and data specified in ``expdef_xxxx.yml``.

To configure experiment project parameters for the experiment, edit ``proj_<EXPID>.yml``.

*proj_<EXPID>.yml* contains:
    - The project dependant experiment variables that Autosubmit will substitute in the scripts to be run.

.. warning:: The ``proj_<EXPID>.yml`` has to be defined in INI style so it should has section headers. At least one.

Example:
::

    vi <experiments_directory>/<EXPID>/conf/proj_<EXPID>.yml

.. code-block:: yaml

    common:
        # No need to change.
        MODEL: ecearth
        # No need to change.
        VERSION: v3.1
        # No need to change.
        TEMPLATE_NAME: ecearth3
        # Select the model output control class. STRING: Option
        # listed under the section: https://earth.bsc.es/wiki/doku.php?id=overview_outclasses
        OUTCLASS: specs
        # After transferring output at /cfunas/exp remove a copy available at permanent storage of HPC
        # [Default: Do set "TRUE"]. BOOLEAN: TRUE, FALSE
        MODEL_output_remove: TRUE
        # Activate cmorization [Default: leave empty]. BOOLEAN: TRUE, FALSE
        CMORIZATION: TRUE
        # Essential if cmorization is activated.
        # STRING:   (http://www.specs-fp7.eu/wiki/images/1/1c/SPECS_standard_output.pdf)
        CMORFAMILY:
        # Supply the name of the experiment associated (if there is any) otherwise leave it empty.
        # STRING (with space): seasonal r1p1, seaiceinit r?p?
        ASSOCIATED_EXPERIMENT:
        # Essential if cmorization is activated (Forcing). STRING: Nat,Ant (Nat and Ant is a single option)
        FORCING:
        # Essential if cmorization is activated (Initialization description). STRING: N/A
        INIT_DESCR:
        # Essential if cmorization is activated (Physics description). STRING: N/A
        PHYS_DESCR:
        # Essential if cmorization is activated (Associated model). STRING: N/A
        ASSOC_MODEL:

    grid:
        # AGCM grid resolution, horizontal (truncation T) and vertical (levels L).
        # STRING: T159L62, T255L62, T255L91, T511L91, T799L62 (IFS)
        IFS_resolution: T511L91
        # OGCM grid resolution. STRING: ORCA1L46, ORCA1L75, ORCA025L46, ORCA025L75 (NEMO)
        NEMO_resolution: ORCA025L75

    oasis:
        # Coupler (OASIS) options.
        OASIS3: yes
        # Number of pseudo-parallel cores for coupler [Default: Do set "7"]. NUMERIC: 1, 7, 10
        OASIS_nproc: 7
        # Handling the creation of coupling fields dynamically [Default: Do set "TRUE"].
        # BOOLEAN: TRUE, FALSE
        OASIS_flds: TRUE

    ifs:
        # Atmospheric initial conditions ready to be used.
        # STRING: ID found here: https://earth.bsc.es/wiki/doku.php?id=initial_conditions:atmospheric
        ATM_ini:
        # A different IC member per EXPID member ["PERT"] or which common IC member
        # for all EXPID members ["fc0" / "fc1"]. String: PERT/fc0/fc1...
        ATM_ini_member:
        # Set timestep (in sec) w.r.t resolution.
        # NUMERIC: 3600 (T159), 2700 (T255), 900 (T511), 720 (T799)
        IFS_timestep: 900
        # Number of parallel cores for AGCM component. NUMERIC: 28, 100
        IFS_nproc: 640
        # Coupling frequency (in hours) [Default: Do set "3"]. NUMERIC: 3, 6
        RUN_coupFreq: 3
        # Post-processing frequency (in hours) [Default: Do set "6"]. NUMERIC: 3, 6
        NFRP: 6
        # [Default: Do set "TRUE"]. BOOLEAN: TRUE, FALSE
        LCMIP5: TRUE
        # Choose RCP value [Default: Do set "2"]. NUMERIC: 0, 1=3-PD, 2=4.5, 3=6, 4=8.5
        NRCP: 0
        # [Default: Do set "TRUE"]. BOOLEAN: TRUE, FALSE
        LHVOLCA: TRUE
        # [Default: Do set "0"]. NUMERIC: 1850, 2005
        NFIXYR: 0
        # Save daily output or not [Default: Do set "FALSE"]. BOOLEAN: TRUE, FALSE
        SAVEDDA: FALSE
        # Save reduced daily output or not [Default: Do set "FALSE"]. BOOLEAN: TRUE, FALSE
        ATM_REDUCED_OUTPUT: FALSE
        # Store grib codes from SH files [User need to refer defined  ppt* files for the experiment]
        ATM_SH_CODES:
        # Store levels against "ATM_SH_CODES" e.g: level1,level2,level3, ...
        ATM_SH_LEVELS:
        # Store grib codes from GG files [User need to refer defined  ppt* files for the experiment]
        ATM_GG_CODES:
        # Store levels against "ATM_GG_CODES" (133.128, 246.128, 247.128, 248.128)
        # e.g: level1,level2,level3, ...
        ATM_GG_LEVELS:
        # SPPT stochastic physics active or not [Default: set "FALSE"]. BOOLEAN: TRUE, FALSE
        LSPPT: FALSE
        # Write the perturbation patterns for SPPT or not [Default: set "FALSE"].
        # BOOLEAN: TRUE, FALSE
        LWRITE_ARP:
        # Number of scales for SPPT [Default: set 3]. NUMERIC: 1, 2, 3
        NS_SPPT:
        # Standard deviations of each scale [Default: set 0.50,0.25,0.125]
        # NUMERIC values separated by ,
        SDEV_SPPT:
        # Decorrelation times (in seconds) for each scale [Default: set 2.16E4,2.592E5,2.592E6]
        # NUMERIC values separated by ,
        TAU_SPPT:
        # Decorrelation lengths (in meters) for each scale [Default: set 500.E3,1000.E3,2000.E3]
        # NUMERIC values separated by ,
        XLCOR_SPPT:
        # Clipping ratio (number of standard deviations) for SPPT [Default: set 2] NUMERIC
        XCLIP_SPPT:
        # Stratospheric tapering in SPPT [Default: set "TRUE"]. BOOLEAN: TRUE, FALSE
        LTAPER_SPPT:
        # Top of stratospheric tapering layer in Pa [Default: set to 50.E2] NUMERIC
        PTAPER_TOP:
        # Bottom of stratospheric tapering layer in Pa [Default: set to 100.E2] NUMERIC
        PTAPER_BOT:
        ## ATMOSPHERIC NUDGING PARAMETERS ##
        # Atmospheric nudging towards re-interpolated ERA-Interim data. BOOLEAN: TRUE, FALSE
        ATM_NUDGING: FALSE
        # Atmospheric nudging reference data experiment name. [T255L91: b0ir]
        ATM_refund:
        # Nudge vorticity. BOOLEAN: TRUE, FALSE
        NUD_VO:
        # Nudge divergence. BOOLEAN: TRUE, FALSE
        NUD_DI:
        # Nudge temperature. BOOLEAN: TRUE, FALSE
        NUD_TE:
        # Nudge specific humidity. BOOLEAN: TRUE, FALSE
        NUD_Q:
        # Nudge liquid water content. BOOLEAN: TRUE, FALSE
        NUD_QL:
        # Nudge ice water content. BOOLEAN: TRUE, FALSE
        NUD_QI:
        # Nudge cloud fraction. BOOLEAN: TRUE, FALSE
        NUD_QC:
        # Nudge log of surface pressure. BOOLEAN: TRUE, FALSE
        NUD_LP:
        # Relaxation coefficient for vorticity. NUMERIC in ]0,inf[;
        # 1 means half way between model value and ref value
        ALPH_VO:
        # Relaxation coefficient for divergence. NUMERIC in ]0,inf[;
        # 1 means half way between model value and ref value
        ALPH_DI:
        # Relaxation coefficient for temperature. NUMERIC in ]0,inf[;
        # 1 means half way between model value and ref value
        ALPH_TE:
        # Relaxation coefficient for specific humidity. NUMERIC in ]0,inf[;
        # 1 means half way between model value and ref value
        ALPH_Q:
        # Relaxation coefficient for log surface pressure. NUMERIC in ]0,inf[;
        # 1 means half way between model value and ref value
        ALPH_LP:
        # Nudging area Northern limit [Default: Do set "90"]
        NUD_NLAT:
        # Nudging area Southern limit [Default: Do set "-90"]
        NUD_SLAT:
        # Nudging area Western limit NUMERIC in [0,360] [Default: Do set "0"]
        NUD_WLON:
        # Nudging area Eastern limit NUMERIC in [0,360] [Default: Do set "360"; E<W will span Greenwich]
        NUD_ELON:
        # Nudging vertical levels: lower level [Default: Do set "1"]
        NUD_VMIN:
        # Nudging vertical levels: upper level [Default: Do set to number of vertical levels]
        NUD_VMAX:

    nemo:
        # Ocean initial conditions ready to be used. [Default: leave empty].
        # STRING: ID found here: https://earth.bsc.es/wiki/doku.php?id=initial_conditions:oceanic
        OCEAN_ini:
        # A different IC member per EXPID member ["PERT"] or which common IC member
        # for all EXPID members ["fc0" / "fc1"]. String: PERT/fc0/fc1...
        OCEAN_ini_member:
        # Set timestep (in sec) w.r.t resolution. NUMERIC: 3600 (ORCA1), 1200 (ORCA025)
        NEMO_timestep: 1200
        # Number of parallel cores for OGCM component. NUMERIC: 16, 24, 36
        NEMO_nproc: 960
        # Ocean Advection Scheme [Default: Do set "tvd"]. STRING: tvd, cen2
        ADVSCH: cen2
        # Nudging activation. BOOLEAN: TRUE, FALSE
        OCEAN_NUDGING: FALSE
        # Toward which data to nudge; essential if "OCEAN_NUDGING" is TRUE.
        # STRING: fa9p, s4, glorys2v1
        OCEAN_NUDDATA: FALSE
        # Rebuild and store restarts to HSM for an immediate prediction experiment.
        # BOOLEAN: TRUE, FALSE
        OCEAN_STORERST: FALSE

    ice:
        # Sea-Ice Model [Default: Do set "LIM2"]. STRING: LIM2, LIM3
        ICE: LIM3
        # Sea-ice initial conditions ready to be used. [Default: leave empty].
        # STRING: ID found here: https://earth.bsc.es/wiki/doku.php?id=initial_conditions:sea_ice
        ICE_ini:
        # A different IC member per EXPID member ["PERT"] or which common IC member
        # for all EXPID members ["fc0" / "fc1"]. String: PERT/fc0/fc1...
        ICE_ini_member:
        # Set timestep (in sec) w.r.t resolution. NUMERIC: 3600 (ORCA1), 1200 (ORCA025)
        LIM_timestep: 1200

    pisces:
        # Activate PISCES (TRUE) or not (FALSE) [Default: leave empty]
        PISCES: FALSE
        # PISCES initial conditions ready to be used. [Default: leave empty].
        # STRING: ID found here: https://earth.bsc.es/wiki/doku.php?id=initial_conditions:biogeochemistry
        PISCES_ini:
        # Set timestep (in sec) w.r.t resolution. NUMERIC: 3600 (ORCA1), 3600 (ORCA025)
        PISCES_timestep: 3600

Proj configuration:: Full example
---------------------------------

This section contains a full example of a valid proj file with a valid user script.

Configuration of proj.yml

    vi <expid>/conf/proj_<EXPID>.yml

.. code-block:: yaml

    PROJECT_ROOT: /gpfs/scratch/bsc32/bsc32070/a000/automatic_performance_profile
    REFRESH_GIT_REPO: false

Write your original script in the user project directory:

    vi <expid>/proj/template/autosubmit/remote_setup.sh

.. code-block:: bash

    cd %CURRENT_ROOTDIR% # This comes from autosubmit.
    # Clone repository to the remote for needed files
    # if exist or force refresh is true
    if [ ! -d %PROJECT_ROOT% ] || [ %REFRESH_GIT_REPO% == true ];
    then
        chmod +w -R %PROJECT_ROOT% || :
        rm -rf %PROJECT_ROOT% || :
        git clone (...)
    fi
    (...)


Final script, which is generated by `autosubmit run` or ``autosubmit inspect``

    cat <experiments_directory>/<EXPID>/tmp/remote_setup.cmd

.. code-block:: bash

    cd /gpfs/scratch/bsc32/bsc32070/a000
    # Clone repository to the remote for needed files
    # if exist or force refresh is true
    if [ ! -d /gpfs/scratch/bsc32/bsc32070/a000/automatic_performance_profile ] || [ false == true ];
    then
        chmod +w -R /gpfs/scratch/bsc32/bsc32070/a000/automatic_performance_profile || :
        rm -rf /gpfs/scratch/bsc32/bsc32070/a000/automatic_performance_profile || :
        git clone (...)
    fi
    (...)

Detailed platform configuration
-------------------------------

In this section, we describe the platform configuration using `-QOS` and also `PARTITION`

    vi <expid>/conf/platform_<EXPID>.yml

.. code-block:: yaml

    PLATFORMS:
        marenostrum0:
            TYPE: ps
            HOST: mn0.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            ADD_PROJECT_TO_HOST: false
            SCRATCH_DIR: /gpfs/scratch

        marenostrum4:
            # Queue type. Options: ps, SLURM, eceaccess
            TYPE: slurm
            HOST: mn1.bsc.es,mn2.bsc.es,mn3.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: False
            # use 72:00 if you are using a PRACE account, 48:00 for the bsc account
            MAX_WALLCLOCK: 02:00
            # use 19200 if you are using a PRACE account, 2400 for the bsc account
            MAX_PROCESSORS: 2400
            PROCESSORS_PER_NODE: 48
            #SERIAL_QUEUE: debug
            #QUEUE: debug
            CUSTOM_DIRECTIVES: ["#SBATCH -p small", "#SBATCH --no-requeue", "#SBATCH --usage"]

        marenostrum_archive:
            TYPE: ps
            HOST: dt02.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: False
            TEST_SUITE: False

        power9:
            TYPE: slurm
            HOST: plogin1.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            SCRATCH_DIR: /gpfs/scratch
            ADD_PROJECT_TO_HOST: False
            TEST_SUITE: False
            SERIAL_QUEUE: debug
            QUEUE: debug

        transfer_node:
            TYPE: ps
            HOST: dt01.bsc.es
            PROJECT: bsc32
            USER: bsc32070
            ADD_PROJECT_TO_HOST: false
            SCRATCH_DIR: /gpfs/scratch

        transfer_node_bscearth000:
            TYPE: ps
            HOST: bscearth000
            USER: dbeltran
            PROJECT: Earth
            ADD_PROJECT_TO_HOST: false
            QUEUE: serial
            SCRATCH_DIR: /esarchive/scratch

        bscearth000:
            TYPE: ps
            HOST: bscearth000
            PROJECT: Earth
            USER: dbeltran
            SCRATCH_DIR: /esarchive/scratch

.. warning::

    The ``TYPE`` field is mandatory.
    The ``HOST`` field is mandatory.
    The ``PROJECT`` field is mandatory.
    The ``USER`` field is mandatory.
    The ``SCRATCH_DIR`` field is mandatory.
    The ``ADD_PROJECT_TO_HOST`` field is mandatory.

.. warning::

    The ``TEST_SUITE`` field is optional.
    The ``MAX_WALLCLOCK`` field is optional.
    The ``MAX_PROCESSORS`` field is optional.
    The ``PROCESSORS_PER_NODE`` field is optional.

.. warning::

    The ``SERIAL_QUEUE`` and ``QUEUE`` field are used for specify a -QOS.
    For specify a partition, you must use ``PARTITION``.
    For specify the memory usage you must use ``MEMORY`` but only in jobs.yml.

The custom directives can be used for multiple parameters at the same time using the follow syntax.

    vi <expid>/conf/platform_<EXPID>.yml

.. code-block:: yaml

    PLATFORMS:
        puhti:
            #Check your partition ( test/small/large])
            CUSTOM_DIRECTIVES: ["#SBATCH -p test", "#SBATCH --no-requeue", "#SBATCH --usage"]
            ### Batch job system / queue at HPC
            TYPE: slurm
            ### Hostname of the HPC
            HOST: puhti
            ### Project name-ID at HPC (WEATHER)
            PROJECT: project_test
            ### User name at HPC
            USER: dbeltran
            ### Path to the scratch directory for the project at HPC
            SCRATCH_DIR: /scratch
            # Should've false already, just in case it is not
            ADD_PROJECT_TO_HOST: False

            #Check your partition ( test[00:15]/small[72:00]/large[72:00]) max_wallclock
            MAX_WALLCLOCK: 00:15
            # [test [80] // small [40] // large [1040]
            MAX_PROCESSORS: 80
            # test [40] / small [40] // large [40]
            PROCESSORS_PER_NODE: 40

Controlling the number of active concurrent tasks in an experiment
----------------------------------------------------------------------

In some cases, you may want to control the number of concurrent tasks/jobs that can be active in an experiment.

To set the maximum number of concurrent tasks/jobs, you can use the ``TOTAL_JOBS`` and ``MAX_WAITING_JOBS`` variable in the ``conf/autosubmit_<EXPID>.yml`` file.

    vi <EXPID>/conf/autosubmit_<EXPID>.yml

.. code-block:: yaml

    # Controls the maximum number of submitted,waiting and running tasks
    TOTAL_JOBS: 10
    # Controls the maximum number of submitted and waiting tasks
    MAX_WAITING_JOBS: 10

To control the number of jobs included in a wrapper, you can use the `MAX_WRAPPED_JOBS` and `MIN_WRAPPED_JOBS` variables in the ``conf/autosubmit_<EXPID>.yml`` file.

Note that a wrapped job is counted as a single job regardless of the number of tasks it contains. Therefore, `TOTAL_JOBS` and `MAX_WAITING_JOBS` won't have an impact inside a wrapper.

    vi <expid>/conf/autosubmit_<EXPID>.yml

.. code-block:: yaml

    wrappers:
        wrapper:
            TYPE: <ANY>
            MIN_WRAPPED: 2 # Minium amount of jobs that will be wrapped together in any given time.
            MIN_WRAPPED_H: 2 # Same as above but only for the horizontal packages.
            MIN_WRAPPED_V: 2 # Same as above but only for the vertical packages.
            MAX_WRAPPED: 99999 # Maximum amount of jobs that will be wrapped together in any given time.
            MAX_WRAPPED_H: 99999 # Same as above but only for the horizontal packages.
            MAX_WRAPPED_V: 99999 # Same as above but only for the vertical packages.

- **MAX_WRAPPED** can be defined in ``jobs_<EXPID>.yml`` in order to limit the number of jobs wrapped for the corresponding job section
    - If not defined, it considers the **MAX_WRAPPED** defined under wrapper: in ``autosubmit_<EXPID>.yml``
        - If **MAX_WRAPPED** is not defined, then the max_wallclock of the platform will be final factor.
- **MIN_WRAPPED** can be defined in ``autosubmit_<EXPID>.yml`` in order to limit the minimum number of jobs that a wrapper can contain
    - If not defined, it considers that **MIN_WRAPPED** is 2.
    - If **POLICY** is flexible and it is not possible to wrap **MIN_WRAPPED** or more tasks, these tasks will be submitted as individual jobs, as long as the condition is not satisfied.
    - If **POLICY** is mixed and there are failed jobs inside a wrapper, these jobs will be submitted as individual jobs.
    - If **POLICY** is strict and it is not possible to wrap **MIN_WRAPPED** or more tasks, these tasks will not be submitted until there are enough tasks to build a package.
    - strict and mixed policies can cause **deadlocks**.
