# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Code to manage Autosubmit workflows."""

import copy
import os
import platform
import signal
import threading
import time
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

import paramiko
from portalocker import Lock

import autosubmit.helpers.autosubmit_helper as AutosubmitHelper
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import database_backup
from autosubmit.database.db_manager_historical import HistoricalDbManager
from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.experiment.manage import (
    provenance,
)
from autosubmit.experiment.utils import print_job_details
from autosubmit.git.autosubmit_git import check_unpushed_changes
from autosubmit.history.database_managers.experiment_history_db_manager import (
    get_last_run_id,
)
from autosubmit.history.experiment_history import (
    ExperimentHistory,
    get_historical_database,
)
from autosubmit.job.filters import (
    apply_job_filters,
    filter_jobs_by_chunks_splits,
    filter_sections_splits,
)
from autosubmit.job.job_common import Status, get_job_status
from autosubmit.job.job_grouping import JobGrouping
from autosubmit.job.job_list import JobList, load_job_list
from autosubmit.job.job_utils import check_non_wrapped_jobs, check_wrappers
from autosubmit.job.validation import validate_job_filters
from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.platforms.manage import restore_platforms
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.scheduler import (
    Scheduler,
    generate_scripts_andor_wrappers,
    submit_ready_jobs,
)

if TYPE_CHECKING:
    from autosubmit.platforms.paramiko_platform import ParamikoPlatform
    from autosubmit.platforms.platform import Platform
    from autosubmit.profiler.profiler import Profiler


__all__ = ["inspect", "monitor", "recover", "run", "statistics", "stop"]


def _signal_handler(signal_received, frame) -> None:
    # Disable all the no-member violations in this function
    # pylint: disable=W0613
    """
    Used to handle interrupt signals, allowing autosubmit to clean before exit

    :param signal_received: The signal received by the process.
    :param frame: Current program frame.
    """
    Log.info("Autosubmit will interrupt at the next safe occasion")
    Scheduler.exit = True


def _prepare_run(
    expid: str,
    start_time: str | None = None,
    start_after: str | None = None,
    run_only_members: str | None = None,
    recover: bool = False,
    check_scripts: bool = False,
    submitter: ParamikoSubmitter | None = None,
) -> tuple[
    JobList,
    ParamikoSubmitter,
    ExperimentHistory | None,
    str | None,
    AutosubmitConfig,
    set["Platform"],
    bool,
]:
    """Prepare the run of the experiment.

    :param expid: a string with the experiment id.
    :param start_time: a string with the starting time of the experiment.
    :param start_after: a string with the experiment id to start after.
    :param run_only_members: a string with the members to run.
    :param recover: a boolean to indicate if the experiment is recovering from a failure.
    :param check_scripts: Whether to check the scripts before submitting.
    :param submitter: the actual loaded platforms if any
    :return: a Union
    """
    host = platform.node()
    # Init the AutosubmitConfig and check that every file exists, and it is a valid configuration.
    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.check_conf_files(running_time=True, force_load=True)
    if not recover:
        # Database stuff, to check if the experiment is active or not.
        try:
            # Handling starting time
            AutosubmitHelper.handle_start_time(start_time)
            # Start after completion trigger block
            AutosubmitHelper.handle_start_after(start_after, expid)
            # Handling run_only_members
        except AutosubmitCritical:
            raise
        except Exception as e:
            raise AutosubmitCritical(
                "Failure during setting the start time check trace for details",
                7014,
                str(e),
            )
        os.system("clear")
        if threading.current_thread().name is threading.main_thread():
            signal.signal(signal.SIGINT, _signal_handler)
        else:
            Log.debug("Not setting signal handler: Autosubmit running within a thread.")
        # The time between running iterations, default to 10 seconds. Can be changed by the user
        safetysleeptime = as_conf.get_safetysleeptime()
        retrials = as_conf.get_retrials()
        Log.debug(f"The Experiment name is: {expid}")
        Log.debug(f"Sleep: {safetysleeptime}")
        Log.debug(f"Default retrials: {retrials}")
        # Is where Autosubmit stores the job_list and wrapper packages to the disc.
        db_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "db")
        Log.debug(f"Starting from job list restored from {db_dir} files")

    if not submitter:
        submitter = ParamikoSubmitter(as_conf=as_conf)
    # Tries to load the job_list from disk, discarding any changes in running time ( if recovery ).
    # Could also load a backup from previous iteration.
    # The submit ready functions will cancel all job submitted if one submitted in that iteration had issues,
    # so it should be safe to recover from a backup without losing job ids

    if recover:
        Log.info("Recovering job_list")
    try:
        job_list = load_job_list(
            expid,
            as_conf,
            new=False,
            full_load=False,
            submitter=submitter,
            check_failed_jobs=True,
            run_mode=False,
        )

    except OSError as e:
        raise AutosubmitError("Job_list not found", 6016, str(e))
    except AutosubmitCritical as e:
        raise AutosubmitCritical(
            "Corrupted job_list, backup couldn't be restored", 7040, e.message
        )
    except Exception as e:
        Log.debug(f"Error while loading job_list: {str(e)}")
        raise AutosubmitCritical(
            "Corrupted job_list, backup couldn't be restored", 7040, str(e)
        )
    Log.debug(f"Length of the jobs list: {len(job_list)}")
    if recover:
        Log.info("Recovering parameters info")
    # This function name is not clear after the transformation it received across years.
    # What it does, is to load and transform all as_conf.experiment_data into a 1D dict stored in job_list object.
    as_conf.set_platform_parameters(job_list, submitter.platforms)
    Log.debug("Checking experiment templates...")
    # Load only platforms used by the experiment, by looking at JOBS.$JOB.PLATFORM. So Autosubmit only establishes connections to the machines that are used.
    # Also, it ignores platforms used by "COMPLETED/FAILED" jobs as they are no need any more. ( in case of recovery or run a workflow that were already running )
    # This function, looks at %JOBS.$JOB.FILE% ( mandatory ) and %JOBS.$JOB.CHECK% ( default True ).
    # Checks the contents of the .sh/.py/r files and looks for AS placeholders.
    try:
        if check_scripts:
            job_list.check_scripts(as_conf)
    except Exception as e:
        raise AutosubmitCritical("Error while checking job templates", 7014, str(e))

    # Check if the user wants to continue using wrappers and loads the appropriate info.
    if as_conf.experiment_data.get("WRAPPERS", None) is not None:
        try:
            job_list.run_id = get_last_run_id(expid)
            job_list.load_wrappers()
        except OSError as e:
            raise AutosubmitError(
                "wrappers not found in job_list database", 6016, str(e)
            )
    if recover:
        Log.info("Recovering wrappers... Done")

    Log.debug("Checking job_list current status")
    job_list.update_list(as_conf)
    job_list.clear_generate()
    job_list.save_jobs()
    as_conf.save()
    # Before starting main loop, setup historical database tables and main information
    # Check if the user has launch autosubmit run with -rom option ( previously named -rm )
    allowed_members = AutosubmitHelper.get_allowed_members(run_only_members, as_conf)
    if allowed_members:
        # Set allowed members after checks have been performed.
        # This triggers the setter and main logic of the -rm feature.
        job_list.run_members = allowed_members
        Log.result(
            f"Only jobs with member value in {str(allowed_members)} or no member will be allowed in this "
            f"run. Also, those jobs already SUBMITTED, QUEUING, or RUNNING will be allowed to complete and"
            f" will be tracked."
        )
    if not recover:
        # This function, looks at the "TWO_STEP_START" variable in the experiment configuration file.
        # This may not be necessary any more as the same can be achieved by using the new DEPENDENCIES dict.
        # I replicated the same functionality in the new DEPENDENCIES dict using crossdate wrappers of
        # auto-monarch da ( documented in rst .)
        # We can look at it when auto-monarch starts to use AS 4.0, now it is maintained for compatibility.
        unparsed_two_step_start = as_conf.get_parse_two_step_start()
        if unparsed_two_step_start != "":
            job_list.parse_jobs_by_filter(unparsed_two_step_start)
        Log.debug("Running job data structure")
        exp_history = get_historical_database(expid, job_list, as_conf)
        # establish the connection to all platforms
        # Restore is misleading, it is actually a "connect" function when the recover flag is not set.
        restore_platforms(job_list.submitter.platforms_object, as_conf=as_conf)
        return (
            job_list,
            submitter,
            exp_history,
            host,
            as_conf,
            job_list.submitter.platforms_object,
            False,
        )
    return (
        job_list,
        submitter,
        None,
        None,
        as_conf,
        job_list.submitter.platforms_object,
        True,
    )


def stop(
    expids: str | None,
    force=False,
    all_expids=False,
    force_all=False,
    cancel=False,
    current_status="",
    status="FAILED",
    force_yes=False,
) -> bool:
    """The stop command allows users to stop the desired experiments.

    :param expids: expids to stop
    :param force: force the stop of the experiment
    :param all_expids: stop all experiments
    :param force_all: force the stop of all experiments
    :param cancel: cancel the jobs of the experiment
    :param current_status: what status to change # defaults to all active jobs.
    :param status: status to change the active jobs to
    :param force_yes: force yes answer to prompts
    """
    from autosubmit.helpers.processes import process_id, retrieve_expids
    from autosubmit.job.job_utils import cancel_jobs

    if status not in Status.VALUE_TO_KEY.values():
        raise AutosubmitCritical(
            f"Invalid status. Expected one of {Status.VALUE_TO_KEY.keys()}", 7011
        )

    try:
        current_status = current_status.replace(",", " ").split(" ")
        current_status = [
            status.upper() for status in filter(lambda x: x, current_status)
        ]
        current_status = [Status.KEY_TO_VALUE[x.strip()] for x in current_status]
    except KeyError:
        raise AutosubmitCritical(
            f"Invalid status -fs. All values must match one "
            f"of {Status.VALUE_TO_KEY.keys()}",
            7011,
        )
    if all_expids:
        expid_list: list[str] = retrieve_expids()
    else:
        expid_list = expids.replace(",", " ").split(" ")
        expid_list = [expid.lower() for expid in filter(lambda x: x, expid_list)]

    truthy_values = ["true", "yes", "y", "1", ""]
    if not force_all:
        expid_list: list[str] = [
            expid_in_list
            for expid_in_list in expid_list
            if force_yes
            or input(
                f"Confirm stopping the experiment: {expid_in_list} (y/n)[enter=y]? "
            ).lower()
            in truthy_values
        ]

    sig_to_process = signal.SIGKILL if force else signal.SIGINT
    killed_expids = []
    for expid_in_list in expid_list:
        pid: int = process_id(expid_in_list)
        if not pid or pid <= 1:
            Log.info(f"Expid {expid_in_list} was not running")
            continue
        try:
            os.kill(pid, sig_to_process)
            killed_expids.append(expid_in_list)
        except Exception as e:
            Log.warning(
                f"An error occurred while stopping the autosubmit process for expid '{expid_in_list}': {str(e)}"
            )

    for expid_in_list in killed_expids:
        if not force:
            Log.info(f"Checking the status of the expid: {expid_in_list}")
            while True:
                if not process_id(expid_in_list):
                    Log.info(f"Expid {expid_in_list} is stopped")
                    break
                Log.info(
                    f"Waiting for the autosubmit run to safety stop: {expid_in_list}"
                )
                sleep(5)
        if cancel:
            job_list, _, _, _, _, _, _ = _prepare_run(
                expid_in_list, check_scripts=False
            )
            cancel_jobs(
                job_list, active_jobs_filter=current_status, target_status=status
            )
    return True


def monitor(
    expid: str,
    file_format: str,
    lst: str,
    filter_chunks: str,
    filter_status: str,
    filter_section: str,
    hide: bool,
    txt_only=False,
    group_by: str | None = None,
    expand="",
    expand_status="",
    hide_groups=False,
    check_wrapper=False,
    txt_logfiles=False,
) -> bool:
    """Plots workflow graph for a given experiment with status of each job coded by node colour.

    Plot is created in experiment's plot folder with name <expid>_<date>_<time>.<file_format>

    :param txt_logfiles: Whether to include log file paths in the text output.
    :param expid: Identifier of the experiment to plot.
    :param file_format: Plot file format. It can be pdf, png, ps or svg
    :param lst: list of jobs to change status
    :param filter_chunks: chunks to change status
    :param filter_status: current status of the jobs to change status
    :param filter_section: sections to change status
    :param hide: hides plot window
    :param txt_only: workflow will only be written as text
    :param group_by: workflow will only be written as text
    :param expand: Filtering of jobs for its visualisation
    :param expand_status: Filtering of jobs for its visualisation
    :param hide_groups: Simplified workflow illustration by encapsulating the jobs.
    :param check_wrapper: Shows a preview of how the wrappers will look
    :return: True if monitor was executed successfully, False otherwise
    """
    from autosubmit.monitor.monitor import Monitor

    try:
        Log.info("Getting job list...")
        as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
        as_conf.check_conf_files(False)
        # Getting output type from configuration
        output_type = as_conf.get_output_type()
        os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "db")
        job_list = load_job_list(
            expid, as_conf, monitor=True, new=False, run_mode=False
        )
        Log.debug("Job list restored from db")
    except AutosubmitError as e:
        raise AutosubmitCritical(e.message, e.code, e.trace)
    except AutosubmitCritical:
        raise
    except BaseException:
        raise

    try:
        jobs = []
        if not isinstance(job_list, type([])):
            jobs = job_list.get_job_list()

            if filter_section or filter_chunks or filter_status or lst:
                validate_job_filters(
                    as_conf,
                    job_list,
                    lst,
                    filter_chunks,
                    filter_status,
                    filter_section,
                )

                selected_job_names = apply_job_filters(
                    job_list=job_list,
                    base_job_names={job.name for job in jobs},
                    filter_section=filter_section,
                    filter_chunk=filter_chunks,
                    filter_status=filter_status,
                    filter_list=lst,
                    filter_sections_splits_fn=filter_sections_splits,
                    filter_chunks_fn=filter_jobs_by_chunks_splits,
                    status_from_str_fn=get_job_status,
                )

                jobs = [job for job in jobs if job.name in selected_job_names]
    except Exception as e:
        raise AutosubmitCritical(
            "Issues during the job_list generation. Maybe due I/O error",
            7040,
            str(e),
        )

    # WRAPPERS
    try:
        if len(as_conf.experiment_data.get("WRAPPERS", {})) > 0:
            if check_wrapper:
                job_list.clear_wrappers_db(preview=True)
                generate_scripts_andor_wrappers(as_conf, job_list, True)
            if not check_wrapper:
                job_list.run_id = get_last_run_id(expid)
            job_list.load_wrappers(preview=check_wrapper)
    except Exception as e:
        raise AutosubmitCritical(
            "Issues during the wrapper loading, may be related to IO issues",
            7040,
            str(e),
        )

    groups_dict = {}
    try:
        if group_by:
            status = []
            if expand_status:
                for s in expand_status.split():
                    status.append(get_job_status(s.upper()))

            job_grouping = JobGrouping(
                group_by,
                copy.deepcopy(jobs),
                job_list,
                expand_list=expand,
                expanded_status=status,
            )
            groups_dict = job_grouping.group_jobs()
    except Exception as e:
        raise AutosubmitCritical(
            "Jobs can't be grouped, perhaps you're using an invalid format. Take a look into readthedocs",
            7011,
            str(e),
        )

    monitor_exp = Monitor(edge_info=job_list.graph_dict_by_job_name)
    try:
        if txt_only or txt_logfiles or file_format == "txt":
            monitor_exp.generate_output_txt(
                expid,
                jobs,
                str(
                    Path(
                        BasicConfig.LOCAL_ROOT_DIR,
                        expid,
                        BasicConfig.LOCAL_TMP_DIR,
                        f"LOG_{expid}",
                    )
                ),
                txt_logfiles,
                job_list_object=job_list,
            )
            if txt_only:
                current_length = len(job_list.get_job_list())
                if current_length > 1000:
                    Log.info(
                        "Experiment has too many jobs to be printed in the terminal. Maximum job quantity is 1000, your experiment has "
                        + str(current_length)
                        + " jobs."
                    )
                else:
                    Log.info(job_list.print_with_status())
        else:
            # if file_format is set, use file_format, otherwise use conf value
            monitor_exp.generate_output(
                expid,
                jobs,
                str(
                    Path(
                        BasicConfig.LOCAL_ROOT_DIR,
                        expid,
                        BasicConfig.LOCAL_TMP_DIR,
                        f"LOG_{expid}",
                    )
                ),
                output_format=file_format
                if file_format is not None and len(str(file_format)) > 0
                else output_type,
                packages=list(job_list.job_package_map.values()),
                show=not hide,
                groups=groups_dict,
                hide_groups=hide_groups,
                job_list_object=job_list,
            )
    except Exception as e:
        raise AutosubmitCritical(
            "An error has occurred while printing the workflow status. Check if you have X11 redirection and an img viewer correctly set",
            7014,
            str(e),
        )
    return True


def _save_historical_edges(expid):
    """Function to save the historical edges to a separate historical database.

    :param expid: a string with the experiment id
    :return: None
    """
    exp_history = HistoricalDbManager(
        schema=expid, job_manager=JobsDbManager(schema=expid)
    )
    exp_history.save_historical_edges()


def _finish_current_experiment_run(expid):
    """Update the finish time of the current experiment run in the database.

    :param expid: a string with the experiment id
    :return: None
    """
    _save_historical_edges(expid)
    # TODO: Add all methods and functions to the new historical db manager
    old_exp_history = ExperimentHistory(expid)
    old_exp_history.finish_current_experiment_run()


def _process_historical_data_iteration(job_list, job_changes_tracker, expid):
    """Process the historical data for the current iteration.

    :param job_list: a JobList object.
    :param job_changes_tracker: a dictionary with the changes in the job status.
    :param expid: a string with the experiment id.
    :return: an ExperimentHistory object.
    """
    exp_history = ExperimentHistory(expid)
    if len(job_changes_tracker) > 0:
        exp_history.process_job_list_changes_to_experiment_totals(
            job_list.get_job_list()
        )
        database_backup(expid)


def run(
    expid: str,
    start_time: str | None = None,
    start_after: str | None = None,
    run_only_members: str | None = None,
    profiler: "Profiler | None" = None,
    stop_event=None,
) -> int:
    """Runs and experiment (submitting all the jobs properly and repeating its execution in case of failure).

    :param expid: the experiment id
    :param start_time: the time at which the experiment should start
    :param start_after: the expid after which the experiment should start
    :param run_only_members: the members to run
    :param profiler: The optional instance. If set, the code will run with the profiler.
    :param stop_event: optional threading.Event used to signal interruption (e.g. from tests)
    :raises BaseLockException: If the experiment is locked for another command.
    :raises AutosubmitCritical: In case of a failure during the execution of the workflow.
    :return: An integer representing the command exit status.
    """
    Scheduler.exit = False

    # TODO: We can probably delete this? The CLI validators should be checking these paths already?
    # Initialize common folders'
    try:
        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
        tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
    except Exception as e:
        raise AutosubmitCritical(
            "Failure during the loading of the experiment configuration, check file paths",
            7014,
            str(e),
        )

    with Lock(os.path.join(tmp_path, "autosubmit.lock"), timeout=1):
        try:
            Log.debug("Preparing run")
            # This function is called only once, when the experiment is started.
            # It is used to initialise the experiment and to check the correctness of the configuration files.
            # If there are issues while running, this function will be called again to reinitialise the experiment.
            (
                job_list,
                submitter,
                _exp_history,
                _host,
                as_conf,
                platforms_to_test,
                _,
            ) = _prepare_run(expid, start_time, start_after, run_only_members)
        except Exception as e:
            raise AutosubmitCritical(
                "Error in run initialization", 7014, str(e)
            )  # Changing default to 7014

        as_conf_config = as_conf.experiment_data.get("CONFIG", {})
        git_operational_check_enabled = as_conf_config.get(
            "GIT_OPERATIONAL_CHECK_ENABLED", True
        )

        if git_operational_check_enabled:
            Log.debug("Checking for dirty local Git repository")
            check_unpushed_changes(expid, as_conf)
        else:
            Log.warning("Git operational check disabled by user")

        Log.debug("Running main running loop")
        #########################
        # AUTOSUBMIT - MAIN LOOP
        #########################
        # Main loop
        # Recovery retries, when platforms have issues. The hard limit is set just in case an Autosubmit bug or
        # wrong configuration. The minimum duration is the weekend (72 h).
        # Run experiment steps:
        # 0. Prepare the experiment to start running it.
        # 1. Check if there are jobs in the workflow that have to run (get_active)
        # For each platform:
        #  2. Check the status of all jobs in the current workflow that are queuing or running. Also updates
        #  all workflow jobs status by checking the status in the platform machines and job parent status.
        #  3. Submit jobs that are on ready status.
        # 4. When there are no more active jobs, wait until all log recovery threads finish and exit Autosubmit.
        # In case of issues, the experiment is reinitialised and the process starts with the last
        # non-corrupted workflow status.
        # User can always stop the run, and unless force killed, Autosubmit will exit in a clean way.
        # Experiment run will always start from the last known workflow status.

        # 3650 = (72h - 122h)
        max_recovery_retrials = as_conf.experiment_data.get("CONFIG", {}).get(
            "RECOVERY_RETRIALS", 3650
        )
        recovery_retrials = 0
        if profiler is not None:
            loaded_jobs = len(job_list.get_job_list())
            loaded_edges = 0
            for job in job_list.get_job_list():
                loaded_edges += len(job.children)
        as_conf.set_platform_parameters(job_list, submitter.platforms)
        # Save metadata.
        as_conf.save()
        job_changes_tracker = {}
        _save_historical_edges(expid)
        job_list.recover_logs(from_db=True)
        job_list.reset_updated_logs()
        job_list.load_wrappers()
        while job_list.continue_run():
            try:
                if profiler is not None:
                    Scheduler.exit = profiler.iteration_checkpoint(
                        loaded_jobs, loaded_edges
                    )

                if stop_event and stop_event.is_set():
                    Scheduler.exit = True

                # TODO fix in another PR, this is a workaround to avoid having mismatching job_list and platform experiment_data
                if as_conf.needs_reload():
                    as_conf.reload()
                    as_conf.set_platform_parameters(job_list, submitter.platforms)
                    job_list.update_as_conf(as_conf)
                    for p in platforms_to_test:
                        p.update_as_conf(as_conf)

                # Submit ready jobs
                if len(job_list.get_ready()) > 0:
                    submit_ready_jobs(as_conf, job_list, platforms_to_test)
                    save_jobs = job_list.update_list(as_conf)
                    if save_jobs:
                        job_list.save_jobs()

                # Check wrappers status and inner jobs
                _, _wrapper_job_changes = check_wrappers(as_conf, job_list, expid)
                # Check non-wrapped jobs
                check_non_wrapped_jobs(platforms_to_test, job_list, as_conf, expid)
                # Safe spot to store changes
                try:
                    # Track all jobs change for GUI
                    job_changes_tracker = {}
                    for job in [
                        job
                        for job in job_list.get_job_list()
                        if job.prev_status is not None and job.prev_status != job.status
                    ]:
                        job_changes_tracker[job.name] = (
                            Status.VALUE_TO_KEY[job.prev_status],
                            Status.VALUE_TO_KEY[job.status],
                        )
                    _process_historical_data_iteration(
                        job_list, job_changes_tracker, expid
                    )
                except Exception:
                    Log.printlog(
                        "Historic database seems corrupted, AS will repair it and resume the run",
                        Log.INFO,
                    )
                    Log.warning(
                        "Couldn't recover the Historical database, AS will continue without it, GUI may be affected"
                    )
                if Scheduler.exit:
                    job_list.update_db_wrappers()
                    job_list.save_jobs()
                    as_conf.save()
                    break
                else:
                    safetysleeptime = as_conf.get_safetysleeptime()
                    time.sleep(safetysleeptime)

            except (
                AutosubmitError
            ) as ae:  # If an error is detected, restore all connections and job_list
                Log.error(f"Trace: {ae.trace}")
                Log.error(f"{ae.message} [eCode={ae.code}]")
                # No need to wait until the remote platform reconnection
                recovery = False
                as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
                consecutive_retrials = 1
                failed_names = {}
                Log.info("Storing failed job count...")
                try:
                    for job in job_list.get_job_list():
                        if job.fail_count > 0:
                            failed_names[job.name] = job.fail_count
                except Exception as e:
                    Log.printlog(
                        f"Error trying to store failed job count: {str(e)}",
                        Log.WARNING,
                    )
                Log.result("Storing failed job count...done")
                while not recovery and (
                    recovery_retrials < max_recovery_retrials
                    or max_recovery_retrials <= 0
                ):
                    delay = min(15 * consecutive_retrials, 120)
                    recovery_retrials += 1
                    sleep(delay)
                    consecutive_retrials = consecutive_retrials + 1
                    Log.info(f"Waiting {delay} seconds before continue")
                    try:
                        (
                            job_list,
                            submitter,
                            _,
                            _,
                            as_conf,
                            platforms_to_test,
                            recovery,
                        ) = _prepare_run(
                            expid,
                            start_time,
                            start_after,
                            run_only_members,
                            recover=True,
                            submitter=submitter,
                        )
                    except AutosubmitError as e:
                        recovery = False
                        Log.result(f"Recover of job_list has fail {e.message}")
                    except OSError as e:
                        recovery = False
                        Log.result(f"Recover of job_list has fail {str(e)}")
                    except Exception as e:
                        recovery = False
                        Log.result(f"Recover of job_list has fail {str(e)}")
                # Restore platforms and try again to avoid endless loop with failed configuration.
                # A hard limit is set.
                reconnected = False
                times = 0
                max_times = 10
                Log.info("Restoring the connection to all experiment platforms")
                consecutive_retrials = 1
                delay = min(15 * consecutive_retrials, 120)
                while not reconnected and (
                    recovery_retrials < max_recovery_retrials
                    or max_recovery_retrials <= 0
                ):
                    recovery_retrials += 1
                    Log.info("Recovering the remote platform connection")
                    Log.info(f"Waiting {delay} seconds before continue")
                    sleep(delay)
                    consecutive_retrials = consecutive_retrials + 1
                    try:
                        if times % max_times == 0:
                            mail_notify = True
                            max_times = max_times + max_times
                            times = 0
                        else:
                            mail_notify = False
                        times = times + 1
                        restore_platforms(
                            platforms_to_test,
                            mail_notify=mail_notify,
                            as_conf=as_conf,
                            expid=expid,
                        )
                        reconnected = True
                    except AutosubmitCritical as e:
                        # Message prompt by restore_platforms.
                        Log.info(
                            f"{e.message}\nCouldn't recover the platforms, retrying in 15seconds..."
                        )
                        reconnected = False
                    except OSError:
                        reconnected = False
                    except Exception:
                        reconnected = False
                if (
                    recovery_retrials == max_recovery_retrials
                    and max_recovery_retrials > 0
                ):
                    raise AutosubmitCritical(
                        f"Autosubmit Encounter too much errors during running time, limit of {max_recovery_retrials * 120} reached",
                        7051,
                        ae.message,
                    )
            except AutosubmitCritical as e:  # Critical errors can't be recovered. Failed configuration or autosubmit error
                raise AutosubmitCritical(e.message, e.code, e.trace)

        Log.result("No more jobs to run.")
        # search hint - finished run
        Log.info("Waiting for all logs to be updated")
        for p in platforms_to_test:
            p.clean_log_recovery_process()
        _process_historical_data_iteration(job_list, job_changes_tracker, expid)

        for p in platforms_to_test:
            p.close_connection()
        if len(job_list.get_failed_from_db()) > 0:
            Log.info("Some jobs have failed and reached maximum retrials")
        else:
            Log.result("Run successful")
            if profiler:
                profiler.iteration_checkpoint(
                    len(job_list.graph.nodes()), len(job_list.graph_dict)
                )
            # Updating finish time for job data header
            try:
                _finish_current_experiment_run(expid)
            except Exception as e:
                Log.warning(f"Database is locked: {str(e)}")
        rocrate_data = as_conf.experiment_data.get("ROCRATE", None)
        if rocrate_data:
            provenance(expid, create_rocrate=True)
        else:
            Log.info(
                "ROCRATE not present in experiment YAML configuration. No RO-Crate archive created."
            )

    # Suppress in case ``job_list`` was not defined yet...
    with suppress(NameError):
        if len(job_list.get_failed_from_db()) > 0:
            return 1
    return 0


def _online_recovery(
    as_conf: AutosubmitConfig,
    platforms: Iterable["ParamikoPlatform"],
    job_list: JobList,
    offline: bool = False,
) -> list[str]:
    """Return a list of completed job names recovered from the given platforms.

    Test each platform connection and collect completed job names. If a platform
    is not reachable and ``offline`` is True, recover completed jobs from the
    experiment history for that platform. On unrecoverable connection errors
    raise ``AutosubmitCritical``.

    :param as_conf: Autosubmit configuration object.
    :param platforms: Sequence of Platform objects to query.
    :param job_list: JobList used to recover completed jobs when offline.
    :param offline: If True, proceed with offline recovery when a platform is not reachable.
    :return: List of completed job names (it may be empty).
    :raises AutosubmitCritical: If a platform is unreachable and offline is False,
                                or if fetching completed job names fails.
    """
    completed_jobnames = set()
    for p in platforms:
        message = p.test_connection(as_conf)
        if not p.connected:
            if offline:
                Log.warning(
                    f"Platform {p.name} is not reachable, proceeding with offline recovery for this platform"
                )
                completed_jobnames.update(
                    job_list.recover_all_completed_jobs_from_exp_history(p)
                )
            else:
                raise AutosubmitCritical(
                    f"Couldn't connect to platform {p.name} during recovery: {message}",
                    7050,
                )
        else:
            # Fetch completed jobs from platform.
            try:
                completed_jobnames.update(p.get_completed_job_names())
            except (AutosubmitError, OSError, paramiko.SSHException) as e:
                if offline:
                    Log.warning(
                        f"Platform {p.name} failed to report completed jobs, "
                        f"proceeding with offline recovery for this platform: {e}"
                    )
                    completed_jobnames.update(
                        job_list.recover_all_completed_jobs_from_exp_history(p)
                    )
                else:
                    raise AutosubmitCritical(
                        f"Couldn't fetch completed jobs from platform {p.name} during recovery: {e}",
                        7050,
                    )

    return list(completed_jobnames)


def recover(
    expid: str,
    noplot: bool,
    save: bool,
    all_jobs: bool,
    hide: bool,
    group_by: str | None = None,
    expand: list[str] | None = None,
    expand_status: list[str] | None = None,
    detail: bool = False,
    force: bool = False,
    offline: bool = False,
    filter_list: str | None = None,
    filter_chunks: str | None = None,
    filter_status: str | None = None,
    filter_section: str | None = None,
) -> bool:
    """Recover job statuses for an experiment and update the job list.

    Return True when the recovery completed successfully.

    :param expid: Experiment identifier.
    :param noplot: If True, do not generate a plot.
    :param save: If True, persist changes to the job list.
    :param all_jobs: If True, recover all jobs; otherwise only active jobs.
    :param hide: If True, hide GUI/windows when generating plots.
    :param group_by: Optional grouping key for display.
    :param expand: Optional list of job names/sections to expand in the view.
    :param expand_status: Optional list of statuses to expand in the view.
    :param detail: If True, produce a more detailed (and more expensive) textual representation.
    :param force: If True, cancel active jobs before recovery.
    :param offline: If True, avoid connecting to remote platforms and use offline recovery.
    :param filter_list: Optional list of job names to filter for recovery.
    :param filter_chunks: Optional list of chunk identifiers to filter for recovery.
    :param filter_status: Optional list of job statuses to filter for recovery.
    :param filter_section: Optional list of job sections to filter for recovery.
    :return: True if recovery ran successfully, False otherwise.
    :raises AutosubmitCritical: On configuration/IO failures.
    """
    if not save:
        Log.warning("Changes will be NOT saved to the jobList. Use -s option to save")

    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.check_conf_files(True)
    Log.info(f"Recovering experiment {expid}")
    job_list = load_job_list(expid, as_conf, new=False, monitor=True)
    as_conf.check_conf_files(False)

    # Getting output type provided by the user in config, 'pdf' as default
    hpcarch = as_conf.get_platform()

    submitter = ParamikoSubmitter(as_conf)
    # TODO: Rebase check if this still works
    # Changed to check the platforms in used by iterating the configuration instead of the whole job_list
    platforms_to_test: set["ParamikoPlatform"] = set()
    for section_data in as_conf.jobs_data.values():
        if (
            "PLATFORM" in section_data
            and section_data["PLATFORM"] in submitter.platforms
        ):
            platforms_to_test.add(submitter.platforms[section_data["PLATFORM"]])

    if hpcarch in submitter.platforms:
        platforms_to_test.add(submitter.platforms[hpcarch])

    completed_jobnames = _online_recovery(as_conf, platforms_to_test, job_list, offline)
    current_active_jobs = job_list.get_in_queue()
    if current_active_jobs and not (force and save):
        raise AutosubmitCritical(
            f"Experiment can't be recovered due being {len(current_active_jobs)} "
            f"active jobs in your experiment, If you want to recover the experiment,"
            f" please use the flag -f and all active jobs will be cancelled. "
            f"Be warned that -f and --offline won't cancel jobs if the connection can't be established",
            7053,
        )
    elif current_active_jobs and force and save and not offline:
        all_connected = True
        for p in platforms_to_test:
            if not p.connected:
                all_connected = False
                Log.warning(f"Platform {p.name} is not reachable")
        if not all_connected:
            raise AutosubmitCritical(
                "You can use --offline and -f to avoid cancelling jobs", 7050
            )
    # TODO: https://github.com/BSC-ES/autosubmit/issues/1251 don't need force flag
    if save:
        offline_jobs = []
        for job in current_active_jobs:
            if not job.id:
                Log.warning(f"Skipping cancellation of job with invalid ID: {job.id}")
                continue

            if (
                offline
                or not job.platform.connected
                or job.platform_name not in submitter.platforms
            ):
                offline_jobs.append(job.name)
            else:
                job.platform_name = (
                    as_conf.jobs_data.get(job.section, {})
                    .get("PLATFORM", hpcarch)
                    .upper()
                )
                # noinspection PyTypeChecker
                job.platform = submitter.platforms[job.platform_name]
                try:
                    job.platform.send_command(
                        f"{job.platform.cancel_cmd} {job.id}", ignore_log=True
                    )
                except AutosubmitError as e:
                    # Not sure if this is the best way to check for invalid job id error for non-slurm
                    if "invalid job id" in e.message.lower():
                        Log.warning(
                            f"Job {job.name} could not be cancelled because it was not found on the platform"
                        )
                    else:
                        Log.warning(
                            f"Job {job.name} could not be cancelled: {e.message}"
                        )
        if offline_jobs:
            Log.warning(
                f"Jobs {''.join(offline_jobs)} could not be cancelled due to offline mode."
            )

    jobs_to_recover = job_list.get_job_list() if all_jobs else job_list.get_active()
    selected_job_names = {job.name for job in jobs_to_recover}

    # filters will be applied to all_jobs or only active_jobs, depending on the all_jobs flag
    if filter_section or filter_chunks or filter_status or filter_list:
        # Validate filters. Raises AutosubmitCritical if any filter is invalid, with a message specifying the issue.
        validate_job_filters(
            as_conf,
            job_list,
            filter_list,
            filter_chunks,
            filter_status,
            filter_section,
        )

        # Starts the filtering process
        Log.info("Filtering jobs...")

        selected_job_names = apply_job_filters(
            job_list=job_list,
            base_job_names=selected_job_names,
            filter_section=filter_section,
            filter_chunk=filter_chunks,
            filter_status=filter_status,
            filter_list=filter_list,
            filter_sections_splits_fn=filter_sections_splits,
            filter_chunks_fn=filter_jobs_by_chunks_splits,
            status_from_str_fn=get_job_status,
        )

    jobs_to_recover = [job for job in jobs_to_recover if job.name in selected_job_names]

    Log.info(f"The selected number of jobs to recover is {len(jobs_to_recover)}")

    try:
        for job in jobs_to_recover:
            if job.name in completed_jobnames:
                job.status = Status.COMPLETED
                Log.info(f"CHANGED job '{job.name}' status to COMPLETED")

            elif job.status != Status.SUSPENDED:
                job.status = Status.WAITING
                job.fail_count = 0
                job.updated_log = 0
                job.updated_stats = 0
                job.log_recovery_call_count = 0
                job.wrapper_type = None
                Log.info(f"CHANGED job '{job.name}' status to WAITING")

        job_list.check_completed_jobs_after_recovery()

        Log.info("Updating the jobs list")
        job_list.update_list(as_conf)

        if save:
            job_list.recover_last_data()
            job_list.save_jobs(reset_log_counters=True)
            job_list.save_edges()
        else:
            Log.warning("Changes NOT saved to the jobList. Use -s option to save")

        Log.result("Recovery finalized")

    except Exception as e:
        raise AutosubmitCritical(
            "Couldn't restore the experiment workflow", 7040, str(e)
        )

    # The expand/group was not covered before, in this PR it was just moved from be mandatory to optional
    # Added TRY EXCEPT for plotting and detail to avoid recovery failure (as the jobs were recovered)
    try:
        if not noplot:
            from autosubmit.monitor.monitor import Monitor

            status = []
            if group_by and expand_status:
                if isinstance(expand_status, str):
                    for s in expand_status.split():
                        status.append(get_job_status(s.upper()))
                elif isinstance(expand_status, list):
                    for s in expand_status:
                        status.append(get_job_status(s.upper()))
                else:
                    Log.warning(
                        "Grouping status has an invalid format, it should be a string or a list of strings"
                    )
            job_grouping = JobGrouping(
                group_by,
                copy.deepcopy(job_list.get_job_list()),
                job_list,
                expand_list=expand,
                expanded_status=status,
            )
            groups_dict = job_grouping.group_jobs()
            output_type = as_conf.get_output_type()

            Log.info("\nPlotting the jobs list...")
            monitor_exp = Monitor(edge_info=job_list.graph_dict_by_job_name)
            monitor_exp.generate_output(
                expid,
                job_list.get_job_list(),
                os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "tmp", f"LOG_{expid}"),
                output_format=output_type,
                packages=list(job_list.job_package_map.values()),
                show=hide,
                groups=groups_dict,
                job_list_object=job_list,
            )
    except Exception as e:
        Log.warning(
            "An error has occurred while plotting the jobs list after recovery. "
            f"Check if you have X11 redirection and an img viewer correctly set. Trace: {str(e)}"
        )
    try:
        if detail:
            print_job_details(job_list)
    except Exception as e:
        Log.warning(
            f"An error has occurred while generating the detailed view of the jobs after recovery. Trace: {str(e)}"
        )

    return True


def inspect(
    expid: str,
    lst: str,
    filter_chunks: str,
    filter_status: str,
    filter_section: str,
    force=False,
    check_wrapper=False,
    quick=False,
) -> bool:
    """Generates cmd files experiment.

    :param expid: Identifier of experiment to be run
    :param lst: Optional list of job names to filter for inspect.
    :param filter_chunks: Optional list of chunk identifiers to filter for inspect.
    :param filter_status: Optional list of job statuses to filter for inspect.
    :param filter_section: Optional list of job sections to filter for inspect.
    :param force: If true, forces the generation of all cmd files.
    :param check_wrapper: If true, checks the wrapper.
    :param quick: If true, performs a quick inspect.
    :return: True if run to the end, False otherwise
    :raises BaseLockException: If the experiment is already locked by another command.
    :raises AutosubmitCritical: If there is a critical error during the inspection process.
    """
    Log.info(f"Inspecting experiment {expid}")
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid
    tmp_path = exp_path / BasicConfig.LOCAL_TMP_DIR
    locked = (tmp_path / "autosubmit.lock").exists()
    Log.info("Starting inspect command")
    os.system("clear")
    signal.signal(signal.SIGINT, _signal_handler)
    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.check_conf_files(True)
    as_conf.get_project_type()
    safetysleeptime = as_conf.get_safetysleeptime()
    Log.debug(f"The Experiment name is: {expid}")
    Log.debug(f"Sleep: {safetysleeptime}")

    job_list = load_job_list(expid, as_conf, full_load=True)
    job_list.clear_wrappers_db(preview=True)

    job_list.packages_dict = {}
    job_list.job_package_map = {}

    Log.debug(f"Length of the jobs list: {len(job_list)}")

    # variables to be updated on the fly
    safetysleeptime = as_conf.get_safetysleeptime()
    Log.debug(f"Sleep: {safetysleeptime}")
    # Generate
    Log.info("Starting to generate cmd scripts")
    jobs = []
    jobs_cw = []

    if not isinstance(job_list, type([])):
        if check_wrapper and (not locked or (force and locked)):
            job_list.clear_wrappers_db(preview=True)
            Log.info("Generating all cmd script adapted for wrappers")
            jobs = job_list.get_uncompleted()
            if force:
                jobs_cw = job_list.get_completed()
        else:
            if locked:
                Log.warning(
                    "There is a .lock file and not -f, generating only all unsubmitted cmd scripts"
                )
                jobs = job_list.get_unsubmitted()
            elif force:
                Log.info("Overwriting all cmd scripts")
                jobs = job_list.get_job_list()
            else:
                Log.info("Generating cmd scripts only for selected jobs")
                jobs = job_list.get_job_list()
                if filter_section or filter_chunks or filter_status or lst:
                    validate_job_filters(
                        as_conf,
                        job_list,
                        lst,
                        filter_chunks,
                        filter_status,
                        filter_section,
                    )

                    selected_job_names = apply_job_filters(
                        job_list=job_list,
                        base_job_names={job.name for job in jobs},
                        filter_section=filter_section,
                        filter_chunk=filter_chunks,
                        filter_status=filter_status,
                        filter_list=lst,
                        filter_sections_splits_fn=filter_sections_splits,
                        filter_chunks_fn=filter_jobs_by_chunks_splits,
                        status_from_str_fn=get_job_status,
                    )

                    jobs = [job for job in jobs if job.name in selected_job_names]
                else:
                    jobs = job_list.get_job_list()
    if quick:
        wrapped_sections = []
        if check_wrapper:
            job_list.clear_wrappers_db(preview=True)
            for wrapper_data in as_conf.experiment_data.get("WRAPPERS", {}).values():
                if isinstance(wrapper_data, dict):
                    jobs_in_wrapper = wrapper_data.get("JOBS_IN_WRAPPER", [])
                    wrapped_sections.extend(jobs_in_wrapper)
                    wrapped_sections = list(set(wrapped_sections))
        jobs_aux = []
        sections_added = set()
        for job in jobs:
            if job.section not in sections_added or job.section in wrapped_sections:
                sections_added.add(job.section)
                jobs_aux.append(job)
        jobs = jobs_aux
        del jobs_aux
        sections_added = set()
        jobs_aux = []
        for job in jobs_cw:
            if job.section not in sections_added or job.section in wrapped_sections:
                sections_added.add(job.section)
                jobs_aux.append(job)
            jobs_cw = jobs_aux
        del jobs_aux
    file_paths = ""

    if isinstance(jobs, type([])):
        for job in jobs:
            file_paths += f"{str(tmp_path / (job.name + '.cmd'))} | {job.file}\n"
            job.status = Status.WAITING

        generate_scripts_andor_wrappers(
            as_conf, job_list, only_wrappers=False, jobs=jobs
        )
    if len(jobs_cw) > 0:
        for job in jobs_cw:
            file_paths += f"{str(tmp_path / (job.name + '.cmd'))}\n"
            job.status = Status.WAITING
        generate_scripts_andor_wrappers(as_conf, job_list, False)
    Log.info("No more scripts to generate, you can proceed to check them manually")
    Log.result(file_paths)

    return True


def statistics(
    expid: str,
    filter_type: str,
    filter_period: int,
    file_format: str,
    section_summary: bool,
    jobs_summary: bool,
    hide: bool,
) -> bool:
    """Plots statistics graph for a given experiment.

    Plot is created in the experiment's plot folder with the name
    ``<expid>_<date>_<time>.<file_format>``.

    :param expid: Experiment identifier.
    :param filter_type: Section of the jobs to plot.
    :param filter_period: Period to plot.
    :param file_format: The file format of the plot (PDF, PNG, PS, SVG).
    :param section_summary: Show summary statistics.
    :param jobs_summary: Show jobs statistics.
    :param hide: Hide the window with the plot.
    """
    import autosubmit.statistics.utils as StatisticsUtils
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job_common import Status
    from autosubmit.job.job_list import JobList
    from autosubmit.job.job_utils import SubJob, SubJobManager
    from autosubmit.monitor.monitor import Monitor

    try:
        Log.info("Loading jobs...")
        as_conf = AutosubmitConfig(expid)
        as_conf.check_conf_files(False)

        os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "db")
        job_list = load_job_list(expid, as_conf, new=False)
        for job in job_list.get_job_list():
            job.update_parameters(as_conf, set_attributes=True)
        Log.debug("Job list restored from db")
        jobs = StatisticsUtils.filter_by_section(job_list.get_job_list(), filter_type)
        jobs, period_ini, period_fi = StatisticsUtils.filter_by_time_period(
            jobs, filter_period
        )
        # Package information
        queue_time_fixes = {}
        if job_list.packages_dict:
            current_table_structure = job_list.graph_dict
            subjobs = []
            for job in job_list.get_job_list():
                # find associated_wrapper
                job_info = JobList.retrieve_times(
                    job.status,
                    job.name,
                    job._tmp_path,
                    make_exception=True,
                    job_times=None,
                    seconds=True,
                    job_data_collection=None,
                )
                time_total = (
                    (job_info.queue_time + job_info.run_time) if job_info else 0
                )
                subjobs.append(
                    SubJob(
                        job.name,
                        job_list.job_package_map.get(job.id, None),
                        job_info.queue_time if job_info else 0,
                        job_info.run_time if job_info else 0,
                        time_total,
                        job_info.status if job_info else Status.UNKNOWN,
                    )
                )
            queue_time_fixes = SubJobManager(
                subjobs,
                job_list.job_package_map,
                job_list.packages_dict,
                current_table_structure,
            ).get_collection_of_fixes_applied()

        if len(jobs) > 0:
            try:
                Log.info("Plotting stats...")
                monitor_exp = Monitor(edge_info=job_list.graph_dict_by_job_name)
                # noinspection PyTypeChecker
                report_created = monitor_exp.generate_output_stats(
                    expid,
                    jobs,
                    file_format,
                    hide,
                    section_summary,
                    jobs_summary,
                    period_ini,
                    period_fi,
                    queue_time_fixes,
                )
                report_message = (
                    "Statistics plot ready"
                    if report_created
                    else "No statistics plot produced."
                )
                Log.result(report_message)
            except Exception as e:
                raise AutosubmitCritical("Stats couldn't be shown", 7061, str(e))
        else:
            Log.info(
                f"There are no {filter_type} jobs in the period from {period_ini} to {period_fi}..."
            )
    except Exception as e:
        raise AutosubmitCritical(
            "Stats couldn't be generated. Check trace for more details",
            7061,
            str(e),
        )
    return True
