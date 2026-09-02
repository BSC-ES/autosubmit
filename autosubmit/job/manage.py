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

"""Code to manage Autosubmit jobs."""

import copy
import time
from pathlib import Path
from typing import TYPE_CHECKING

from portalocker import Lock

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.job.filters import (
    apply_job_filters,
    filter_jobs_by_chunks_splits,
    filter_sections_splits,
)
from autosubmit.job.job_common import Status, get_job_status
from autosubmit.job.job_grouping import JobGrouping
from autosubmit.job.job_list import load_job_list
from autosubmit.job.job_utils import change_jobs_status
from autosubmit.job.validation import validate_job_filters
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.platforms.manage import restore_platforms
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

if TYPE_CHECKING:
    from autosubmit.job.job import Job

__all__ = ["change_status", "set_status"]


def set_status(
    expid: str,
    noplot: bool,
    save: bool,
    final: str,
    filter_list: str,
    filter_chunks: str,
    filter_status: str,
    filter_section: str,
    filter_type_chunk: str,
    filter_type_chunk_split: str,
    hide: bool,
    group_by: str | None = None,
    expand: list | None = None,
    expand_status: str | None = None,
    check_wrapper=False,
    detail=False,
) -> bool:
    """Set status of jobs.

    :param expid: experiment id
    :param noplot: do not plot
    :param save: save
    :param final: final status
    :param filter_list: list of jobs
    :param filter_chunks: filter chunks
    :param filter_status: filter status
    :param filter_section: filter section
    :param filter_type_chunk: filter type chunk
    :param filter_type_chunk_split: filter chunk split
    :param hide: hide
    :param group_by: group by
    :param expand: Whether to expand during job grouping or not.
    :param expand_status: The status to use when expanding.
    :param check_wrapper: check wrapper
    :param detail: detail
    :return: ``True`` if executed successfully.
    :raises AutosubmitCritical: if any of the filters is malformed or if no filter is provided, with a message describing the errors found.
    """
    if filter_status:
        filter_status = filter_status.upper()
    # legacy filters
    if filter_type_chunk:
        Log.warning(
            "--filter_type_chunk is deprecated and will be removed in future versions. Use a combination of -ft and -fc."
        )
    if filter_type_chunk_split:
        Log.warning(
            "--filter_type_chunk_split is deprecated and will be removed in future versions. Use a combination of -ft and -fc."
        )
    # multiple overlapping filters selected
    provided_chunk_filters = [
        ("-fc/--filter_chunks", filter_chunks),
        ("-ftc/--filter_type_chunk", filter_type_chunk),
        ("-ftcs/--filter_type_chunk_split", filter_type_chunk_split),
    ]
    selected_chunk_filters = [name for name, value in provided_chunk_filters if value]
    if len(selected_chunk_filters) > 1:
        Log.warning(
            "Multiple chunk filters provided ({}). Using -fc first, then -ftc, and finally -ftcs."
            " Use only one of them to avoid ambiguity.".format(
                ", ".join(selected_chunk_filters)
            )
        )
    # keep retro-compatibility with legacy filters while prioritising -fc, then -ftc, and finally -ftcs
    filter_chunk_section_split = (
        filter_chunks or filter_type_chunk or filter_type_chunk_split
    )

    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid
    tmp_path = exp_path / BasicConfig.LOCAL_TMP_DIR
    try:
        with Lock(Path(tmp_path, "autosubmit.lock"), timeout=1):
            Log.info(
                "Preparing .lock file to avoid multiple instances with same expid."
            )

            Log.debug(f"Exp ID: {expid}")
            Log.debug(f"Save: {save}")
            Log.debug(f"Final status: {final}")
            Log.debug(f"List of jobs to change: {filter_list}")
            Log.debug(f"Chunks to change: {filter_chunk_section_split}")
            Log.debug(f"Status of jobs to change: {filter_status}")
            Log.debug(f"Sections to change: {filter_section}")

            as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
            as_conf.check_conf_files(True)

            # Getting output type from configuration
            output_type = as_conf.get_output_type()
            # Getting db connections
            # To be added in a function that checks which platforms must be connected to
            job_list = load_job_list(expid, as_conf, monitor=True, new=False)
            submitter = ParamikoSubmitter(as_conf=as_conf)
            hpcarch = as_conf.get_platform()
            for job in job_list.get_job_list():
                job.platform_name = (
                    as_conf.jobs_data.get(job.section, {}).get("PLATFORM", "").upper()
                )
                if not job.platform_name:
                    job.platform_name = hpcarch
                # noinspection PyTypeChecker
                job.platform = submitter.platforms[job.platform_name]
            platforms_to_test = set()
            platforms = submitter.platforms
            for job in job_list.get_job_list():
                job.submitter = submitter
                if job.platform_name is None:
                    job.platform_name = hpcarch
                # noinspection PyTypeChecker
                job.platform = platforms[job.platform_name]
                # noinspection PyTypeChecker
                if job.status in [Status.QUEUING, Status.SUBMITTED, Status.RUNNING]:
                    platforms_to_test.add(platforms[job.platform_name])
            # establish the connection to all platforms
            definitive_platforms = []
            for platform in platforms_to_test:
                try:
                    restore_platforms([platform], as_conf=as_conf)
                    definitive_platforms.append(platform.name)
                except Exception:
                    pass
            ##### End of the ""function""
            # This will raise an autosubmit critical if any of the filters has issues in the format specified by the user
            validate_job_filters(
                as_conf,
                job_list,
                filter_list,
                filter_chunk_section_split,
                filter_status,
                filter_section,
            )
            #### Starts the filtering process ####
            jobs_to_set_status = job_list.get_job_list()
            selected_job_names = {job.name for job in jobs_to_set_status}
            final_status = get_job_status(final)
            if final_status is None:
                raise AutosubmitCritical(
                    f"Invalid status '{final}'. Expected one of {Status.VALUE_TO_KEY.keys()}",
                    7011,
                )
            if final_status in Status.ACTIVE:
                raise AutosubmitCritical(
                    f"Cannot set jobs to the active status '{final}'. Active statuses "
                    f"(SUBMITTED, QUEUING, RUNNING) cannot be set via set_status.",
                    7011,
                )

            Log.info("Filtering jobs...")

            selected_job_names = apply_job_filters(
                job_list=job_list,
                base_job_names=selected_job_names,
                filter_section=filter_section,
                filter_chunk=filter_chunk_section_split,
                filter_status=filter_status,
                filter_list=filter_list,
                filter_sections_splits_fn=filter_sections_splits,
                filter_chunks_fn=filter_jobs_by_chunks_splits,
                status_from_str_fn=get_job_status,
            )

            # preserve job list ordering
            final_list = [
                job for job in jobs_to_set_status if job.name in selected_job_names
            ]
            # Time to change status
            Log.info(f"The selected number of jobs to change is: {len(final_list)}")
            performed_changes = change_status(
                final, final_status, final_list, save, definitive_platforms
            )

            if performed_changes:
                if detail:
                    current_length = len(job_list.get_job_list())
                    if current_length > 1000:
                        Log.warning(
                            "-d option: Experiment has too many jobs to be printed in the terminal. Maximum job quantity is 1000, your experiment has "
                            + str(current_length)
                            + " jobs."
                        )
                    else:
                        Log.info(
                            job_list.print_with_status(status_change=performed_changes)
                        )
            else:
                Log.warning("No changes were performed.")
            Log.info(f"Updating JobList for experiment {expid}...")
            job_list.update_list(as_conf, False, True)
            start = time.time()
            if save:
                if final_status in job_list._FINAL_STATUSES:
                    job_list.recover_last_data(final_list)
                job_list.save_jobs(reset_log_counters=final_status in Status.RE_RUNNABLE)
                end = time.time()
                Log.info(f"JobList saved in {end - start:.2f} seconds.")
                exp_history = ExperimentHistory(expid)
                exp_history.initialize_database()
                exp_history.process_status_changes(
                    job_list.get_job_list(),
                    chunk_unit=as_conf.get_chunk_size_unit(),
                    chunk_size=as_conf.get_chunk_size(),
                    current_config=as_conf.get_full_config_as_json(),
                )
                # TODO: No more database backup? https://github.com/BSC-ES/autosubmit/issues/3179
                # Autosubmit.database_backup(expid)
            else:
                Log.printlog(
                    "Changes NOT saved to the JobList!!!!:  use -s option to save", 3000
                )
            # Visualization stuff that should be in a function common to monitor , create, -cw flag, inspect and so on
            if not noplot:
                from autosubmit.monitor.monitor import Monitor

                groups_dict = {}
                if group_by:
                    status = []
                    if expand_status:
                        for s in expand_status.split():
                            status.append(get_job_status(s.upper()))

                    job_grouping = JobGrouping(
                        group_by,
                        copy.deepcopy(job_list.get_job_list()),
                        job_list,
                        expand_list=expand,
                        expanded_status=status,
                    )
                    groups_dict = job_grouping.group_jobs()
                Log.info("\nPlotting joblist...")
                monitor_exp = Monitor(edge_info=job_list.graph_dict_by_job_name)
                monitor_exp.generate_output(
                    expid,
                    job_list.get_job_list(),
                    str(Path(exp_path, "tmp", "LOG_" + expid)),
                    output_format=output_type,
                    packages=list(job_list.job_package_map.values()),
                    show=not hide,
                    groups=groups_dict,
                    job_list_object=job_list,
                )
            return True
    except Exception:
        raise


def change_status(
    final: str,
    final_status: int,
    final_list: list["Job"],
    save: bool,
    definitive_platforms: list[str],
) -> dict[str, str]:
    """Apply a status change to all jobs in final_list and cancel active jobs on their platforms.

    Iterates over ``final_list``, skipping jobs already at ``final_status``. For jobs whose
    current status is ACTIVE, the platform connection is verified against ``definitive_platforms``
    and the job is skipped if it cannot be reached. The actual cancellation (batched per
    platform) and status application are delegated to :func:`change_jobs_status`.

    :param final: Human-readable name of the target status.
    :param final_status: Numeric status value to assign.
    :param final_list: Jobs selected for the status change.
    :param save: Whether changes should be persisted.
    :param definitive_platforms: Names of platforms with a confirmed connection.
    :return: Mapping of job name to ``"old_status -> new_status"`` strings.
    """
    job_status_pairs: list[tuple["Job", int]] = []
    for job in final_list:
        if job.status == final_status:
            continue
        if save and job.status in Status.ACTIVE:
            if job.platform.name not in definitive_platforms:
                Log.error(
                    f"Cannot change status of job [{job.name}] because the connection to its "
                    f"platform [{job.platform.name}] could not be established. "
                    f"Please check the platform connection and try again.",
                    7013,
                )
                Log.error(
                    f"Job [{job.name}] status will remain as {Status.VALUE_TO_KEY[job.status]}."
                )
                continue
        job_status_pairs.append((job, final_status))

    performed_changes = change_jobs_status(job_status_pairs, cancel_active=save)
    for job_name in performed_changes:
        Log.info(f"CHANGED: job: {job_name} status to: {final}")
        Log.status(f"CHANGED: job: {job_name} status to: {final}")
    return performed_changes
