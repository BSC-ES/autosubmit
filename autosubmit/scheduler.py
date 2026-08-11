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

"""Autosubmit Scheduler.

It is responsible for the main-loop of the workflow manager, controlling
the workflow loading and task scheduling.
"""

from typing import TYPE_CHECKING

from autosubmit.helpers.utils import check_jobs_file_exists
from autosubmit.job.job_common import Status
from autosubmit.job.job_packager import JobPackager
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job_list import JobList

__all__ = [
    "Scheduler",
    "generate_scripts_andor_wrappers",
    "submit_ready_jobs",
]


def generate_scripts_andor_wrappers(
    as_conf: "AutosubmitConfig", job_list, only_wrappers=False, jobs=None
) -> None:
    """TODO: Add docs. And review if this function and check_deadlock belong to scheduler.

    :param as_conf: Class that handles basic configuration parameters of Autosubmit.
    :param job_list: Representation of the jobs of the experiment, keeps the list of jobs inside.
    :param only_wrappers: True when coming from Autosubmit.create(). False when coming from Autosubmit.inspect(),
    :param jobs: Optional list of jobs to process.
    :return: Nothing
    """
    # We don't want to store inspect/-cw related jobs and edges stuff
    job_list.disable_save = True
    Log.warning("Generating the auxiliary job_list used for the -CW flag.")
    date_list = as_conf.get_date_list()
    if len(date_list) != len(set(date_list)):
        raise AutosubmitCritical("There are repeated start dates!", 7014)
    wrapper_jobs = {}
    for wrapper_section, wrapper_data in as_conf.experiment_data.get(
        "WRAPPERS", {}
    ).items():
        if type(wrapper_data) is not dict:
            continue
        wrapper_jobs[wrapper_section] = as_conf.get_wrapper_jobs(wrapper_data)
    Log.info("Aux Job_list was generated successfully")

    # Load platforms.
    submitter = ParamikoSubmitter(as_conf=as_conf)
    hpcarch = as_conf.get_platform()

    as_conf.set_platform_parameters(job_list, submitter.platforms)
    platforms_to_test = set()
    for job in job_list.get_job_list():
        if job.platform_name == "" or job.platform_name is None:
            job.platform_name = hpcarch
        job.platform = submitter.platforms[job.platform_name]
        if job.platform is not None and job.platform != "":
            platforms_to_test.add(job.platform)
    job_list.update_list(as_conf, False)
    # Loading parameters again
    as_conf.set_platform_parameters(job_list, submitter.platforms)
    # Related to TWO_STEP_START new variable defined in expdef
    # TODO: For another day, this was a workaround in AS 3 to fake dependencies for crossdate, this should not be longer neccesary
    unparsed_two_step_start = as_conf.get_parse_two_step_start()
    if unparsed_two_step_start != "":
        job_list.parse_jobs_by_filter(unparsed_two_step_start)

    for job in job_list.get_job_list():
        if job.status != Status.WAITING and job.status != Status.READY:
            job.status = Status.WAITING
        job.update_parameters(as_conf, set_attributes=True, reset_logs=False)
    while job_list.get_active():
        submit_ready_jobs(as_conf, job_list, platforms_to_test, True, only_wrappers)
        for job in job_list.get_job_list():
            if job.wrapper_type is not None:
                job.status = Status.COMPLETED
        job_list.update_list(as_conf, False)
    for job in job_list.get_job_list():
        job.status = Status.WAITING
    job_list.disable_save = False


def check_deadlock(
    wrapper_errors: dict, any_job_submitted: bool, job_list: "JobList"
) -> None:
    """Check for deadlock situations and raise an exception if detected.

    :param wrapper_errors: Dictionary of wrapper errors.
    :param any_job_submitted: Boolean indicating if any job was submitted.
    :param job_list: Job list object containing the jobs.
    """
    if wrapper_errors and not any_job_submitted and len(job_list.get_in_queue()) == 0:
        # Deadlock situation
        err_msg = ""
        for wrapper in wrapper_errors:
            err_msg += f"wrapped_jobs:{wrapper} in {wrapper_errors[wrapper]}\n"
        raise AutosubmitCritical(err_msg, 7014)


def submit_ready_jobs(
    as_conf: "AutosubmitConfig",
    job_list: "JobList",
    platforms_to_test: list["ParamikoPlatform"],
    inspect=False,
    only_wrappers=False,
) -> None:
    """Gets READY jobs and send them to the platforms if there is available space on the queues.

    :param as_conf: autosubmit config object
    :param job_list: job list to check
    :param platforms_to_test: platforms used
    :param inspect: True if coming from generate_scripts_andor_wrappers().
    :param only_wrappers: True if it comes from create -cw, False if it comes from inspect -cw.
    :return: True if at least one job was submitted, False otherwise
    """
    wrapper_errors = {}
    any_job_submitted = False
    # Check section jobs
    if not only_wrappers and not inspect:
        jobs_section = {job.section for job in job_list.get_ready()}
        for section in jobs_section:
            if check_jobs_file_exists(as_conf, section):
                raise AutosubmitCritical(
                    f"Job {section} does not have a correct template// template not found",
                    7014,
                )

    for platform_interface in platforms_to_test:
        packager = JobPackager(as_conf, platform_interface, job_list)
        packages_to_submit = packager.build_packages()

        scripts_to_submit_by_section, x11_scripts_to_submit_by_section = (
            platform_interface.prepare_submission(
                as_conf,
                job_list,
                packages_to_submit,
                inspect=inspect,
                only_wrappers=only_wrappers,
            )
        )
        if not only_wrappers and not inspect:
            if scripts_to_submit_by_section:
                for (
                    section,
                    scripts_to_submit_by_name,
                ) in scripts_to_submit_by_section.items():
                    try:
                        platform_interface.process_ready_jobs(scripts_to_submit_by_name)
                        any_job_submitted = True
                    except Exception:
                        job_list.save_jobs()
                        raise
                job_list.save_jobs()

            if x11_scripts_to_submit_by_section:
                for section, x11_scripts in x11_scripts_to_submit_by_section.items():
                    # X11 only works sequentially, so we need to process them one by one, and not in parallel by section like the normal scripts.
                    for script_name, package in x11_scripts.items():
                        try:
                            platform_interface.process_ready_jobs(
                                {script_name: package}
                            )
                            any_job_submitted = True
                        except Exception:
                            if not inspect:
                                job_list.save_jobs()
                            raise
                job_list.save_jobs()

        wrapper_errors.update(packager.wrappers_with_error)
        job_list.save_wrappers(scripts_to_submit_by_section, as_conf, preview=inspect)

    check_deadlock(wrapper_errors, any_job_submitted, job_list)


class Scheduler:
    exit = False
    """Whether the scheduler should exit."""
