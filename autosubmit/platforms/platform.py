# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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

import atexit
import multiprocessing
import os
import time
import traceback
from contextlib import suppress
from multiprocessing.process import BaseProcess
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
# noinspection PyProtectedMember
from os import _exit  # type: ignore
from pathlib import Path
from typing import Any, Optional, Union, TYPE_CHECKING

import setproctitle

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.helpers.parameters import autosubmit_parameter
from autosubmit.job.job_common import Status
from autosubmit.log.log import Log

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job_packages import JobPackageBase
    from autosubmit.job.job import Job
    from autosubmit.job.job_list import JobList
    from multiprocessing.process import BaseProcess


def _init_logs_log_process(as_conf: 'AutosubmitConfig', platform_name: str) -> None:
    Log.set_console_level(as_conf.experiment_data.get("LOG_RECOVERY_CONSOLE_LEVEL", "DEBUG"))
    if as_conf.experiment_data.get("ROOTDIR", None):
        aslogs_path = Path(as_conf.experiment_data["ROOTDIR"], "tmp/ASLOGS")
        Log.set_file(
            str(aslogs_path / f'{platform_name.lower()}_log_recovery.log'), "out",
            as_conf.experiment_data.get("LOG_RECOVERY_FILE_LEVEL", "EVERYTHING"))
        Log.set_file(str(aslogs_path / f'{platform_name.lower()}_log_recovery_err.log'), "err")


def recover_platform_job_logs_wrapper(
        platform: 'Platform',
        recovery_queue: Queue,
        worker_event: Event,
        cleanup_event: Event,
        as_conf: 'AutosubmitConfig'
) -> None:
    """Wrapper function to recover platform job logs.

    :param platform: The platform object responsible for managing the connection and job recovery.
    :param recovery_queue: A multiprocessing queue used to store jobs for recovery.
    :param worker_event: An event to signal work availability.
    :param cleanup_event: An event to signal cleanup operations.
    :param as_conf: The Autosubmit configuration object containing experiment data.
    :type as_conf: AutosubmitConfig
    :return: None
    :rtype: None
    """
    platform.recovery_queue = recovery_queue
    platform.work_event = worker_event
    platform.cleanup_event = cleanup_event
    BasicConfig.read()
    _init_logs_log_process(as_conf, platform.name)
    platform.recover_platform_job_logs(as_conf)
    # Exit userspace after manually closing ssh sockets, recommended for child processes,
    # the queue() and shared signals should be in charge of the main process.
    _exit(0)


class CopyQueue(Queue):
    """
    A queue that copies the object gathered.
    """

    def __init__(self, maxsize: int = -1, block: bool = True, timeout: float = None, ctx: Any = None) -> None:
        """Initializes the Queue.

        :param maxsize: Maximum size of the queue. Defaults to -1 (infinite size).
        :type maxsize: int
        :param block: Whether to block when the queue is full. Defaults to True.
        :type block: bool
        :param timeout: Timeout for blocking operations. Defaults to None.
        :type timeout: float
        :param ctx: Context for the queue. Defaults to None.
        :type ctx: Context
        """
        self.block = block
        self.timeout = timeout
        super().__init__(maxsize, ctx=ctx)

    def put(self, job: Any, block: bool = True, timeout: Optional[float] = None) -> None:
        """Puts a job into the queue if it is not a duplicate.

        :param job: The job to be added to the queue.
        :type job: Any
        :param block: Whether to block when the queue is full. Defaults to True.
        :type block: bool
        :param timeout: Timeout for blocking operations. Defaults to None.
        :type timeout: float
        """
        job_data: dict = {
            "id": job.id,
            "name": job.name,
            "fail_count": job.fail_count,
            "submit_time_timestamp": job.submit_time_timestamp,
            "start_time_timestamp": job.start_time_timestamp,
            "finish_time_timestamp": job.finish_time_timestamp,
            "wrapper_type": job.wrapper_type,
        }
        super().put(job_data, block, timeout)


class Platform:
    """
    Class to manage the connections to the different platforms.
    """
    # This is a list of the keep_alive events, used to send the signal outside the main loop of Autosubmit
    worker_events: list[Event] = []
    # Shared lock between the main process and a retrieval log process
    lock = multiprocessing.Lock()

    def __init__(self, expid: str, name: str, config: dict, auth_password: Optional[Union[str, list[str]]] = None):
        """Initializes the Platform object with the given experiment ID, platform name, configuration,
        and optional authentication password for two-factor authentication.

        :param expid: The experiment ID associated with the platform.
        :type expid: str
        :param name: The name of the platform.
        :type name: str
        :param config: Configuration dictionary containing platform-specific settings.
        :type config: dict
        :param auth_password: Optional password for two-factor authentication.
        :type auth_password: str or list, optional
        """
        self._atexit_registered = False
        self.processed_wrapper_logs = None
        self.ctx = self.get_mp_context()
        self.connected = False
        self.expid: str = expid
        self._name: str = name
        self.config = config
        self.tmp_path = os.path.join(
            self.config.get("LOCAL_ROOT_DIR", ""), self.expid, self.config.get("LOCAL_TMP_DIR", ""))
        self._serial_platform = None
        self._serial_queue = None
        self._serial_partition = None
        self._default_queue = None
        self._partition = None
        self.ec_queue = "hpc"
        self.processors_per_node = None
        self.scratch_free_space = None
        self.custom_directives = None
        self._host = ''
        self._user = ''
        self._project = ''
        self._budget = ''
        self._reservation = ''
        self._exclusivity = ''
        self._type = ''
        self._scratch = ''
        self._project_dir = ''
        self.temp_dir = ''
        self._root_dir = ''
        self.service = None
        self.scheduler = None
        self.directory = None
        self._hyperthreading = False
        self.max_wallclock = '2:00'
        self.total_jobs = 20
        self.max_processors = "480"
        self._allow_arrays = False
        self._allow_wrappers = False
        self._allow_python_jobs = True
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._submit_hold_cmd = None
        self._submit_command_name = None
        self._submit_cmd = None
        self._submit_cmd_x11 = None
        self._checkhost_cmd = None
        self.cancel_cmd = None
        self.otp_timeout = None
        self.otp_timeout = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("2FA_TIMEOUT", 60 * 5)
        self.two_factor_auth = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("2FA", False)
        self.two_factor_method = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("2FA_METHOD", "token")
        if auth_password is not None and self.two_factor_auth:
            if isinstance(auth_password, list):
                self.pw = auth_password[0]
            else:
                self.pw = auth_password
        else:
            self.pw = None
        self.max_waiting_jobs = 20
        self.recovery_queue: Optional[Queue] = None
        self.work_event: Optional[Event] = None
        self.cleanup_event: Optional[Event] = None
        self.log_retrieval_process_active: bool = False
        self.log_recovery_process: Optional['BaseProcess'] = None
        self.keep_alive_timeout = 60 * 5  # Useful in case of kill -9
        self.compress_remote_logs = False
        self.remote_logs_compress_type = "gzip"
        self.compression_level = 9
        log_queue_size = 200
        if self.config:
            platform_config: dict = self.config.get("PLATFORMS", {}).get(self.name.upper(), {})
            # We still support TOTALJOBS and TOTAL_JOBS for backwards compatibility... # TODO change in 4.2, I think
            default_queue_size = self.config.get("CONFIG", {}).get("LOG_RECOVERY_QUEUE_SIZE", 100)
            platform_default_queue_size = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get(
                "LOG_RECOVERY_QUEUE_SIZE", default_queue_size)
            config_total_jobs = self.config.get("CONFIG", {}).get("TOTAL_JOBS", platform_default_queue_size)
            platform_total_jobs = self.config.get("PLATFORMS", {}).get('TOTAL_JOBS', config_total_jobs)
            log_queue_size = int(platform_total_jobs) * 2
            self.compress_remote_logs = platform_config.get("COMPRESS_REMOTE_LOGS", False)
            self.remote_logs_compress_type = platform_config.get("REMOTE_LOGS_COMPRESS_TYPE", "gzip")
            self.compression_level = platform_config.get("COMPRESSION_LEVEL", 9)

        self.log_queue_size = log_queue_size
        self.remote_log_dir = None
        self.has_scheduler = True

    @classmethod
    def update_workers(cls, event_worker):
        # This is visible on all instances simultaneously. Is to send the keep alive signal.
        if event_worker is None:
            return
        with cls.lock:
            try:
                if event_worker in cls.worker_events:
                    cls.worker_events.append(event_worker)
            except ValueError:
                pass
            except AttributeError as e:
                Log.warning(f"The event couldn't be stored, event has an invalid state: \n{str(e)}")

    @classmethod
    def remove_workers(cls, event_worker: Event) -> None:
        """Remove the given even worker from the list of workers in this class. """
        if event_worker is None:
            return
        with cls.lock:
            try:
                if event_worker in cls.worker_events:
                    cls.worker_events.remove(event_worker)
            except ValueError:
                pass
            except AttributeError as e:
                Log.warning(f"The event couldn't be removed, event has an invalid state: \n{str(e)}")

    @property
    @autosubmit_parameter(name='current_arch')
    def name(self):
        """Platform name. """
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    @autosubmit_parameter(name='current_host')
    def host(self):
        """Platform url. """
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    @property
    @autosubmit_parameter(name='current_user')
    def user(self):
        """Platform user. """
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    @autosubmit_parameter(name='current_proj')
    def project(self):
        """Platform project. """
        return self._project

    @project.setter
    def project(self, value):
        self._project = value

    @property
    @autosubmit_parameter(name='current_budg')
    def budget(self):
        """Platform budget. """
        return self._budget

    @budget.setter
    def budget(self, value):
        self._budget = value

    @property
    @autosubmit_parameter(name='current_reservation')
    def reservation(self):
        """You can configure your reservation id for the given platform. """
        return self._reservation

    @reservation.setter
    def reservation(self, value):
        self._reservation = value

    @property
    @autosubmit_parameter(name='current_exclusivity')
    def exclusivity(self):
        """True if you want to request exclusivity nodes. """
        return self._exclusivity

    @exclusivity.setter
    def exclusivity(self, value):
        self._exclusivity = value

    @property
    @autosubmit_parameter(name='current_hyperthreading')
    def hyperthreading(self):
        """TODO"""
        return self._hyperthreading

    @hyperthreading.setter
    def hyperthreading(self, value):
        self._hyperthreading = value

    @property
    @autosubmit_parameter(name='current_type')
    def type(self):
        """Platform scheduler type. """
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    @autosubmit_parameter(name='current_scratch_dir')
    def scratch(self):
        """Platform's scratch folder path. """
        return self._scratch

    @scratch.setter
    def scratch(self, value):
        self._scratch = value

    @property
    @autosubmit_parameter(name='current_proj_dir')
    def project_dir(self):
        """Platform's project folder path. """
        return self._project_dir

    @project_dir.setter
    def project_dir(self, value):
        self._project_dir = value

    @property
    @autosubmit_parameter(name='current_rootdir')
    def root_dir(self):
        """Platform's experiment folder path. """
        return self._root_dir

    @root_dir.setter
    def root_dir(self, value):
        self._root_dir = value

    def prepare_submission(self, as_conf: 'AutosubmitConfig', job_list: 'JobList', packages_to_submit: list['JobPackageBase'],
                           inspect=False, only_wrappers=False) -> tuple[dict[str, dict[str, 'JobPackageBase']], dict[str, dict[str, 'JobPackageBase']]]:
        """Prepare job packages for submission on the current platform.

        Log the number of ready jobs, optionally initialize the platform submit
        script, and process each package selected for submission. Depending on
        the ``inspect`` and ``only_wrappers`` flags, this method updates wrapper
        metadata, generates job scripts, transfers files to the platform, and
        collects the jobs prepared for later submission handling.

        :param as_conf: Autosubmit configuration for the current experiment.
        :type as_conf: AutosubmitConfig
        :param job_list: Job container used to inspect ready jobs and register
            wrapper information.
        :type job_list: JobList
        :param packages_to_submit: Packages built for this platform and ready to
            be prepared.
        :type packages_to_submit: list[JobPackageBase]
        :param inspect: If ``True``, prepare packages for inspect mode without
            generating the platform submit script or sending files.
        :type inspect: bool
        :param only_wrappers: If ``True``, prepare only wrapper-related metadata
            and skip the regular package submission flow.
        :type only_wrappers: bool
        :raises Exception: Propagate any exception raised while preparing packages, generating scripts, or transferring files.
        :return: A list containing the jobs gathered while preparing the given
            packages for submission.
        :rtype: list

        """
        Log.debug(f"\nJobs ready for {self.name}: {len(job_list.get_ready(self))}")
        # Submitting by sections allows to detect Scheduler misconfiguration derived from a bad configuration without submitting any job.
        scripts_to_submit_by_section: dict[str, dict[str, 'JobPackageBase']] = {}
        x11_scripts_to_submit_by_section: dict[str, dict[str, 'JobPackageBase']] = {}
        for package in packages_to_submit:
            self.prepare_dry_run_if_applicable(job_list, package, only_wrappers, inspect, as_conf)
            if not only_wrappers:
                package.generate_scripts(as_conf, inspect)
                if not inspect:
                    package.send_files()
                    if package.x11:
                        x11_scripts_to_submit_by_section.setdefault(package.sections, {})[
                            f"{package.name}.cmd"] = package
                    else:
                        scripts_to_submit_by_section.setdefault(package.sections, {})[f"{package.name}.cmd"] = package

        return scripts_to_submit_by_section, x11_scripts_to_submit_by_section

    @staticmethod
    def prepare_dry_run_if_applicable(job_list: 'JobList', package: 'JobPackageBase', only_wrappers: bool,
                                      inspect: bool, as_conf: 'AutosubmitConfig') -> None:
        """Dry-run preparation of a package to emulate that the package was submitted, without following the normal submission flow.

        :param job_list: Job container used to register wrapper package and job
            mappings.
        :type job_list: JobList
        :param package: Package being prepared for inspect or wrapper-only mode.
        :type package: JobPackageBase
        :param only_wrappers: If ``True``, prepare wrapper metadata without
            following the normal submission flow.
        :type only_wrappers: bool
        :param inspect: If ``True``, prepare package metadata for inspect mode.
        :type inspect: bool
        :param as_conf: Autosubmit configuration for the current experiment.
        :type as_conf: AutosubmitConfig
        :raises Exception: Propagate any exception raised while creating wrapper
            job metadata or saving the package.
        """

        if only_wrappers or inspect:
            # Now name is used for submit scripts, before it was used to determine if a package had a wrapper or not
            if package.is_wrapped:
                job_list.packages_dict[package.name] = package.jobs
                from ..job.job import WrapperJob
                wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.READY, 0,
                                         package.jobs, package._wallclock, package._num_processors, package.platform,
                                         as_conf)
                job_list.job_package_map[package.jobs[0].id] = wrapper_job
                job_list.save_wrappers([package], as_conf, inspect)
            for innerJob in package._jobs:
                # Setting status to COMPLETED, so it does not get stuck in the loop that calls this function
                innerJob.status = Status.COMPLETED
                innerJob.updated_log = False

    @property
    def serial_platform(self):
        """Platform to use for serial jobs.

        :return: platform's object
        :rtype: platform
        """
        if self._serial_platform is None:
            return self
        return self._serial_platform

    @serial_platform.setter
    def serial_platform(self, value):
        self._serial_platform = value

    @property
    @autosubmit_parameter(name='current_partition')
    def partition(self):
        """Partition to use for jobs.

        :return: queue's name
        :rtype: str
        """
        if self._partition is None:
            return ''
        return self._partition

    @partition.setter
    def partition(self, value):
        self._partition = value

    @property
    def queue(self):
        """Queue to use for jobs.

        :return: queue's name
        :rtype: str
        """
        if self._default_queue is None or self._default_queue == "":
            return ''
        return self._default_queue

    @queue.setter
    def queue(self, value):
        self._default_queue = value

    @property
    def serial_partition(self):
        """Partition to use for serial jobs.

        :return: partition's name
        :rtype: str
        """
        if self._serial_partition is None or self._serial_partition == "":
            return self.partition
        return self._serial_partition

    @serial_partition.setter
    def serial_partition(self, value):
        self._serial_partition = value

    @property
    def serial_queue(self):
        """Queue to use for serial jobs.

        :return: queue's name
        :rtype: str
        """
        if self._serial_queue is None or self._serial_queue == "":
            return self.queue
        return self._serial_queue

    @serial_queue.setter
    def serial_queue(self, value):
        self._serial_queue = value

    @property
    def allow_arrays(self):
        if type(self._allow_arrays) is bool and self._allow_arrays:
            return True
        return self._allow_arrays == "true"

    @property
    def allow_wrappers(self):
        if type(self._allow_wrappers) is bool and self._allow_wrappers:
            return True
        return self._allow_wrappers == "true"

    @property
    def allow_python_jobs(self):
        if type(self._allow_python_jobs) is bool and self._allow_python_jobs:
            return True
        return self._allow_python_jobs == "true"

    def add_parameters(self, as_conf: 'AutosubmitConfig'):
        """Add parameters for the current platform to the given parameters list

        :param as_conf: autosubmit config object
        :type as_conf: AutosubmitConfig object
        """

        as_conf.experiment_data['HPCARCH'] = self.name
        as_conf.experiment_data['HPCHOST'] = self.host
        as_conf.experiment_data['HPCQUEUE'] = self.queue
        as_conf.experiment_data['HPCEC_QUEUE'] = self.ec_queue
        as_conf.experiment_data['HPCPARTITION'] = self.partition

        as_conf.experiment_data['HPCUSER'] = self.user
        as_conf.experiment_data['HPCPROJ'] = self.project
        as_conf.experiment_data['HPCBUDG'] = self.budget
        as_conf.experiment_data['HPCRESERVATION'] = self.reservation
        as_conf.experiment_data['HPCEXCLUSIVITY'] = self.exclusivity
        as_conf.experiment_data['HPCTYPE'] = self.type
        as_conf.experiment_data['HPCSCRATCH_DIR'] = self.scratch
        as_conf.experiment_data['HPCTEMP_DIR'] = self.temp_dir
        if self.temp_dir is None:
            self.temp_dir = ''

    def send_file(self, filename: str, check=True) -> bool:
        """Sends a local file to the platform.

        :param filename: The name of the file to send.
        :param check: Whether the platform must perform tests (e.g. for permission).
        """
        raise NotImplementedError  # pragma: no cover

    def move_file(self, src, dest):
        """Moves a file on the platform.

        :param src: source name
        :type src: str
        :param dest: destination name
        :type dest: str
        """
        raise NotImplementedError  # pragma: no cover

    def get_file(self, filename, must_exist=True, relative_path='', ignore_log=False, wrapper_failed=False):
        """Copies a file from the current platform to experiment's tmp folder

        :param wrapper_failed:
        :param ignore_log:
        :param filename: file name
        :type filename: str
        :param must_exist: If True, raises an exception if file can not be copied
        :type must_exist: bool
        :param relative_path: relative path inside tmp folder
        :type relative_path: str
        :return: True if file is copied successfully, false otherwise
        :rtype: bool
        """
        raise NotImplementedError  # pragma: no cover

    def get_files(self, files, must_exist=True, relative_path=''):
        """Copies some files from the current platform to experiment's tmp folder.

        :param files: file names
        :type files: [str]
        :param must_exist: If True, raises an exception if file can not be copied
        :type must_exist: bool
        :param relative_path: relative path inside tmp folder
        :type relative_path: str
        :return: True if file is copied successfully, false otherwise
        :rtype: bool
        """
        for filename in files:
            self.get_file(filename, must_exist, relative_path)

    def delete_file(self, filename: str):
        """Deletes a file from this platform.

        :param filename: file name
        :type filename: str
        :return: True if successful or file does not exist
        :rtype: bool
        """
        raise NotImplementedError  # pragma: no cover

    # Executed when calling from Job
    def get_logs_files(self, exp_id: str, remote_logs: tuple[str, str]) -> None:
        """Get the given LOGS files.

        :param exp_id: experiment id
        :type exp_id: str
        :param remote_logs: names of the log files
        :type remote_logs: (str, str)
        """
        raise NotImplementedError  # pragma: no cover

    def get_checkpoint_files(self, job):
        """Get all the checkpoint files of a job.

        :param job: Get the checkpoint files
        :type job: Job
        """
        if not job.current_checkpoint_step:
            job.current_checkpoint_step = 0
        if not job.max_checkpoint_step:
            job.max_checkpoint_step = 0
        if job.current_checkpoint_step < job.max_checkpoint_step:
            remote_checkpoint_path = f'{self.get_files_path()}/CHECKPOINT_'
            self.get_file(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}', False, ignore_log=True)
            while self.check_file_exists(
                    f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}') and job.current_checkpoint_step < job.max_checkpoint_step:
                self.remove_checkpoint_file(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}')
                job.current_checkpoint_step += 1
                self.get_file(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}', False, ignore_log=True)

    def remove_stat_file(self, job: Any) -> bool:
        """Removes STAT files from remote.

        :param job: Job to check.
        :type job: Job
        :return: True if the file was removed, False otherwise.
        :rtype: bool
        """
        # TODO: After rebasing everything I noticed that sometimes the stat file ends with '_'
        if job.stat_file.endswith('_'):
            stat_file_to_delete = f"{job.stat_file}{job.fail_count}"
        else:
            stat_file_to_delete = f"{job.stat_file[:-1]}{job.fail_count}"
        if self.delete_file(stat_file_to_delete):
            Log.debug(f"{job.stat_file[:-1]}{job.fail_count} have been removed")
            return True
        return False

    def remove_completed_file(self, job_name):
        """Removes *COMPLETED* files from remote.

        :param job_name: name of job to check
        :type job_name: str
        :return: True if successful, False otherwise
        :rtype: bool
        """
        filename = job_name + '_COMPLETED'
        if self.delete_file(filename):
            Log.debug(f'{filename} been removed')
            return True
        return False

    def remove_checkpoint_file(self, filename):
        """Removes *CHECKPOINT* files from remote.

        :param filename: file name to delete.
        :return: True if successful, False otherwise
        """
        if self.check_file_exists(filename):
            self.delete_file(filename)

    def check_file_exists(self, src, wrapper_failed=False, sleeptime=5, max_retries=3, show_logs: bool = True):
        return True

    def get_stat_file(self, job, count=-1):
        if count == -1:  # No internal retrials
            filename = f"{job.stat_file}{job.fail_count}"
        else:
            filename = f'{job.name}_STAT_{str(count)}'
        stat_local_path = os.path.join(
            self.config.get("LOCAL_ROOT_DIR"), self.expid, self.config.get("LOCAL_TMP_DIR"), filename)
        if os.path.exists(stat_local_path):
            os.remove(stat_local_path)
        if self.check_file_exists(filename):
            if self.get_file(filename, True):
                if count == -1:
                    Log.debug(f'{job.name}_STAT_{str(job.fail_count)} file have been transferred')
                else:
                    Log.debug(f'{job.name}_STAT_{str(count)} file have been transferred')
                return True
        Log.warning(f'{job.name}_STAT_{str(count)} file not found')
        return False

    @autosubmit_parameter(name='current_logdir')
    def get_files_path(self) -> str:
        """The platform's LOG directory.

        :return: platform's LOG directory
        :rtype: str
        """
        if self.type == "local":
            path = Path(self.root_dir) / self.config.get("LOCAL_TMP_DIR") / f'LOG_{self.expid}'
        else:
            path = Path(self.remote_log_dir)
        return str(path)

    def check_all_jobs(self, job_list: list['Job'], as_conf: 'AutosubmitConfig', retries: int = 5):
        """Checks jobs running status

        :param job_list: list of jobs
        :type job_list: list
        :param as_conf: config
        :type as_conf: as_conf
        :param retries: retries
        :type retries: int
        """

    def check_job(self, job: 'Job', default_status: str = Status.COMPLETED, retries: int = 5,
                  submit_hold_check: bool = False, is_wrapper: bool = False):
        """Checks job running status.

        :param is_wrapper:
        :param submit_hold_check:
        :param job:
        :param retries: retries
        :param default_status: status to assign if it can be retrieved from the platform
        :type default_status: autosubmit.job.job_common.Status
        :return: current job status
        :rtype: autosubmit.job.job_common.Status
        """
        raise NotImplementedError  # pragma: no cover

    def close_connection(self):
        return

    def write_jobid(self, jobid: str, complete_path: str) -> None:
        """Writes Job id in an out/err file.

        :param jobid: job id
        :type jobid: str
        :param complete_path: complete path to the file, includes filename
        :type complete_path: str
        """
        raise NotImplementedError  # pragma: no cover

    def add_job_to_log_recover(self, job):
        if job.id and int(job.id) != 0:
            self.recovery_queue.put(job)
            Log.debug(f"Added job {job.name} and retry number:{job.fail_count} to the log recovery queue.")
        else:
            Log.warning(
                f"Job {job.name} and retry number:{job.fail_count} has no job id. Autosubmit will no record this retry. This shouldn't happen!")
        job.updated_log += 1

    def connect(self, as_conf: 'AutosubmitConfig', reconnect: bool = False, log_recovery_process: bool = False) -> None:
        """Establishes an SSH connection to the host.

        :param as_conf: The Autosubmit configuration object.
        :param reconnect: Indicates whether to attempt reconnection if the initial connection fails.
        :param log_recovery_process: Specifies if the call is made from the log retrieval process.
        :return: None
        """
        raise NotImplementedError  # pragma: no cover

    def restore_connection(self, as_conf: 'AutosubmitConfig', log_recovery_process: bool = False) -> None:
        """Restores the SSH connection to the platform.

        :param as_conf: The Autosubmit configuration object used to establish the connection.
        :type as_conf: AutosubmitConfig
        :param log_recovery_process: Indicates that the call is made from the log retrieval process.
        :type log_recovery_process: bool
        """
        raise NotImplementedError  # pragma: no cover

    def clean_log_recovery_process(self) -> None:
        """Cleans the log recovery process variables.

        This method sets the cleanup event to signal the log recovery process to finish,
        waits for the process to join with a timeout, and then resets all related variables.
        """
        if self.ctx is None:
            self.ctx = self.get_mp_context()

        if self.cleanup_event:
            self.cleanup_event.set()

        if self.log_recovery_process:
            self.log_recovery_process.join(timeout=60)

            if self.log_recovery_process.is_alive():
                self.log_recovery_process.terminate()
                self.log_recovery_process.join(timeout=60)
                if self.log_recovery_process.is_alive():
                    Log.error("Log recovery process refused to terminate")
                    self.log_recovery_process.kill()

            if hasattr(self.log_recovery_process, "close"):
                try:
                    self.log_recovery_process.close()
                except Exception as e:
                    Log.warning(f"Failed to close process: {e}")

        if self.recovery_queue:
            try:
                self.recovery_queue.close()
                self.recovery_queue.join_thread()
            except Exception as e:
                Log.warning(f"Queue cleanup failed: {e}")

        self.log_retrieval_process_active = False

        # Reuse existing Event objects instead of destroying them.
        # Clearing the events is sufficient to give the next child process a clean initial state.
        if self.cleanup_event is not None:
            self.cleanup_event.clear()
        if self.work_event is not None:
            self.work_event.clear()

        self.cleanup_event = None
        self.recovery_queue = None
        self.log_recovery_process = None
        self.work_event = None
        self.processed_wrapper_logs = set()

    def update_as_conf(self, as_conf: 'AutosubmitConfig') -> None:
        self.config = as_conf.experiment_data

    def load_process_info(self, platform):

        platform.host = self.host
        # Retrieve more configurations settings and save them in the object
        platform.project = self.project
        platform.budget = self.budget
        platform.reservation = self.reservation
        platform.exclusivity = self.exclusivity
        platform.user = self.user
        platform.scratch = self.scratch
        platform.project_dir = self.project_dir
        platform.temp_dir = self.temp_dir
        platform._default_queue = self.queue
        platform._partition = self.partition
        platform._serial_queue = self.serial_queue
        platform._serial_partition = self.serial_partition
        platform.ec_queue = self.ec_queue
        platform.custom_directives = self.custom_directives
        platform.scratch_free_space = self.scratch_free_space
        platform.root_dir = self.root_dir
        platform.update_cmds()
        del platform.poller
        platform.config = {}
        for key in [conf_param for conf_param in self.config]:
            # Basic configuration settings
            if not isinstance(self.config[key], dict) or key in ["PLATFORMS", "EXPERIMENT", "DEFAULT", "CONFIG"]:
                platform.config[key] = self.config[key]

    def prepare_process(self) -> 'Platform':
        new_platform = self.create_a_new_copy()
        if self.ctx is None:
            self.ctx = self.get_mp_context()

        # Allocate Events only on the first call.
        # On subsequent respawn cycles the events are reused (cleared by clean_log_recovery_process)
        if self.work_event is None:
            self.work_event = self.ctx.Event()
            Platform.update_workers(self.work_event)
        if self.cleanup_event is None:
            self.cleanup_event = self.ctx.Event()
        self.load_process_info(new_platform)
        if self.recovery_queue is not None:
            self.recovery_queue.close()
            self.recovery_queue.join_thread()
            del self.recovery_queue
        # Retrieval log process variables
        self.recovery_queue = CopyQueue(ctx=self.ctx)
        # Cleanup will be automatically prompt on control + c or a normal exit
        if not getattr(self, "_atexit_registered", False):
            atexit.register(self.send_cleanup_signal)
            atexit.register(self.close_connection)
            self._atexit_registered = True
        return new_platform

    def create_new_process(self, new_platform: 'Platform', as_conf) -> None:
        try:
            self.log_recovery_process = self.ctx.Process(
                target=recover_platform_job_logs_wrapper,
                args=(new_platform, self.recovery_queue, self.work_event, self.cleanup_event, as_conf),
                name=f"{self.name}_log_recovery")
            self.log_recovery_process.daemon = True
            self.log_recovery_process.start()
        except Exception as e:
            Log.error(f"Failed to start log recovery process: {e}")
            self.log_recovery_process = None

    def get_mp_context(self):
        if not hasattr(self, 'ctx') or self.ctx is None:
            self.ctx = multiprocessing.get_context('spawn')
        return self.ctx

    def join_new_process(self):
        # Check if the process is finished; prevent zombies
        if self.log_recovery_process is not None:
            ret_pid, ret_status = os.waitpid(self.log_recovery_process.pid, os.WNOHANG)
            if ret_pid == 0:  # Process is still running
                Log.info(f"Process {self.log_recovery_process.pid} is still running.")
            else:
                Log.result(
                    f"Process {self.log_recovery_process.name} finished with pid {self.log_recovery_process.pid}")
        else:
            Log.result("Log_Recovery_Process is empty no process joinned")

    def spawn_log_retrieval_process(self, as_conf: Optional['AutosubmitConfig']) -> None:
        """Spawns a process to recover the logs of the jobs that have been completed on this platform.

        :param as_conf: Configuration object for the platform.
        :type as_conf: AutosubmitConfig
        """
        if not self.log_retrieval_process_active and (
                as_conf is None or str(as_conf.platforms_data.get(self.name, {}).get('DISABLE_RECOVERY_THREADS',
                                                                                     "false")).lower() == "false"):
            if as_conf and as_conf.misc_data.get("AS_COMMAND", "").lower() == "run":
                self.log_retrieval_process_active = True
                self.ctx = self.get_mp_context()
                new_platform = self.prepare_process()
                self.create_new_process(new_platform, as_conf)
                self.join_new_process()

    def send_cleanup_signal(self) -> None:
        """Sends a cleanup signal to the log recovery process if it is alive.
        This function is executed by the atexit module
        """
        if (self.work_event is not None and self.cleanup_event is not None and
                self.log_recovery_process is not None and self.log_recovery_process.is_alive()):
            self.work_event.clear()
            self.cleanup_event.set()
            self.log_recovery_process.join(timeout=60)

        if self.log_recovery_process.is_alive():
            Log.warning(f"Process {self.log_recovery_process.pid} didn't terminate within the timeout.")
            self.log_recovery_process.terminate()
            self.log_recovery_process.join(timeout=60)

        if self.work_event is not None:
            self.remove_workers(self.work_event)
            self.work_event = None
            self.cleanup_event = None

    def wait_for_work(self) -> bool:
        """Waits until there is work, or the keep alive timeout is reached.

        :return: True if there is work to process, False otherwise.
        """
        process_log = False
        for _ in range(self.keep_alive_timeout, 0, -1):
            if self.work_event.is_set() or not self.recovery_queue.empty() or self.cleanup_event.is_set():
                process_log = True
                break
            else:
                time.sleep(1)

        self.work_event.clear()
        return process_log

    def recover_job_log(self, jobs_db_manager: 'JobsDbManager', as_conf: 'AutosubmitConfig') -> set[Any]:
        """Recover log files for jobs and persist job state.
        :param jobs_db_manager: Optional jobs DB manager to use for persistence.
        :type jobs_db_manager: JobsDbManager
        :param as_conf: Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :return: Updated set of jobs still pending log recovery.
        """
        from autosubmit.job.job import Job

        while not self.recovery_queue.empty():
            job_data = self.recovery_queue.get(timeout=1)
            job = Job(loaded_data=jobs_db_manager.load_job_by_name(job_data["name"]))
            job.platform_name = self.name  # Change the original platform to this process platform.º
            job.platform = self
            # Fill cpus etc..
            job.update_parameters(as_conf, True, False, True)
            for key in job_data:
                setattr(job, key, job_data[key])
            job.update_local_logs()
            job.retrieve_logfiles(raise_error=True)
            jobs_db_manager.save_job_log(job)

    def recover_platform_job_logs(self, as_conf: 'AutosubmitConfig') -> None:
        """Recovers the logs of the jobs that have been submitted.
        When this is executed as a process, the exit is controlled by the work_event and cleanup_events of the main process.
        """
        setproctitle.setproctitle(f"autosubmit log {self.expid} recovery {self.name.lower()}")
        identifier = f"{self.name.lower()}(log_recovery):"
        jobs_db_manager = JobsDbManager(schema=self.expid)
        try:
            Log.info(f"{identifier} Starting...")
            self.connected = False
            self.restore_connection(as_conf, log_recovery_process=True)
            Log.result(f"{identifier} successfully connected.")
            self.keep_alive_timeout = self.config.get("LOG_RECOVERY_TIMEOUT", 60 * 5)
            while not self.cleanup_event.is_set() and self.wait_for_work():
                try:
                    self.recover_job_log(jobs_db_manager, as_conf)
                except Exception as e:
                    Log.debug(f'{identifier} Error during log recovery: {e}')
                    Log.debug(traceback.format_exc())
                    self.restore_connection(as_conf, log_recovery_process=True)
            self.recover_job_log(jobs_db_manager, as_conf)
        except Exception as e:
            Log.error(f"{identifier} {e}")
            Log.debug(traceback.format_exc())

        with suppress(Exception):
            self.close_connection()

        Log.info(f"{identifier} Exiting.")
        # Exit userspace after manually closing ssh sockets, recommended for child processes,
        # the queue() and shared signals should be in charge of the main process.
        _exit(0)

    def create_a_new_copy(self):
        raise NotImplementedError  # pragma: no cover

    def get_file_size(self, src: str) -> Union[int, None]:
        """Get file size in bytes.

        :param src: file path
        """
        raise NotImplementedError  # pragma: no cover

    def read_file(self, src: str, max_size: int = None) -> Union[bytes, None]:
        """Read file content as bytes. If max_size is set, only the first max_size bytes are read.

        :param src: file path
        :param max_size: maximum size to read
        """
        raise NotImplementedError  # pragma: no cover

    def compress_file(self, file_path: str) -> Union[str, None]:
        """Compress a file.

        :param file_path: file path
        :return: The path to the compressed file. None if compression failed.
        """
        raise NotImplementedError  # pragma: no cover

    def get_remote_log_dir(self) -> str:
        """Get the variable remote_log_dir that stores the directory of the experiment's log.

        :return: The remote_log_dir variable.
        """
        raise NotImplementedError  # pragma: no cover

    def get_completed_job_names(self, job_names: Optional[list[str]] = None) -> list[str]:
        """Get the names of the completed jobs on this platform.

        :param job_names: List of job names to check. If None, all jobs will be checked.
        :return: List of completed job names.
        """
        raise NotImplementedError  # pragma: no cover

    def get_failed_job_names(self, job_names: Optional[list[str]] = None) -> list[str]:
        """Retrieve the names of all files ending with '_COMPLETED' from the remote log directory using SSH.

        :param job_names: If provided, filters the results to include only these job names.
        :type job_names: Optional[List[str]]
        :return: List of job names with COMPLETED files.
        :rtype: List[str]
        """
        raise NotImplementedError  # pragma: no cover

    def delete_failed_and_completed_names(self, job_names: list[str]) -> None:
        """Deletes the COMPLETED and FAILED files for the given job names from the remote log directory.

        :param job_names: List of job names whose COMPLETED and FAILED files should be deleted
        :type job_names: List[str]
        """
        raise NotImplementedError  # pragma: no cover
