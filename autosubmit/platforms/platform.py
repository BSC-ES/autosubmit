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
import queue  # only for the exception
import time
import traceback
from contextlib import suppress
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
# noinspection PyProtectedMember
from os import _exit  # type: ignore
from pathlib import Path
from typing import Any, Optional, Union, TYPE_CHECKING

import setproctitle

from autosubmit.helpers.parameters import autosubmit_parameter
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job_packages import JobPackageBase
    from autosubmit.job.job import Job
    from autosubmit.job.job_list import JobList
    from autosubmit.job.job_package_persistence import JobPackagePersistence
    from multiprocessing.process import BaseProcess


def _init_logs_log_process(as_conf: 'AutosubmitConfig', platform_name: str) -> None:
    Log.set_console_level(as_conf.experiment_data.get("LOG_RECOVERY_CONSOLE_LEVEL", "DEBUG"))
    if as_conf.experiment_data["ROOTDIR"]:
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
    as_conf.experiment_data = {
        "AS_ENV_PLATFORMS_PATH": as_conf.experiment_data.get("AS_ENV_PLATFORMS_PATH", None),
        "AS_ENV_SSH_CONFIG_PATH": as_conf.experiment_data.get("AS_ENV_SSH_CONFIG_PATH", None),
        "AS_ENV_CURRENT_USER": as_conf.experiment_data.get("AS_ENV_CURRENT_USER", None),
        "ROOTDIR": as_conf.experiment_data.get("ROOTDIR", None),
        "LOG_RECOVERY_CONSOLE_LEVEL": as_conf.experiment_data.get("CONFIG", {}).get("LOG_RECOVERY_CONSOLE_LEVEL",
                                                                                    "DEBUG"),
        "LOG_RECOVERY_FILE_LEVEL": as_conf.experiment_data.get("CONFIG", {}).get("LOG_RECOVERY_FILE_LEVEL",
                                                                                 "EVERYTHING"),
    }
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
        super().put(job.__getstate__(), block, timeout)


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
        self.two_factor_auth = None
        self.otp_timeout = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("2FA_TIMEOUT", 60 * 5)
        self.two_factor_auth = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("2FA", False)
        self.two_factor_method = self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("2FA_METHOD", "token")
        if not self.two_factor_auth:
            self.pw = None
        elif auth_password is not None and self.two_factor_auth:
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
        self.processed_wrapper_logs: set[str] = set()
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

    @classmethod
    def update_workers(cls, event_worker):
        # This is visible on all instances simultaneosly. Is to send the keep alive signal.
        cls.worker_events.append(event_worker)

    @classmethod
    def remove_workers(cls, event_worker: Event) -> None:
        """Remove the given even worker from the list of workers in this class. """
        if event_worker in cls.worker_events:
            cls.worker_events.remove(event_worker)

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

    def process_batch_ready_jobs(self, valid_packages_to_submit, failed_packages, error_message="", hold=False):
        return True, valid_packages_to_submit

    def submit_ready_jobs(self, as_conf: 'AutosubmitConfig', job_list: 'JobList',
                          packages_persistence: 'JobPackagePersistence', packages_to_submit: list['JobPackageBase'],
                          inspect=False, only_wrappers=False, hold=False):
        """Gets READY jobs and send them to the platforms if there is available space on the queues.

        :param hold:
        :param packages_to_submit:
        :param as_conf: autosubmit config object
        :type as_conf: AutosubmitConfig object
        :param job_list: job list to check
        :type job_list: JobList object
        :param packages_persistence: Handles database per experiment.
        :type packages_persistence: JobPackagePersistence object
        :param inspect: True if coming from generate_scripts_andor_wrappers().
        :type inspect: Boolean
        :param only_wrappers: True if it comes from create -cw, False if it comes from inspect -cw.
        :type only_wrappers: Boolean
        :return: True if at least one job was submitted, False otherwise \n
        :rtype: Boolean
        """
        any_job_submitted = False
        save = False
        failed_packages = list()
        error_message = ""
        if not inspect:
            job_list.save()
        if not hold:
            Log.debug(f"\nJobs ready for {self.name}: {len(job_list.get_ready(self, hold=hold))}")
        else:
            Log.debug(f"\nJobs prepared for {self.name}: {len(job_list.get_prepared(self))}")
        if not inspect:
            self.generate_submit_script()
        valid_packages_to_submit: list['JobPackageBase'] = []
        for package in packages_to_submit:
            try:
                # If called from inspect command or -cw
                if only_wrappers or inspect:
                    if hasattr(package, "name"):
                        job_list.packages_dict[package.name] = package.jobs
                        from ..job.job import WrapperJob
                        wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.READY, 0,
                                                 package.jobs, package._wallclock, package.platform, as_conf, hold)
                        job_list.job_package_map[package.jobs[0].id] = wrapper_job
                        packages_persistence.save(package, inspect)
                    for innerJob in package._jobs:
                        any_job_submitted = True
                        # Setting status to COMPLETED, so it does not get stuck in the loop that calls this function
                        innerJob.status = Status.COMPLETED
                        innerJob.updated_log = False

                # If called from RUN or inspect command
                if not only_wrappers:
                    try:
                        package.submit(as_conf, job_list.parameters, inspect, hold=hold)
                        save = True
                        if not inspect:
                            job_list.save()
                        if package.x11 != "true":
                            valid_packages_to_submit.append(package)
                    except (IOError, OSError) as e:
                        if package.jobs[0].id != 0:
                            failed_packages.append(package.jobs[0].id)
                        Log.warning(f'An unexpected error happened while submitting the package: {str(e)}')
                        continue
                    except AutosubmitError as e:
                        if package.jobs[0].id != 0:
                            failed_packages.append(package.jobs[0].id)
                        self.connected = False
                        if e.message.lower().find("bad parameters") != -1 or e.message.lower().find(
                                "scheduler is not installed") != -1:
                            error_msg = ""
                            for package_tmp in valid_packages_to_submit:
                                for job_tmp in package_tmp.jobs:
                                    if job_tmp.section not in error_msg:
                                        error_msg += job_tmp.section + "&"
                            for job_tmp in package.jobs:
                                if job_tmp.section not in error_msg:
                                    error_msg += job_tmp.section + "&"
                            if e.message.lower().find("bad parameters") != -1:
                                error_message += f"\ncheck job and queue specified in your JOBS definition in YAML. Sections that could be affected: {error_msg[:-1]}"
                            else:
                                error_message += f"\ncheck that {self.name} platform has set the correct scheduler. Sections that could be affected: {error_msg[:-1]}"
                    except AutosubmitCritical:
                        raise
                    except Exception:
                        self.connected = False
                        raise
            except Exception:
                raise
        if valid_packages_to_submit:
            any_job_submitted = True
        return save, failed_packages, error_message, valid_packages_to_submit, any_job_submitted

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

    def check_file_exists(self, src: str, wrapper_failed: bool = False, sleeptime: int = 5, max_retries: int = 3):
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
    def get_files_path(self):
        """The platform's LOG directory.

        :return: platform's LOG directory
        :rtype: str
        """
        if self.type == "local":
            path = Path(self.root_dir) / self.config.get("LOCAL_TMP_DIR") / f'LOG_{self.expid}'
        else:
            path = Path(self.remote_log_dir)
        return str(path)

    def submit_job(self, job: 'Job', script_name: str, hold: bool = False, export: str = "none"):
        """Submit a job from a given job object.

        :param job: job object
        :type job: autosubmit.job.job.Job
        :param script_name: job script's name
        :rtype script_name: str
        :param hold: if True, the job will be submitted in hold state
        :type hold: bool
        :param export: export environment variables
        :type export: str
        :return: job id for the submitted job
        :rtype: int
        """
        raise NotImplementedError  # pragma: no cover

    def check_all_jobs(self, job_list: list['Job'], as_conf:'AutosubmitConfig', retries: int = 5):
        for job, job_prev_status in job_list:
            self.check_job(job)

    def check_job(self, job: 'Job', default_status: str = Status.COMPLETED, retries:int = 5,
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
        return NotImplementedError

    def generate_submit_script(self) -> None:
        """Opens Submit script file. """
        raise NotImplementedError  # pragma: no cover

    def submit_script(self, hold: bool = False) -> Union[list[str], str]:
        """Sends a Submit file Script, execute it  in the platform and retrieves the Jobs_ID of all jobs at once. """
        raise NotImplementedError  # pragma: no cover

    def add_job_to_log_recover(self, job):
        if job.id and int(job.id) != 0:
            self.recovery_queue.put(job)
        else:
            Log.warning(
                f"Job {job.name} and retry number:{job.fail_count} has no job id. Autosubmit will no record this retry.")
            job.updated_log = True

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
        if self.cleanup_event is not None:
            self.cleanup_event.set()  # Indicates to old child ( if reachable ) to finish.
        if self.log_recovery_process is not None:
            # Waits for old child ( if reachable ) to finish. Timeout in case of it being blocked.
            self.log_recovery_process.join(timeout=60)
        # Resets everything related to the log recovery process.
        self.recovery_queue = self.ctx.Queue()
        self.log_retrieval_process_active = False
        self.remove_workers(self.work_event)
        self.work_event = self.ctx.Event()
        self.cleanup_event = self.ctx.Event()
        self.log_recovery_process = self.ctx.Process()
        self.processed_wrapper_logs = set()

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
        self.work_event = self.ctx.Event()
        self.cleanup_event = self.ctx.Event()
        Platform.update_workers(self.work_event)
        self.load_process_info(new_platform)
        if self.recovery_queue:
            del self.recovery_queue
        # Retrieval log process variables
        self.recovery_queue = CopyQueue(ctx=self.ctx)
        # Cleanup will be automatically prompt on control + c or a normal exit
        atexit.register(self.send_cleanup_signal)
        atexit.register(self.close_connection)
        return new_platform

    def create_new_process(self, new_platform: 'Platform', as_conf) -> None:
        self.log_recovery_process = self.ctx.Process(
            target=recover_platform_job_logs_wrapper,
            args=(new_platform, self.recovery_queue, self.work_event, self.cleanup_event, as_conf),
            name=f"{self.name}_log_recovery")
        self.log_recovery_process.daemon = True
        self.log_recovery_process.start()

    @staticmethod
    def get_mp_context():
        return multiprocessing.get_context('spawn')

    def join_new_process(self):
        # Prevents zombies
        os.waitpid(self.log_recovery_process.pid, os.WNOHANG)
        Log.result(f"Process {self.log_recovery_process.name} started with pid {self.log_recovery_process.pid}")

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
        if self.log_recovery_process.is_alive():
            self.work_event.clear()
            self.cleanup_event.set()
            self.log_recovery_process.join(timeout=60)

    def wait_mandatory_time(self, sleep_time: int = 60) -> bool:
        """Waits for the work_event to be set or the cleanup_event to be set for a mandatory time.

        :param sleep_time: Minimum time to wait in seconds. Defaults to 60.
        :type sleep_time: int
        :return: True if there is work to process, False otherwise.
        :rtype: bool
        """
        process_log = False
        for remaining in range(sleep_time, 0, -1):
            time.sleep(1)
            if self.work_event.is_set() or not self.recovery_queue.empty():
                process_log = True
            if self.cleanup_event.is_set():
                process_log = True
                break
        return process_log

    def wait_for_work(self, sleep_time: int = 60) -> bool:
        """
        Waits a mandatory time and then waits until there is work, no work to more process or the cleanup event is set.

        :param sleep_time: Maximum time to wait in seconds. Defaults to 60.
        :type sleep_time: int
        :return: True if there is work to process, False otherwise.
        :rtype: bool
        """
        process_log = self.wait_mandatory_time(sleep_time)
        if not process_log:
            process_log = self.wait_until_timeout(self.keep_alive_timeout - sleep_time)
        self.work_event.clear()
        return process_log

    def wait_until_timeout(self, timeout: int = 60) -> bool:
        """Waits until the timeout is reached or any signal is set to process logs.

        :param timeout: Maximum time to wait in seconds. Defaults to 60.
        :return: True if there is work to process, False otherwise.
        """
        process_log = False
        for _ in range(timeout, 0, -1):
            time.sleep(1)
            if self.work_event.is_set() or not self.recovery_queue.empty() or self.cleanup_event.is_set():
                process_log = True
                break
        return process_log

    def recover_job_log(self, identifier: str, jobs_pending_to_process: set[Any],
                        as_conf: 'AutosubmitConfig') -> set[Any]:
        """Recovers log files for jobs from the recovery queue and retries failed jobs.

        :param identifier: Identifier for logging purposes.
        :param jobs_pending_to_process: Set of jobs that had issues during log retrieval.
        :param as_conf: The Autosubmit configuration object containing experiment data.
        :return: Updated set of jobs pending to process.
        """
        while not self.recovery_queue.empty():
            try:
                from autosubmit.job.job import Job
                job = Job(loaded_data=self.recovery_queue.get(timeout=1))
                job.platform_name = self.name  # Change the original platform to this process platform.
                job.platform = self
                job._log_recovery_retries = 0  # Reset the log recovery retries.
                try:
                    job.retrieve_logfiles(raise_error=True)
                except Exception:
                    jobs_pending_to_process.add(job)
                    job._log_recovery_retries += 1
                    Log.warning(
                        f"{identifier} (Retry) Failed to recover log for job '{job.name}' and retry:'{job.fail_count}'.")
            except queue.Empty:
                pass

        if len(jobs_pending_to_process) > 0:  # Restore the connection if there was an issue with one or more jobs.
            self.restore_connection(as_conf, log_recovery_process=True)

        # This second while is to keep retring the failed jobs.
        # With the unique queue, the main process won't send the job again, so we have to store it here.
        while len(jobs_pending_to_process) > 0:  # jobs that had any issue during the log retrieval
            job = jobs_pending_to_process.pop()
            job._log_recovery_retries += 1
            try:
                job.retrieve_logfiles(raise_error=True)
                job._log_recovery_retries += 1
            except Exception as e:
                if job._log_recovery_retries < 5:
                    jobs_pending_to_process.add(job)
                Log.warning(
                    f"{identifier} (Retry) Failed to recover log for job '{job.name}' "
                    f"and retry '{job.fail_count}': {str(e)}")
            Log.result(
                f"{identifier} (Retry) Successfully recovered log for job '{job.name}' and retry '{job.fail_count}'.")
        if len(jobs_pending_to_process) > 0:
            self.restore_connection(as_conf,
                                    log_recovery_process=True)  # Restore the connection if there was an issue with one or more jobs.

        return jobs_pending_to_process

    def recover_platform_job_logs(self, as_conf: 'AutosubmitConfig') -> None:
        """Recovers the logs of the jobs that have been submitted.
        When this is executed as a process, the exit is controlled by the work_event and cleanup_events of the main process.
        """
        setproctitle.setproctitle(f"autosubmit log {self.expid} recovery {self.name.lower()}")
        identifier = f"{self.name.lower()}(log_recovery):"
        try:
            Log.info(f"{identifier} Starting...")
            jobs_pending_to_process: set = set()
            self.connected = False
            self.restore_connection(as_conf, log_recovery_process=True)
            Log.result(f"{identifier} successfully connected.")
            log_recovery_timeout = self.config.get("LOG_RECOVERY_TIMEOUT", 60)
            # Keep alive signal timeout is 5 minutes, but the sleeptime is 60 seconds.
            self.keep_alive_timeout = max(log_recovery_timeout * 5, 60 * 5)
            while self.wait_for_work(sleep_time=max(log_recovery_timeout, 60)):
                jobs_pending_to_process = self.recover_job_log(identifier, jobs_pending_to_process, as_conf)
                if self.cleanup_event.is_set():  # Check if the main process is waiting for this child to end.
                    self.recover_job_log(identifier, jobs_pending_to_process, as_conf)
                    break
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
