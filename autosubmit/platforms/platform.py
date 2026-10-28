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
import queue
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

from autosubmit.config.runtime import LogRecoveryConfig, resolve_log_recovery_queue_size
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
        recovery_queue: 'CopyQueue',
        worker_event: Event,
        cleanup_event: Event,
        as_conf: 'AutosubmitConfig',
        recovery_results: Optional[Any]
) -> None:
    """Wrapper function to recover platform job logs.

    :param platform: The platform object responsible for managing the connection and job recovery.
    :param recovery_queue: A multiprocessing queue used to store jobs for recovery.
    :param worker_event: An event to signal work availability.
    :param cleanup_event: An event to signal cleanup operations.
    :param as_conf: The Autosubmit configuration object containing experiment data.
    :type as_conf: AutosubmitConfig
    :param recovery_results: Optional queue used by the child to acknowledge processed jobs.
    :return: None
    :rtype: None
    """
    platform.recovery_queue = recovery_queue
    platform.recovery_results = recovery_results
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
        # CPMIP notification needs EXPERIMENT (for CALENDAR) and MAIL (for NOTIFICATIONS, TO)
        "EXPERIMENT": as_conf.experiment_data.get("EXPERIMENT", {}),
        "MAIL": as_conf.experiment_data.get("MAIL", {}),
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

    def __init__(self, maxsize: int = -1, ctx: Any = None) -> None:
        """Initializes the Queue.

        :param maxsize: Maximum size of the queue. Defaults to -1 (infinite size).
        :type maxsize: int
        :param ctx: Context for the queue. Defaults to None.
        :type ctx: Context
        """
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

    def put_state(self, state: Any, block: bool = True, timeout: Optional[float] = None) -> None:
        """Put an already serialized job state into the queue.

        Used to re-queue the state snapshots kept in ``_pending_recovery`` without
        calling ``__getstate__`` again (the snapshot is taken before the parent
        increments ``updated_log``).

        :param state: Job state produced by ``Job.__getstate__``.
        :param block: Whether to block when the queue is full. Defaults to True.
        :param timeout: Timeout for blocking operations. Defaults to None.
        """
        super().put(state, block, timeout)


class Platform:
    """
    Class to manage the connections to the different platforms.
    """
    # This is a list of the keep_alive events, used to send the signal outside the main loop of Autosubmit
    worker_events: list[Event] = []
    # Shared lock between the main process and a retrieval log process
    lock = multiprocessing.Lock()
    IO_SAFE_WAIT = 60

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
        self.recovery_queue: Optional[CopyQueue] = None
        self.work_event: Optional[Event] = None
        self.cleanup_event: Optional[Event] = None
        self.log_retrieval_process_active: bool = False
        self.log_recovery_process: Optional['BaseProcess'] = None
        # Log recovery resiliency state (see issue #3342). Pending jobs are kept in
        # memory as pre-increment state snapshots so they can be re-queued if the
        # recovery process dies. The recovery process acknowledges every processed
        # job through ``recovery_results`` so the snapshots can be pruned.
        self._pending_recovery: dict[str, dict] = {}
        self.recovery_results: Optional[Any] = None
        self._recovery_consecutive_failures: int = 0
        self._log_recovery_in_backoff: bool = False
        self._log_recovery_retry_at: float = 0.0
        self._log_recovery_started_at: float = 0.0
        self.keep_alive_timeout = 60 * 5  # Useful in case of kill -9
        self.compress_remote_logs = False
        self.remote_logs_compress_type = "gzip"
        self.compression_level = 9
        log_queue_size = 200
        if self.config:
            platform_config: dict = self.config.get("PLATFORMS", {}).get(self.name.upper(), {})
            log_queue_size = resolve_log_recovery_queue_size(self.config, self.name)
            self.compress_remote_logs = platform_config.get("COMPRESS_REMOTE_LOGS", False)
            self.remote_logs_compress_type = platform_config.get("REMOTE_LOGS_COMPRESS_TYPE", "gzip")
            self.compression_level = platform_config.get("COMPRESSION_LEVEL", 9)

        self.log_queue_size = log_queue_size
        self.remote_log_dir = None
        self.has_scheduler = True
        self.IO_SAFE_WAIT = int(
            self.config.get("PLATFORMS", {}).get(self.name.upper(), {}).get("IO_SAFE_WAIT", self.IO_SAFE_WAIT))

    @classmethod
    def update_workers(cls, event_worker):
        # This is visible on all instances simultaneously. Is to send the keep alive signal.
        if event_worker is None:
            return
        with cls.lock:
            try:
                if event_worker not in cls.worker_events:
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

    def prepare_submission(self, as_conf: 'AutosubmitConfig', job_list: 'JobList',
                           packages_to_submit: list['JobPackageBase'],
                           inspect=False, only_wrappers=False) -> tuple[
        dict[str, dict[str, 'JobPackageBase']], dict[str, dict[str, 'JobPackageBase']]]:
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
            self.prepare_dry_run_if_applicable(package, only_wrappers, inspect)
            if not only_wrappers:
                package.generate_scripts(as_conf, inspect)
            if not inspect and not only_wrappers:
                package.send_files()
            if package.x11:
                x11_scripts_to_submit_by_section.setdefault(package.sections, {})[
                    f"{package.name}.cmd"] = package
            else:
                scripts_to_submit_by_section.setdefault(package.sections, {})[f"{package.name}.cmd"] = package

        return scripts_to_submit_by_section, x11_scripts_to_submit_by_section

    @staticmethod
    def prepare_dry_run_if_applicable(package: 'JobPackageBase', only_wrappers: bool,
                                      inspect: bool,
                                      ) -> None:
        """Dry-run preparation of a package to emulate that the package was submitted, without following the normal submission flow.

        :param package: Package being prepared for inspect or wrapper-only mode.
        :type package: JobPackageBase
        :param only_wrappers: If ``True``, prepare wrapper metadata without
            following the normal submission flow.
        :type only_wrappers: bool
        :param inspect: If ``True``, prepare package metadata for inspect mode.
        :type inspect: bool
        :raises Exception: Propagate any exception raised while creating wrapper
            job metadata or saving the package.
        """

        if only_wrappers or inspect:
            for innerJob in package._jobs:
                # Setting status to COMPLETED, so it does not get stuck in the loop that calls this function
                innerJob.id = 1
                innerJob.status = Status.COMPLETED
                innerJob.updated_log = innerJob.retrials

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

    def should_run_log_recovery(self, as_conf: Optional['AutosubmitConfig']) -> bool:
        """Whether a separate log recovery process is expected for this platform.

        :param as_conf: Experiment configuration.
        :return: ``True`` when recovery threads are enabled and the command is ``run``.
        """
        if as_conf is None or as_conf.is_recovery_threads_disabled(self.name):
            return False
        return as_conf.misc_data.get("AS_COMMAND", "").lower() == "run"

    @property
    def log_recovery_process_alive(self) -> bool:
        """Whether the log recovery process exists and is running."""
        return self.log_recovery_process is not None and self.log_recovery_process.is_alive()

    @property
    def has_pending_log_recovery(self) -> bool:
        """Whether there are jobs waiting for their recovery acknowledgement."""
        return bool(self._pending_recovery)

    @staticmethod
    def _log_recovery_config(as_conf: Optional['AutosubmitConfig']) -> LogRecoveryConfig:
        """Return the parsed log recovery configuration, using defaults when unavailable.

        :param as_conf: Experiment configuration.
        :return: The parsed ``RUNTIME.LOG_RECOVERY`` configuration.
        """
        return as_conf.runtime.log_recovery if as_conf is not None else LogRecoveryConfig()

    def _drain_recovery_results(self) -> None:
        """Remove from the pending map the jobs the recovery process already handled.

        The child acknowledges every job it processes (whether the download succeeded or
        not) through ``recovery_results``. Jobs that were never acknowledged, for example
        because the process died while handling them, stay pending and are re-queued on
        the next successful respawn.
        """
        if self.recovery_results is None or not self._pending_recovery:
            return
        while not self.recovery_results.empty():
            try:
                name = self.recovery_results.get()
            except (EOFError, OSError):
                break
            self._pending_recovery.pop(name, None)

    def _job_from_state(self, state: dict) -> 'Job':
        """Rebuild a job from a ``Job.__getstate__`` snapshot.

        :param state: Serialized job state kept in ``_pending_recovery``.
        :return: A job bound to this platform, ready for log recovery.
        """
        from autosubmit.job.job import Job
        job = Job(loaded_data=state)
        job.platform_name = self.name
        job.platform = self
        return job

    def _start_retry_backoff(self, as_conf: 'AutosubmitConfig') -> None:
        """Wait ``RETRY_DELAY`` before trying to start the recovery process again.

        :param as_conf: Experiment configuration.
        """
        log_recovery = Platform._log_recovery_config(as_conf)
        attempts = self._recovery_consecutive_failures
        self._log_recovery_in_backoff = True
        self._recovery_consecutive_failures = 0
        Log.error(
            f"[{self.name}] Log recovery process failed to start after {attempts} consecutive "
            f"attempt(s). Retrying in {log_recovery.retry_delay} seconds.")
        if log_recovery.inline_fallback:
            Log.warning(f"[{self.name}] Recovering pending logs inline while waiting for the retry.")
            self._recover_pending_inline(as_conf)
        # Start the retry window after the inline recovery, which may take a while.
        self._log_recovery_retry_at = time.time() + log_recovery.retry_delay

    def try_respawn_log_recovery_process(self, as_conf: 'AutosubmitConfig') -> bool:
        """Ensure a log recovery process is running when one is expected.

        Safe to call periodically: it only restarts a dead process and respects the retry
        backoff configured under ``RUNTIME.LOG_RECOVERY``. A process that starts and dies
        shortly after counts as a failed attempt, so a crash loop eventually backs off.

        :param as_conf: Experiment configuration.
        :return: ``True`` when a live recovery process is running.
        """
        self._drain_recovery_results()
        if not self.should_run_log_recovery(as_conf):
            return False

        log_recovery = Platform._log_recovery_config(as_conf)
        now = time.time()
        if self.log_recovery_process_alive:
            if (self._log_recovery_started_at
                    and now - self._log_recovery_started_at >= log_recovery.min_alive_seconds):
                self._recovery_consecutive_failures = 0
            return True

        # A process that was started and is now gone counts as a failed attempt only when it
        # died shortly after starting. A process that lived long enough is considered healthy.
        if self._log_recovery_started_at:
            if now - self._log_recovery_started_at >= log_recovery.min_alive_seconds:
                self._recovery_consecutive_failures = 0
            else:
                self._recovery_consecutive_failures += 1
            self._log_recovery_started_at = 0.0

        if self._log_recovery_in_backoff:
            if now < self._log_recovery_retry_at:
                return False
            self._log_recovery_in_backoff = False
            self._recovery_consecutive_failures = 0

        max_restarts = log_recovery.consecutive_max_restarts
        if 0 < max_restarts <= self._recovery_consecutive_failures:
            self._start_retry_backoff(as_conf)
            return False

        Log.warning(f"[{self.name}] Log recovery process is not alive. Attempting to restart it.")
        self.clean_log_recovery_process(keep_pending_jobs=True)
        if not self.spawn_log_retrieval_process(as_conf):
            self._recovery_consecutive_failures += 1
            if 0 < max_restarts <= self._recovery_consecutive_failures:
                self._start_retry_backoff(as_conf)
            return False

        self._requeue_pending()
        return True

    def _requeue_pending(self) -> None:
        """Re-queue the jobs that were not acknowledged when the process died."""
        if self.recovery_queue is None or not self._pending_recovery:
            return
        for state in list(self._pending_recovery.values()):
            try:
                self.recovery_queue.put_state(state)
            except Exception as e:
                Log.warning(f"[{self.name}] Failed to re-queue a pending log recovery: {e}")
        if self.work_event is not None:
            self.work_event.set()

    def _recover_pending_inline(self, as_conf: 'AutosubmitConfig') -> None:
        """Recover the accumulated pending logs in the main process.

        Entries are removed only when the inline recovery succeeds, so a failed attempt
        can be retried later (inline or by the recovery process).

        :param as_conf: Experiment configuration.
        """
        if not self._pending_recovery:
            return
        for name, state in list(self._pending_recovery.items()):
            try:
                job = self._job_from_state(state)
                if self.recover_log_inline(job, as_conf):
                    self._pending_recovery.pop(name, None)
            except Exception as e:
                Log.warning(f"[{self.name}] Inline log recovery failed for job {name}: {e}")
                Log.debug(traceback.format_exc())

    def _recover_inline(self, job: 'Job', as_conf: 'AutosubmitConfig') -> bool:
        """Recover the given job and any accumulated pending jobs inline.

        :param job: Job whose logs have to be recovered.
        :param as_conf: Experiment configuration.
        :return: ``True`` when the job's logs were recovered, ``False`` otherwise.
        """
        if not Platform._log_recovery_config(as_conf).inline_fallback:
            return False
        recovered = self.recover_log_inline(job, as_conf)
        if recovered:
            self._pending_recovery.pop(job.name, None)
        if self._pending_recovery:
            self._recover_pending_inline(as_conf)
        return recovered

    def add_job_to_log_recover(self, job, as_conf: 'AutosubmitConfig') -> bool:
        """Queue a job so its logs and stats can be recovered by the recovery process.

        When the recovery process is dead it is restarted (re-queueing the jobs that were
        still pending) instead of silently dropping the job. While the process is unavailable,
        the logs are recovered inline when ``RUNTIME.LOG_RECOVERY.INLINE_FALLBACK`` is enabled.

        The state snapshot is taken *before* the caller increments ``updated_log`` so a
        re-queued job still recovers the pending attempts.

        :param job: Job whose logs have to be recovered.
        :param as_conf: Experiment configuration.
        :return: ``True`` when the job was queued (or recovered inline), ``False`` otherwise.
        """
        job_id = 0
        if job.id:
            try:
                job_id = int(job.id)
            except (TypeError, ValueError):
                job_id = 0
        if not job_id:
            Log.warning(
                f"Job {job.name} and retry number:{job.fail_count} has no valid job id "
                f"({job.id!r}). Autosubmit will not record this retry. This shouldn't happen!")
            job.updated_log = job.fail_count + 1
            return False

        if not self.should_run_log_recovery(as_conf):
            Log.debug(f"[{self.name}] Log recovery is disabled; skipping {job.name}.")
            return False

        if not self.try_respawn_log_recovery_process(as_conf) or self.recovery_queue is None:
            return self._recover_inline(job, as_conf)

        try:
            state = job.__getstate__()
            self.recovery_queue.put_state(state, timeout=30)
            self._pending_recovery[job.name] = state
            if self.work_event is not None:
                self.work_event.set()
            Log.debug(
                f"Added job {job.name} and retry number:{job.fail_count} to the log recovery queue.")
            return True
        except Exception as e:
            Log.warning(f"Failed to add job {job.name} to log recovery queue: {e}")
            return self._recover_inline(job, as_conf)

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

    def clean_log_recovery_process(self, keep_pending_jobs: bool = False) -> None:
        """Cleans the log recovery process variables.

        This method sets the cleanup event to signal the log recovery process to finish,
        waits for the process to join with a timeout, and then resets all related variables.

        :param keep_pending_jobs: When ``True`` the jobs pending recovery are preserved so a new
            process can take them over (used while respawning). When ``False`` (default) all
            the log recovery state, including pending jobs, is reset.
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
                self.recovery_queue.cancel_join_thread()
                self.recovery_queue.close()
                self.recovery_queue.join_thread()
            except Exception as e:
                Log.warning(f"Queue cleanup failed: {e}")

        if self.recovery_results:
            try:
                self.recovery_results.close()
            except Exception as e:
                Log.warning(f"Results queue cleanup failed: {e}")

        self.log_retrieval_process_active = False
        self._log_recovery_started_at = 0.0

        # Reuse existing Event objects instead of destroying them.
        # Clearing the events is sufficient to give the next child process a clean initial state.
        if self.cleanup_event is not None:
            self.cleanup_event.clear()
        if self.work_event is not None:
            self.work_event.clear()
            # The respawn creates a new event, do not leave stale ones in the shared list.
            Platform.remove_workers(self.work_event)

        self.cleanup_event = None
        self.recovery_queue = None
        self.recovery_results = None
        self.log_recovery_process = None
        self.work_event = None

        if not keep_pending_jobs:
            self._pending_recovery = {}
            self._recovery_consecutive_failures = 0
            self._log_recovery_in_backoff = False
            self._log_recovery_retry_at = 0.0

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
            self.recovery_queue.cancel_join_thread()
            self.recovery_queue.close()
            self.recovery_queue.join_thread()
            del self.recovery_queue
        if self.recovery_results is not None:
            try:
                self.recovery_results.close()
            except Exception as e:
                Log.debug(f"Results queue cleanup failed: {e}")
        # Retrieval log process variables
        self.recovery_queue = CopyQueue(maxsize=self.log_queue_size, ctx=self.ctx)
        self.recovery_results = self.ctx.SimpleQueue()
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
                args=(new_platform, self.recovery_queue, self.work_event, self.cleanup_event, as_conf,
                      self.recovery_results),
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

    def join_new_process(self) -> None:
        """Check whether the recovery process is running and reap it when it is not.

        ``Process.is_alive()`` calls ``poll()``, which reaps the child if it already
        exited. Reaping through ``multiprocessing`` keeps ``is_alive()``/``exitcode``
        accurate; reaping with a raw ``os.waitpid`` leaves the process object believing
        the child is still alive forever (``ECHILD`` -> ``poll()`` returns ``None``).
        """
        process = self.log_recovery_process
        if process is None:
            Log.result("Log_Recovery_Process is empty no process joinned")
            return
        if process.is_alive():
            Log.info(f"Process {process.pid} is still running.")
        else:
            Log.result(
                f"Process {process.name} finished with pid {process.pid} "
                f"(exit code {process.exitcode})")

    def spawn_log_retrieval_process(self, as_conf: Optional['AutosubmitConfig']) -> bool:
        """Spawns a process to recover the logs of the jobs that have been completed on this platform.

        :param as_conf: Configuration object for the platform.
        :type as_conf: AutosubmitConfig
        :return: ``True`` when a live recovery process is running.
        """
        if self.log_retrieval_process_active:
            alive = self.log_recovery_process_alive
            if not alive:
                self.log_retrieval_process_active = False
            return alive

        if not self.should_run_log_recovery(as_conf):
            return False

        self.log_retrieval_process_active = True
        self.ctx = self.get_mp_context()
        new_platform = self.prepare_process()
        self.create_new_process(new_platform, as_conf)
        self.join_new_process()

        alive = self.log_recovery_process_alive
        if not alive:
            self.log_retrieval_process_active = False
            Log.error(f"Could not start the log recovery process for platform [{self.name}].")
        else:
            self._log_recovery_started_at = time.time()
        return alive

    def send_cleanup_signal(self) -> None:
        """Sends a cleanup signal to the log recovery process if it is alive.
        This function is executed by the atexit module
        """
        process = self.log_recovery_process
        if (self.work_event is not None and self.cleanup_event is not None
                and process is not None and process.is_alive()):
            self.work_event.clear()
            self.cleanup_event.set()
            process.join(timeout=60)

        if process is not None and process.is_alive():
            Log.warning(f"Process {process.pid} didn't terminate within the timeout.")
            process.terminate()
            process.join(timeout=60)

        if self.work_event is not None:
            self.remove_workers(self.work_event)
            self.work_event = None
            self.cleanup_event = None

    def wait_for_work(self) -> bool:
        """Waits until there is work, or the keep alive timeout is reached.

        :return: True if there is work to process, False otherwise.
        """
        process_log = False
        start_time = time.time()
        while (time.time() - start_time) < self.keep_alive_timeout:
            if self.work_event.is_set() or not self.recovery_queue.empty() or self.cleanup_event.is_set():
                process_log = True
                break
            time.sleep(1)
        else:
            # Final check in case something arrived just after the last sleep
            if self.work_event.is_set() or not self.recovery_queue.empty() or self.cleanup_event.is_set():
                process_log = True

        self.work_event.clear()
        return process_log

    def _recover_job(self, job: 'Job', as_conf: 'AutosubmitConfig') -> Any:
        """Recover the logs and stats of ``job``.

        Shared by the recovery child process and the inline recovery path.

        :param job: Job to recover.
        :param as_conf: Experiment configuration.
        :return: The recovery report produced by the job.
        """
        report = job.retrieve_logfiles()
        job.send_cpmip_notification(as_conf)

        if not report.all_succeeded:
            failed = [
                attempt
                for attempt in report.attempts
                if attempt.error and "Remote logs not found" not in attempt.error
            ]
            if failed:
                Log.warning(
                    f"{self.name}(log_recovery): Job {job.name} had "
                    f"{len(failed)} failed recovery attempt(s): "
                    f"{[attempt.error for attempt in failed]}"
                )

        recovered_attempts = sum(1 for attempt in report.attempts if attempt.success)
        if recovered_attempts > 0:
            Log.result(
                f"{self.name}(log_recovery): Job {job.name} recovered "
                f"{recovered_attempts} attempt(s)."
            )
        return report

    def recover_log_inline(self, job: 'Job', as_conf: 'AutosubmitConfig') -> bool:
        """Recover the given in-memory job's logs in the main process.

        Used when the recovery process is unavailable or the recovery threads are
        disabled for this platform.

        :param job: Job to recover.
        :param as_conf: Experiment configuration.
        :return: ``True`` when all the pending attempts were recovered.
        """
        try:
            report = self._recover_job(job, as_conf)
        except Exception as e:
            Log.warning(f"[{self.name}] Inline log recovery failed: {e}")
            Log.debug(traceback.format_exc())
            return False
        return bool(report.all_succeeded)

    def recover_job_log(self) -> None:
        """Recover the log files of the jobs waiting in the recovery queue.

        A failing job is logged and the connection is restored before continuing with the
        remaining queued jobs. Every processed job is acknowledged so the main process can
        stop tracking it as pending.
        """
        if self.recovery_queue is None:
            return
        while not self.recovery_queue.empty():
            from autosubmit.job.job import Job
            try:
                job_data = self.recovery_queue.get(timeout=5)
            except queue.Empty:
                break
            job = Job(loaded_data=job_data)
            job.platform_name = self.name  # Change the original platform to this process platform.
            job.platform = self
            try:
                self._recover_job(job, self._as_conf)
            except Exception as e:
                Log.warning(f"[{self.name}] Log recovery failed for job {job.name}: {e}")
                Log.debug(traceback.format_exc())
                # A failed job may mean the connection was dropped, restore it before the next job.
                self.restore_connection(self._as_conf, log_recovery_process=True)
            finally:
                if self.recovery_results is not None:
                    try:
                        self.recovery_results.put(job.name)
                    except Exception as e:
                        Log.debug(f"[{self.name}] Failed to acknowledge log recovery: {e}")

    def recover_platform_job_logs(self, as_conf: 'AutosubmitConfig') -> None:
        """Recovers the logs of the jobs that have been submitted.
        When this is executed as a process, the exit is controlled by the work_event and cleanup_events of the main process.
        """
        self._as_conf = as_conf
        setproctitle.setproctitle(f"autosubmit log {self.expid} recovery {self.name.lower()}")
        identifier = f"{self.name.lower()}(log_recovery):"
        try:
            Log.info(f"{identifier} Starting...")
            self.connected = False
            self.restore_connection(as_conf, log_recovery_process=True)
            Log.result(f"{identifier} successfully connected.")
            self.keep_alive_timeout = self.config.get("LOG_RECOVERY_TIMEOUT", 60 * 5)
            while not self.cleanup_event.is_set() and self.wait_for_work():
                try:
                    self.recover_job_log()
                except Exception as e:
                    Log.debug(f'{identifier} Error during log recovery: {e}')
                    Log.debug(traceback.format_exc())
                    self.restore_connection(as_conf, log_recovery_process=True)
            self.recover_job_log()
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

    def delete_previous_run_files_by_job_names(self, job_names: list[str]) -> None:
        """Delete the COMPLETED and FAILED files for the given job names from the remote log directory.

        :param job_names: List of job names whose COMPLETED and FAILED files should be deleted.
        :type job_names: list[str]
        """
        raise NotImplementedError  # pragma: no cover

    def delete_previous_stat_files_by_job_names(self, job_names: list[str]) -> None:
        """Delete all previous STAT files for the given job names from the remote log directory.

        :param job_names: List of job names whose STAT files should be deleted.
        :type job_names: list[str]
        """
        raise NotImplementedError  # pragma: no cover

    def confirm_done_jobs_via_stat(self, job_list: list) -> dict[str, "Status"]:
        """Confirm that jobs marked as done are actually completed by checking their STAT files.

        :param job_list: List of jobs to confirm.
        :param has_internal_retries: Indicates if the jobs have internal retries, which affects the STAT file naming convention.
        :return: List of jobs that are confirmed as completed.
        """
        raise NotImplementedError  # pragma: no cover
