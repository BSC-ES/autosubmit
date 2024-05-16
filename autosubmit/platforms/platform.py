import atexit

import queue
import setproctitle
import locale
import os

import traceback
from autosubmit.job.job_common import Status
from typing import List, Union

from autosubmit.helpers.parameters import autosubmit_parameter
from log.log import AutosubmitCritical, AutosubmitError, Log
from multiprocessing import Process, Queue, Event

# stop the background task gracefully before exit
def stop_background(stop_event, process):
    # request the background thread stop
    stop_event.set()
    # wait for the background thread to stop
    process.join()

def processed(fn):
    def wrapper(*args, **kwargs):
        stop_event = Event()
        args = (args[0], stop_event)
        process = Process(target=fn, args=args, kwargs=kwargs, name=f"{args[0].name}_platform")
        process.daemon = True  # Set the process as a daemon process
        process.start()
        atexit.register(stop_background, stop_event, process)
        return process

    return wrapper

class Platform(object):
    """
    Class to manage the connections to the different platforms.
    """

    def __init__(self, expid, name, config, auth_password = None):
        """
        :param config:
        :param expid:
        :param name:
        """
        self.connected = False
        self.expid = expid # type: str
        self._name = name # type: str
        self.config = config
        self.tmp_path = os.path.join(
            self.config.get("LOCAL_ROOT_DIR"), self.expid, self.config.get("LOCAL_TMP_DIR"))
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
        self.otp_timeout = self.config.get("PLATFORMS", {}).get(self.name.upper(),{}).get("2FA_TIMEOUT", 60*5)
        self.two_factor_auth = self.config.get("PLATFORMS", {}).get(self.name.upper(),{}).get("2FA", False)
        self.two_factor_method = self.config.get("PLATFORMS", {}).get(self.name.upper(),{}).get("2FA_METHOD", "token")
        if not self.two_factor_auth:
            self.pw = None
        elif auth_password is not None and self.two_factor_auth:
            if type(auth_password) == list:
                self.pw = auth_password[0]
            else:
                self.pw = auth_password
        else:
            self.pw = None
        self.recovery_queue = Queue()
        self.log_retrieval_process_active = False


    @property
    @autosubmit_parameter(name='current_arch')
    def name(self):
        """Platform name."""
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    @autosubmit_parameter(name='current_host')
    def host(self):
        """Platform url."""
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    @property
    @autosubmit_parameter(name='current_user')
    def user(self):
        """Platform user."""
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @property
    @autosubmit_parameter(name='current_proj')
    def project(self):
        """Platform project."""
        return self._project

    @project.setter
    def project(self, value):
        self._project = value

    @property
    @autosubmit_parameter(name='current_budg')
    def budget(self):
        """Platform budget."""
        return self._budget

    @budget.setter
    def budget(self, value):
        self._budget = value

    @property
    @autosubmit_parameter(name='current_reservation')
    def reservation(self):
        """You can configure your reservation id for the given platform."""
        return self._reservation

    @reservation.setter
    def reservation(self, value):
        self._reservation = value

    @property
    @autosubmit_parameter(name='current_exclusivity')
    def exclusivity(self):
        """True if you want to request exclusivity nodes."""
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
        """Platform scheduler type."""
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    @autosubmit_parameter(name='current_scratch_dir')
    def scratch(self):
        """Platform's scratch folder path."""
        return self._scratch

    @scratch.setter
    def scratch(self, value):
        self._scratch = value

    @property
    @autosubmit_parameter(name='current_proj_dir')
    def project_dir(self):
        """Platform's project folder path."""
        return self._project_dir

    @project_dir.setter
    def project_dir(self, value):
        self._project_dir = value

    @property
    @autosubmit_parameter(name='current_rootdir')
    def root_dir(self):
        """Platform's experiment folder path."""
        return self._root_dir

    @root_dir.setter
    def root_dir(self, value):
        self._root_dir = value

    def get_exclusive_directive(self, job):
        """
        Returns exclusive directive for the specified job
        :param job: job to create exclusive directive for
        :type job: Job
        :return: exclusive directive
        :rtype: str
        """
        # only implemented for slurm
        return ""
    def get_multiple_jobids(self,job_list,valid_packages_to_submit,failed_packages,error_message="",hold=False):
        return False,valid_packages_to_submit
        #raise NotImplementedError

    def process_batch_ready_jobs(self, valid_packages_to_submit, failed_packages, error_message="", hold=False):
        return True, valid_packages_to_submit

    def submit_ready_jobs(self, as_conf, job_list, platforms_to_test, packages_persistence, packages_to_submit,
                          inspect=False, only_wrappers=False, hold=False):

        """
        Gets READY jobs and send them to the platforms if there is available space on the queues

        :param hold:
        :param packages_to_submit:
        :param as_conf: autosubmit config object \n
        :type as_conf: AutosubmitConfig object  \n
        :param job_list: job list to check  \n
        :type job_list: JobList object  \n
        :param platforms_to_test: platforms used  \n
        :type platforms_to_test: set of Platform Objects, e.g. SgePlatform(), LsfPlatform().  \n
        :param packages_persistence: Handles database per experiment. \n
        :type packages_persistence: JobPackagePersistence object \n
        :param inspect: True if coming from generate_scripts_andor_wrappers(). \n
        :type inspect: Boolean \n
        :param only_wrappers: True if it comes from create -cw, False if it comes from inspect -cw. \n
        :type only_wrappers: Boolean \n
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
            Log.debug("\nJobs ready for {1}: {0}", len(
                job_list.get_ready(self, hold=hold)), self.name)
            ready_jobs = job_list.get_ready(self, hold=hold)
        else:
            Log.debug("\nJobs prepared for {1}: {0}", len(
                job_list.get_prepared(self)), self.name)
        if not inspect:
            self.open_submit_script()
        valid_packages_to_submit = []  # type: List[JobPackageBase]
        for package in packages_to_submit:
            try:
                # If called from inspect command or -cw
                if only_wrappers or inspect:
                    if hasattr(package, "name"):
                        job_list.packages_dict[package.name] = package.jobs
                        from ..job.job import WrapperJob
                        wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.READY, 0,
                                                 package.jobs,
                                                 package._wallclock, package._num_processors,
                                                 package.platform, as_conf, hold)
                        job_list.job_package_map[package.jobs[0].id] = wrapper_job
                        packages_persistence.save(
                            package.name, package.jobs, package._expid, inspect)
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
                        # Log.debug("FD end-submit: {0}".format(log.fd_show.fd_table_status_str(open()))
                    except (IOError, OSError):
                        if package.jobs[0].id != 0:
                            failed_packages.append(package.jobs[0].id)
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
                                error_message += "\ncheck job and queue specified in jobs.conf. Sections that could be affected: {0}".format(
                                    error_msg[:-1])
                            else:
                                error_message += "\ncheck that {1} platform has set the correct scheduler. Sections that could be affected: {0}".format(
                                    error_msg[:-1], self.name)
                    except AutosubmitCritical:
                        raise
                    except Exception as e:
                        self.connected = False
                        raise

            except AutosubmitCritical as e:
                raise AutosubmitCritical(e.message, e.code, e.trace)
            except AutosubmitError as e:
                raise
            except Exception as e:
                raise
        if valid_packages_to_submit:
            any_job_submitted = True
        return save, failed_packages, error_message, valid_packages_to_submit, any_job_submitted

    @property
    def serial_platform(self):
        """
        Platform to use for serial jobs
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
        """
        Partition to use for jobs.

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
        """
        Queue to use for jobs
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
        """
        Partition to use for serial jobs
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
        """
        Queue to use for serial jobs
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

    def add_parameters(self, parameters, main_hpc=False):
        """
        Add parameters for the current platform to the given parameters list

        :param parameters: parameters list to update
        :type parameters: dict
        :param main_hpc: if it's True, uses HPC instead of NAME_ as prefix for the parameters
        :type main_hpc: bool
        """
        if main_hpc:
            prefix = 'HPC'
            parameters['SCRATCH_DIR'.format(prefix)] = self.scratch
        else:
            prefix = self.name + '_'

        parameters['{0}ARCH'.format(prefix)] = self.name
        parameters['{0}HOST'.format(prefix)] = self.host
        parameters['{0}QUEUE'.format(prefix)] = self.queue
        parameters['{0}EC_QUEUE'.format(prefix)] = self.ec_queue
        parameters['{0}PARTITION'.format(prefix)] = self.partition


        parameters['{0}USER'.format(prefix)] = self.user
        parameters['{0}PROJ'.format(prefix)] = self.project
        parameters['{0}BUDG'.format(prefix)] = self.budget
        parameters['{0}RESERVATION'.format(prefix)] = self.reservation
        parameters['{0}EXCLUSIVITY'.format(prefix)] = self.exclusivity
        parameters['{0}TYPE'.format(prefix)] = self.type
        parameters['{0}SCRATCH_DIR'.format(prefix)] = self.scratch
        parameters['{0}TEMP_DIR'.format(prefix)] = self.temp_dir
        if self.temp_dir is None:
            self.temp_dir = ''
        parameters['{0}ROOTDIR'.format(prefix)] = self.root_dir

        parameters['{0}LOGDIR'.format(prefix)] = self.get_files_path()

    def send_file(self, filename, check=True):
        """
        Sends a local file to the platform
        :param check:
        :param filename: name of the file to send
        :type filename: str
        """
        raise NotImplementedError

    def move_file(self, src, dest):
        """
        Moves a file on the platform
        :param src: source name
        :type src: str
        :param dest: destination name
        :type dest: str
        """
        raise NotImplementedError

    def get_file(self, filename, must_exist=True, relative_path='', ignore_log=False, wrapper_failed=False):
        """
        Copies a file from the current platform to experiment's tmp folder

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
        raise NotImplementedError

    def get_files(self, files, must_exist=True, relative_path=''):
        """
        Copies some files from the current platform to experiment's tmp folder

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

    def delete_file(self, filename):
        """
        Deletes a file from this platform

        :param filename: file name
        :type filename: str
        :return: True if successful or file does not exist
        :rtype: bool
        """
        raise NotImplementedError

    # Executed when calling from Job
    def get_logs_files(self, exp_id, remote_logs):
        """
        Get the given LOGS files

        :param exp_id: experiment id
        :type exp_id: str
        :param remote_logs: names of the log files
        :type remote_logs: (str, str)
        """
        (job_out_filename, job_err_filename) = remote_logs
        self.get_files([job_out_filename, job_err_filename], False, 'LOG_{0}'.format(exp_id))

    def get_checkpoint_files(self, job):
        """
        Get all the checkpoint files of a job
        :param job: Get the checkpoint files
        :type job: Job
        :param max_step: max step possible
        :type max_step: int
        """

        if job.current_checkpoint_step < job.max_checkpoint_step:
            remote_checkpoint_path = f'{self.get_files_path()}/CHECKPOINT_'
            self.get_file(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}', False, ignore_log=True)
            while self.check_file_exists(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}') and job.current_checkpoint_step < job.max_checkpoint_step:
                self.remove_checkpoint_file(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}')
                job.current_checkpoint_step += 1
                self.get_file(f'{remote_checkpoint_path}{str(job.current_checkpoint_step)}', False, ignore_log=True)
    def get_completed_files(self, job_name, retries=0, recovery=False, wrapper_failed=False):
        """
        Get the COMPLETED file of the given job


        :param wrapper_failed:
        :param recovery:
        :param job_name: name of the job
        :type job_name: str
        :param retries: Max number of tries to get the file
        :type retries: int
        :return: True if successful, false otherwise
        :rtype: bool
        """
        if recovery:
            retries = 5
            for i in range(retries):
                if self.get_file('{0}_COMPLETED'.format(job_name), False, ignore_log=recovery):
                    return True
            return False
        if self.check_file_exists('{0}_COMPLETED'.format(job_name), wrapper_failed=wrapper_failed):
            if self.get_file('{0}_COMPLETED'.format(job_name), True, wrapper_failed=wrapper_failed):
                return True
            else:
                return False
        else:
            return False

    def remove_stat_file(self, job_name):
        """
        Removes *STAT* files from remote

        :param job_name: name of job to check
        :type job_name: str
        :return: True if successful, False otherwise
        :rtype: bool
        """
        filename = job_name + '_STAT'
        if self.delete_file(filename):
            Log.debug('{0}_STAT have been removed', job_name)
            return True
        return False

    def remove_stat_file_by_retrials(self, job_name):
        """
        Removes *STAT* files from remote

        :param job_name: name of job to check
        :type job_name: str
        :return: True if successful, False otherwise
        :rtype: bool
        """
        filename = job_name
        if self.delete_file(filename):
            return True
        return False

    def remove_completed_file(self, job_name):
        """
        Removes *COMPLETED* files from remote

        :param job_name: name of job to check
        :type job_name: str
        :return: True if successful, False otherwise
        :rtype: bool
        """
        filename = job_name + '_COMPLETED'
        if self.delete_file(filename):
            Log.debug('{0} been removed', filename)
            return True
        return False
    def remove_checkpoint_file(self, filename):
        """
        Removes *CHECKPOINT* files from remote

        :param job_name: name of job to check
        :return: True if successful, False otherwise
        """
        if self.check_file_exists(filename):
            self.delete_file(filename)

    def check_file_exists(self, src, wrapper_failed=False, sleeptime=5, max_retries=3, first=True):
        return True

    def get_stat_file(self, job_name, retries=0, count = -1):
        """
        Copies *STAT* files from remote to local

        :param retries: number of intents to get the completed files
        :type retries: int
        :param job_name: name of job to check
        :type job_name: str
        :return: True if successful, False otherwise
        :rtype: bool
        """
        if count == -1: # No internal retrials
            filename = job_name + '_STAT'
        else:
            filename = job_name + '_STAT_{0}'.format(str(count))
        stat_local_path = os.path.join(
            self.config.get("LOCAL_ROOT_DIR"), self.expid, self.config.get("LOCAL_TMP_DIR"), filename)
        if os.path.exists(stat_local_path):
            os.remove(stat_local_path)
        if self.check_file_exists(filename):
            if self.get_file(filename, True):
                Log.debug('{0}_STAT file have been transferred', job_name)
                return True
        Log.debug('{0}_STAT file not found', job_name)
        return False


    @autosubmit_parameter(name='current_logdir')
    def get_files_path(self):
        """
        The platform's LOG directory.

        :return: platform's LOG directory
        :rtype: str
        """
        if self.type == "local":
            path = os.path.join(
                self.root_dir, self.config.get("LOCAL_TMP_DIR"), 'LOG_{0}'.format(self.expid))
        else:
            path = os.path.join(self.root_dir, 'LOG_{0}'.format(self.expid))
        return path

    def submit_job(self, job, script_name, hold=False, export="none"):
        """
        Submit a job from a given job object.

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
        raise NotImplementedError
    def check_Alljobs(self, job_list, as_conf, retries=5):
        for job,job_prev_status in job_list:
            self.check_job(job)
    def check_job(self, job, default_status=Status.COMPLETED, retries=5, submit_hold_check=False, is_wrapper=False):
        """
        Checks job running status

        :param is_wrapper:
        :param submit_hold_check:
        :param job:
        :param retries: retries
        :param default_status: status to assign if it can be retrieved from the platform
        :type default_status: autosubmit.job.job_common.Status
        :return: current job status
        :rtype: autosubmit.job.job_common.Status
        """
        raise NotImplementedError

    def closeConnection(self):
        return

    def write_jobid(self, jobid, complete_path):
        """
        Writes Job id in an out file.

        :param jobid: job id
        :type jobid: str
        :param complete_path: complete path to the file, includes filename
        :type complete_path: str
        :return: Modifies file and returns True, False if file could not be modified
        :rtype: Boolean
        """
        try:
            lang = locale.getlocale()[1]
            if lang is None:
                lang = locale.getdefaultlocale()[1]
                if lang is None:
                    lang = 'UTF-8'
            title_job = b"[INFO] JOBID=" + str(jobid).encode(lang)
            if os.path.exists(complete_path):
                file_type = complete_path[-3:]
                if file_type == "out" or file_type == "err":
                    with open(complete_path, "rb+") as f:
                        # Reading into memory (Potentially slow)
                        first_line = f.readline()
                        # Not rewrite
                        if not first_line.startswith(b'[INFO] JOBID='):
                            content = f.read()
                            # Write again (Potentially slow)
                            # start = time()
                            # Log.info("Attempting job identification of " + str(jobid))
                            f.seek(0, 0)
                            f.write(title_job + b"\n\n" + first_line + content)
                        f.close()
                        # finish = time()
                        # Log.info("Job correctly identified in " + str(finish - start) + " seconds")

        except Exception as ex:
            Log.error("Writing Job Id Failed : " + str(ex))

    def write_job_extrainfo(self, job_hdata, complete_path):
        """[summary]

        :param job_hdata: job extra data 
        :type job_hdata: str 
        :param complete_path: complete path to the file, includes filename 
        :type complete_path: str 
        :return: Modifies file and returns True, False if file could not be modified 
        :rtype: Boolean 
        """
        try:
            # footer = "extra_data = {0}".format()
            # print("Complete path {0}".format(complete_path))
            if os.path.exists(complete_path):
                file_type = complete_path[-3:]
                # print("Detected file type {0}".format(file_type))
                if file_type == "out" or file_type == "err":
                    with open(complete_path, "ab") as f:
                        job_footer_info = "[INFO] HDATA={0}".format(job_hdata)
                        f.write(job_footer_info)
                        f.close()
        except Exception as ex:
            Log.debug(traceback.format_exc())
            Log.warning(
                "Autosubmit has not written extra information into the .out log.")
            pass

    def open_submit_script(self):
        # type: () -> None
        """ Opens Submit script file """
        raise NotImplementedError
    
    def submit_Script(self, hold=False):
        # type: (bool) -> Union[List[str], str]
        """
        Sends a Submit file Script, execute it  in the platform and retrieves the Jobs_ID of all jobs at once.
        """
        raise NotImplementedError

    def add_job_to_log_recover(self, job):
        self.recovery_queue.put((job,job.children))

    def connect(self, as_conf, reconnect=False):
        raise NotImplementedError

    def restore_connection(self,as_conf):
        raise NotImplementedError

    @processed
    def recover_job_logs(self, event):
        setproctitle.setproctitle(f"autosubmit log {self.expid} recovery {self.name.lower()}")
        job_names_processed = set()
        self.connected = False
        self.restore_connection(None)
        while not event.is_set():
            try:
                job,children = self.recovery_queue.get()
                if job.wrapper_type != "vertical":
                    if f'{job.name}_{job.fail_count}' in job_names_processed:
                        continue
                else:
                    if f'{job.name}' in job_names_processed:
                        continue
                job.children = children
                job.platform = self
                if job.x11:
                    Log.debug("Job {0} is an X11 job, skipping log retrieval as they're written in the ASLOGS".format(job.name))
                    continue
                try:
                    job.retrieve_logfiles(self, raise_error=True)
                    if job.wrapper_type != "vertical":
                        job_names_processed.add(f'{job.name}_{job.fail_count}')
                    else:
                        job_names_processed.add(f'{job.name}')
                except:
                    pass
            except queue.Empty:
                pass
            except (IOError, OSError):
                pass
            except Exception as e:
                try:
                    self.restore_connection(None)
                except:
                    pass
