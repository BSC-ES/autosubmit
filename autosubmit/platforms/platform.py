import os

from log.log import Log
import traceback
from autosubmit.job.job_common import Status
from typing import List, Union


class Platform(object):
    """
    Class to manage the connections to the different platforms.
    """

    def __init__(self, expid, name, config):
        """
        :param config:
        :param expid:
        :param name:
        """
        self.expid = expid # type: str
        self.name = name # type: str
        self.config = config
        self.tmp_path = os.path.join(
            self.config.LOCAL_ROOT_DIR, self.expid, self.config.LOCAL_TMP_DIR)
        self._serial_platform = None
        self._serial_queue = None
        self._default_queue = None
        self.processors_per_node = None
        self.ec_queue = "hpc"
        self.scratch_free_space = None
        self.custom_directives = None
        self.host = ''
        self.user = ''
        self.project = ''
        self.budget = ''
        self.reservation = ''
        self.exclusivity = ''
        self.type = ''
        self.scratch = ''
        self.project_dir = ''
        self.temp_dir = ''
        self.root_dir = ''
        self.service = None
        self.scheduler = None
        self.directory = None
        self.hyperthreading = 'false'
        self.max_wallclock = ''
        self.total_jobs = None
        self.max_processors = None
        self._allow_arrays = False
        self._allow_wrappers = False
        self._allow_python_jobs = True
        self.bashrc_output = ""
        self.bashrc_err = ""

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
    def queue(self):
        """
        Queue to use for jobs
        :return: queue's name
        :rtype: str
        """
        if self._default_queue is None:
            return ''
        return self._default_queue

    @queue.setter
    def queue(self, value):
        self._default_queue = value

    @property
    def serial_queue(self):
        """
        Queue to use for serial jobs
        :return: queue's name
        :rtype: str
        """
        if self._serial_queue is None:
            return self.queue
        return self._serial_queue

    @serial_queue.setter
    def serial_queue(self, value):
        self._serial_queue = value

    @property
    def allow_arrays(self):
        return self._allow_arrays is True

    @property
    def allow_wrappers(self):
        return self._allow_wrappers is True

    @property
    def allow_python_jobs(self):
        return self._allow_python_jobs is True

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

    def send_file(self, filename):
        """
        Sends a local file to the platform
        :param filename: name of the file to send
        :type filename: str
        """
        raise NotImplementedError

    def move_file(self, src, dest, must_exists = False, path_root = None):
        """
        Moves a file on the platform
        :param src: source name
        :type src: str
        :param dest: destination name
        :type dest: str
        """
        raise NotImplementedError
    def get_jobid_by_jobname_cmd(self, job_name,minutes="5"):
        return ""
    def get_file(self, filename, must_exist=True, relative_path='', ignore_log=False, wrapper_failed=False, max_tries = 1):
        """
        Copies a file from the current platform to experiment's tmp folder

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
        :return: True if succesful or file does no exists
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

    def get_stat_file(self, exp_id, job_name):
        """
        Get the given stat files for all retrials
        :param exp_id: experiment id
        :type exp_id: str
        :param remote_logs: names of the log files
        :type remote_logs: (str, str)
        """
        self.get_files(job_name,False, 'LOG_{0}'.format(exp_id))

    def get_completed_files(self, job_name, retries=0, recovery=False, wrapper_failed=False):
        """
        Get the COMPLETED file of the given job


        :param job_name: name of the job
        :type job_name: str
        :param retries: Max number of tries to get the file
        :type retries: int
        :return: True if successful, false otherwise
        :rtype: bool
        """
        if recovery:
            if self.get_file('{0}_COMPLETED'.format(job_name), False, ignore_log=recovery, max_tries = 5):
                return True
            else:
                return False
        if self.check_file_exists('{0}_COMPLETED'.format(job_name), wrapper_failed=wrapper_failed):
            if self.get_file('{0}_COMPLETED'.format(job_name), True, wrapper_failed=wrapper_failed):
                return True
            else:
                return False
        else:
            return False
    def get_bashrc_output(self):
        """
        Checks remote bashrc output/err to strip out any unwanted output
        """
        try:
            self.send_command("sleep 1")
            self.bashrc_output = self.get_ssh_output()
            self.bashrc_err = self.get_ssh_output_err()
            return True
        except:
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

    def check_file_exists(self, src, wrapper_failed=False):
        return True

    def get_stat_file(self, job_name, retries=0):
        """
        Copies *STAT* files from remote to local

        :param retries: number of intents to get the completed files
        :type retries: int
        :param job_name: name of job to check
        :type job_name: str
        :return: True if succesful, False otherwise
        :rtype: bool
        """
        filename = job_name + '_STAT'
        stat_local_path = os.path.join(
            self.config.LOCAL_ROOT_DIR, self.expid, self.config.LOCAL_TMP_DIR, filename)
        if os.path.exists(stat_local_path):
            os.remove(stat_local_path)
        if self.check_file_exists(filename):
            if self.get_file(filename, True):
                Log.debug('{0}_STAT file have been transfered', job_name)
                return True
        Log.debug('{0}_STAT file not found', job_name)
        return False

    def check_stat_file_by_retrials(self, job_name, retries=0):
        """
         check *STAT* file

         :param retries: number of intents to get the completed files
         :type retries: int
         :param job_name: name of job to check
         :type job_name: str
         :return: True if succesful, False otherwise
         :rtype: bool
         """
        filename = job_name
        if self.check_file_exists(filename):
            return True
        else:
            return False

    def get_stat_file_by_retrials(self, job_name, retries=0):
        """
        Copies *STAT* files from remote to local

        :param retries: number of intents to get the completed files
        :type retries: int
        :param job_name: name of job to check
        :type job_name: str
        :return: True if succesful, False otherwise
        :rtype: bool
        """
        filename = job_name
        stat_local_path = os.path.join(
            self.config.LOCAL_ROOT_DIR, self.expid, self.config.LOCAL_TMP_DIR, filename)
        if os.path.exists(stat_local_path):
            os.remove(stat_local_path)
        if self.check_file_exists(filename):
            if self.get_file(filename, True):
                return True
            else:
                return False
        else:
            return False

    def get_files_path(self):
        """
        Get the path to the platform's LOG directory

        :return: platform's LOG directory
        :rtype: str
        """
        if self.type == "local":
            path = os.path.join(
                self.root_dir, self.config.LOCAL_TMP_DIR, 'LOG_{0}'.format(self.expid))
        else:
            path = os.path.join(self.root_dir, 'LOG_{0}'.format(self.expid))
        return path

    def submit_job(self, job, script_name, hold=False, export="none"):
        """
        Submit a job from a given job object.

        :param job: job object
        :type job: autosubmit.job.job.Job
        :param scriptname: job script's name
        :rtype scriptname: str
        :return: job id for the submitted job
        :rtype: int
        """
        raise NotImplementedError

    def check_job(self, job, default_status=Status.COMPLETED, retries=5, submit_hold_check=False, is_wrapper=False):
        """
        Checks job running status

        :param retries: retries
        :param jobid: job id
        :type jobid: str
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
            title_job = "[INFO] JOBID=" + str(jobid)
            if os.path.exists(complete_path):
                file_type = complete_path[-3:]
                if file_type == "out" or file_type == "err":
                    with open(complete_path, "r+") as f:
                        # Reading into memory (Potentially slow)
                        first_line = f.readline()
                        # Not rewrite
                        if not first_line.startswith("[INFO] JOBID="):
                            content = f.read()
                            # Write again (Potentially slow)
                            # start = time()
                            # Log.info("Attempting job identification of " + str(jobid))
                            f.seek(0, 0)
                            f.write(title_job + "\n\n" + first_line + content)
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
                    with open(complete_path, "a") as f:
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
