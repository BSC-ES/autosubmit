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

import locale
import os
import subprocess
from pathlib import Path
from time import sleep
from typing import Optional, Union, TYPE_CHECKING

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import Log, AutosubmitError
from autosubmit.platforms.headers.local_header import LocalHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
import autosubmit.log.utils as log_utils

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig


class LocalPlatform(ParamikoPlatform):
    """Class to manage jobs to localhost."""

    def __init__(self, expid: str, name: str, config: dict, auth_password: Optional[Union[str, list[str]]] = None):
        ParamikoPlatform.__init__(self, expid, name, config, auth_password=auth_password)
        self.cancel_cmd = None
        self.mkdir_cmd = None
        self.del_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._checkhost_cmd = None
        self.type = 'local'
        self._header = LocalHeader()
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['1']
        self.job_status['RUNNING'] = ['0']
        self.job_status['QUEUING'] = []
        self.job_status['FAILED'] = []
        self._allow_wrappers = False

        self.update_cmds()

    def submit_script(self, hold=False):
        pass

    def parse_all_jobs_output(self, output, job_id):
        pass

    def parse_queue_reason(self, output, job_id):
        pass

    def get_check_all_jobs_cmd(self, jobs_id):
        pass

    def create_a_new_copy(self):
        return LocalPlatform(self.expid, self.name, self.config)

    def update_cmds(self):
        """Updates commands for platforms."""
        self.root_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "tmp", 'LOG_' + self.expid)
        self.cancel_cmd = "kill -SIGINT"
        self._checkhost_cmd = "echo 1"
        self.put_cmd = "cp -p"
        self.get_cmd = "cp"
        self.del_cmd = "rm -f"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def parse_job_output(self, output):
        return output[0]

    def get_submitted_job_id(self, output, x11=False):
        return output

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if job:  # Not intuitive at all, but if it is not a job, it is a wrapper
            seconds = job.wallclock_in_seconds
        else:
            # TODO for another branch this, it is to add a timeout to the wrapped jobs even if the wallclock is 0, default to 2 days
            seconds = 60 * 60 * 24 * 2
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        command = self.get_call(job_script, job, export=export, timeout=seconds)
        return f"cd {self.remote_log_dir} ; {command}"

    def get_check_job_cmd(self, job_id):
        return self.get_pscall(job_id)

    def write_jobid(self, jobid: str, complete_path: str) -> None:
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
                            f.seek(0, 0)
                            f.write(title_job + b"\n\n" + first_line + content)
                        f.close()
        except Exception as exc:
            Log.error("Writing Job Id Failed : " + str(exc))

    def connect(self, as_conf: 'AutosubmitConfig', reconnect: bool = False, log_recovery_process: bool = False) -> None:
        """Establishes an SSH connection to the host.

        :param as_conf: The Autosubmit configuration object.
        :param reconnect: Indicates whether to attempt reconnection if the initial connection fails.
        :param log_recovery_process: Specifies if the call is made from the log retrieval process.
        :return: None
        """
        self.connected = True
        if log_recovery_process:
            self.spawn_log_retrieval_process(as_conf)

    def test_connection(self, as_conf: 'AutosubmitConfig') -> None:
        if not self.connected:
            self.connect(as_conf)

    def restore_connection(self, as_conf: 'AutosubmitConfig', log_recovery_process: bool = False) -> None:
        """Restores the SSH connection to the platform.

        :param as_conf: The Autosubmit configuration object used to establish the connection.
        :type as_conf: AutosubmitConfig
        :param log_recovery_process: Indicates that the call is made from the log retrieval process.
        :type log_recovery_process: bool
        """
        self.connected = True

    def check_all_jobs(self, job_list, as_conf, retries=5):
        for job, prev_job_status in job_list:
            self.check_job(job)

    def send_command(self, command, ignore_log=False, x11=False) -> bool:

        lang = locale.getlocale()[1]
        if lang is None:
            lang = locale.getdefaultlocale()[1]
            if lang is None:
                lang = 'UTF-8'
        try:
            output = subprocess.check_output(command.encode(lang), shell=True)
        except subprocess.CalledProcessError as e:
            if not ignore_log:
                Log.error(f'Could not execute command {e.cmd} on {self.host}')
            return False
        self._ssh_output = output.decode(lang)
        Log.debug(f"Command '{command}': {self._ssh_output}")

        return True

    def send_file(self, filename: str, check: bool = True) -> bool:
        """Sends a file to a specified location using a command.

        :param filename: The name of the file to send.
        :type filename: str
        :param check: Unused in this platform.
        :type check: bool
        :return: True if the file was sent successfully.
        :rtype: bool
        """
        command = (f'{self.put_cmd} {os.path.join(self.tmp_path, Path(filename).name)} '
                   f'{os.path.join(self.tmp_path, "LOG_" + self.expid, Path(filename).name)}; '
                   f'chmod 770 {os.path.join(self.tmp_path, "LOG_" + self.expid, Path(filename).name)}')
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            Log.error(
                f'Could not send file {os.path.join(self.tmp_path, filename)} to {os.path.join(self.tmp_path, f"LOG_{self.expid}", filename)}')
            raise
        return True

    def remove_multiple_files(self, filenames: str) -> str:
        """Creates a shell script to remove multiple files in the remote and sets the appropriate permissions.

        :param filenames: A string containing the filenames to be removed.
        :type filenames: str
        :return: An empty string.
        :rtype: str
        """
        # This function is a copy of the slurm one
        log_dir = os.path.join(self.tmp_path, f'LOG_{self.expid}')
        multiple_delete_previous_run = os.path.join(
            log_dir, "multiple_delete_previous_run.sh")
        if os.path.exists(log_dir):
            lang = locale.getlocale()[1]
            if lang is None:
                lang = 'UTF-8'
            open(multiple_delete_previous_run, 'wb+').write(("rm -f" + filenames).encode(lang))
            os.chmod(multiple_delete_previous_run, 0o770)
        return ""

    def get_file(self, filename, must_exist=True, relative_path='', ignore_log=False, wrapper_failed=False):
        local_path = os.path.join(self.tmp_path, relative_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        file_path = os.path.join(local_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
        command = f'{self.get_cmd} {os.path.join(self.tmp_path, f"LOG_{self.expid}", filename)} {file_path}'
        try:
            subprocess.check_call(command, stdout=open(os.devnull, 'w'), stderr=open(os.devnull, 'w'), shell=True)
        except subprocess.CalledProcessError:
            if must_exist:
                raise Exception(f'File {filename} does not exists')
            return False
        return True

    def check_remote_permissions(self) -> bool:
        return True

    # Moves .err .out
    def check_file_exists(self, src: str, wrapper_failed: bool = False, sleeptime: int = 1,
                          max_retries: int = 1) -> bool:
        """Checks if a file exists in the platform.

        :param src: source name.
        :type src: str
        :param wrapper_failed: Checks inner jobs files. Defaults to False.
        :type wrapper_failed: bool
        :param sleeptime: Time to sleep between retries. Defaults to 1.
        :type sleeptime: int
        :param max_retries: Maximum number of retries. Defaults to 1.
        :type max_retries: int
        :return: True if the file exists, False otherwise.
        :rtype: bool
        """
        # This function has a short sleep as the files are locally
        sleeptime = 1
        for i in range(max_retries):
            if os.path.isfile(os.path.join(self.get_files_path(), src)):
                return True
            sleep(sleeptime)
        Log.warning(f"File {src} does not exist")
        return False

    def delete_file(self, filename, del_cmd=False):
        if del_cmd:
            command = f'{self.del_cmd} {os.path.join(self.tmp_path, "LOG_" + self.expid, filename)}'
        else:
            command = f'{self.del_cmd} {os.path.join(self.tmp_path, "LOG_" + self.expid, filename)}'
            command += f' ; {self.del_cmd} {os.path.join(self.tmp_path, filename)}'
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            Log.debug(f'Could not remove file {os.path.join(self.tmp_path, filename)}')
            return False
        return True

    def move_file(self, src, dest, must_exist=False):
        """Moves a file on the platform (includes .err and .out)

        :param src: source name.
        :type src: str
        :param dest: destination name.
        :type dest: str
        :param must_exist: ignore if file exist or not.
        :type must_exist: bool
        """
        path_root = ""
        try:
            path_root = self.get_files_path()
            os.rename(os.path.join(path_root, src), os.path.join(path_root, dest))
            return True
        except IOError as e:
            if must_exist:
                raise AutosubmitError(f"File {os.path.join(path_root, src)} does not exists", 6004, str(e))
            else:
                Log.debug(f"File {path_root} doesn't exists ")
                return False
        except Exception as e:
            if str(e) in "Garbage":
                raise AutosubmitError(f'File {os.path.join(self.get_files_path(), src)} does not exists', 6004, str(e))
            if must_exist:
                raise AutosubmitError(f"File {os.path.join(self.get_files_path(), src)} does not exists", 6004, str(e))
            else:
                Log.printlog(f"Log file couldn't be moved: {os.path.join(self.get_files_path(), src)}", 5001)
                return False

    def get_ssh_output(self):
        return self._ssh_output

    def get_ssh_output_err(self):
        return self._ssh_output_err

    def get_logs_files(self, exp_id: str, remote_logs: tuple[str, str]) -> None:
        """Do nothing because the log files are already in the local platform (redundancy)."""
        return

    def check_completed_files(self, sections: str = None) -> Optional[str]:
        """Checks for completed files in the remote log directory.
        This function is used to check inner_jobs of a wrapper.

        :param sections: Space-separated string of sections to check for completed files. Defaults to None.
        :type sections: str
        :return: The output if the command is successful, None otherwise.
        :rtype: str
        """
        # Clone of the slurm one.
        command = "find %s " % self.remote_log_dir
        if sections:
            for i, section in enumerate(sections.split()):
                command += " -name *%s_COMPLETED" % section
                if i < len(sections.split()) - 1:
                    command += " -o "
        else:
            command += " -name *_COMPLETED"

        if self.send_command(command, True):
            return self._ssh_output
        return None

    def get_file_size(self, src: Union[str, Path]) -> Union[int, None]:
        """Get file size in bytes

        :param src: file path
        """
        try:
            return Path(src).stat().st_size
        except Exception as e:
            Log.debug(f"Error getting file size for {src}: {str(e)}")
        return None

    def read_file(self, src: Union[str, Path], max_size: int = None) -> Union[bytes, None]:
        """Read file content as bytes. If max_size is set, only the first max_size bytes are read.

        :param src: file path
        :param max_size: maximum size to read
        """
        try:
            with open(src, "rb") as f:
                return f.read(max_size)
        except Exception as e:
            Log.debug(f"Error reading file {src}: {str(e)}")
        return None

    def compress_file(self, file_path: str) -> None:
        Log.debug(f"Compressing file {file_path} using {self.remote_logs_compress_type}")
        try:
            compression_level = self.compression_level
            if self.remote_logs_compress_type == "xz":
                output = log_utils.compress_xz(
                    file_path, preset=compression_level, keep_input=False
                )
            else:
                output = log_utils.compress_gzip(
                    file_path, compression_level=compression_level, keep_input=False
                )

            Log.debug(f"File {file_path} compressed")
            return output
        except Exception as exc:
            Log.error(f"Error compressing file {file_path}: {exc}")
        return None
