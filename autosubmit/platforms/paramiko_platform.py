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

import datetime
import getpass
import hashlib
from io import BufferedReader
import locale
import os
import random
import re
import select
import socket
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Optional, Union, TYPE_CHECKING

import Xlib.support.connect as xlib_connect
import paramiko
from paramiko.agent import Agent
from paramiko.ssh_exception import (SSHException)

from autosubmit.job.job_common import Status
from autosubmit.job.template import Language
from autosubmit.log.log import AutosubmitError, AutosubmitCritical, Log
from autosubmit.platforms.platform import Platform

if TYPE_CHECKING:
    # Avoid circular imports
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job import Job
    from autosubmit.platforms.headers import PlatformHeader


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs, name=f"{args[0].name}_X11")
        thread.start()
        return thread

    return wrapper


def _create_ssh_client() -> paramiko.SSHClient:
    """Create a Paramiko SSH Client.

    Sets up all the attributes required by Autosubmit in the :class:`paramiko.SSHClient`.
    This code is in a separated function for composition and to make it easier
    to write tests that mock the SSH client (as having this function makes it
    a lot easier).
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    return ssh


class ParamikoPlatform(Platform):
    """Class to manage the connections to the different platforms with the Paramiko library."""

    def __init__(self, expid: str, name: str, config: dict, auth_password: Optional[Union[str, list[str]]] = None):
        """An SSH-enabled platform, that uses the Paramiko library.

        :param expid: Experiment ID.
        :param name: Platform name.
        :param config: Dictionary with configuration for the platform.
        :param auth_password: Optional password for 2FA.
        """
        Platform.__init__(self, expid, name, config, auth_password=auth_password)
        self._proxy = None
        self._ssh_output_err = ""
        self.connected = False
        self._default_queue = None
        self.job_status: Optional[dict[str, list]] = None
        self._ssh: Optional[paramiko.SSHClient] = None
        self._ssh_config = None
        self._ssh_output = None
        self._user_config_file = None
        self._host_config = None
        self._host_config_id = None
        self.submit_cmd = ""
        self._ftpChannel: Optional[paramiko.SFTPClient] = None
        self.transport = None
        self.channels = {}
        if sys.platform != "linux":
            self.poller = select.kqueue()
        else:
            self.poller = select.poll()
        self._header = None
        self._wrapper = None
        self.remote_log_dir = ""
        # self.get_job_energy_cmd = ""
        self._init_local_x11_display()

        self.remove_log_files_on_transfer = False
        if self.config:
            platform_config: dict = self.config.get("PLATFORMS", {}).get(
                self.name.upper(), {}
            )
            self.remove_log_files_on_transfer = platform_config.get(
                "REMOVE_LOG_FILES_ON_TRANSFER", False
            )

    @property
    def header(self) -> 'PlatformHeader':
        """Header to add to job for scheduler configuration

        :return: header
        :rtype: object
        """
        return self._header

    @property
    def wrapper(self):
        """Handler to manage wrappers

        :return: wrapper-handler
        :rtype: object
        """
        return self._wrapper

    def reset(self):
        self.close_connection()
        self.connected = False
        self._ssh = None
        self._ssh_config = None
        self._ssh_output = None
        self._user_config_file = None
        self._host_config = None
        self._host_config_id = None
        self._ftpChannel = None
        self.transport = None
        self.channels = {}
        if sys.platform != "linux":
            self.poller = select.kqueue()
        else:
            self.poller = select.poll()
        display = os.getenv('DISPLAY', "localhost:0")
        try:
            self.local_x11_display = xlib_connect.get_display(display)
        except Exception as e:
            Log.warning(f"X11 display not found: {e}")
            self.local_x11_display = None

    def test_connection(self, as_conf: Optional['AutosubmitConfig']) -> Optional[str]:
        """Test if the connection is still alive, reconnect if not."""

        try:
            if not self.connected:
                self.reset()
                try:
                    self.restore_connection(as_conf)
                    message = "OK"
                except Exception as e:
                    message = str(e)
                if message.find("t accept remote connections") == -1:
                    try:
                        transport = self._ssh.get_transport()
                        transport.send_ignore()
                    except Exception as e:
                        Log.debug(f'Test connection error: {str(e)}')
                        message = "Timeout connection"
                        Log.debug(str(e))
                return message

        except EOFError as e:
            self.connected = False
            raise AutosubmitError(f"[{self.name}] not alive. Host: {self.host}", 6002, str(e))
        except (AutosubmitError, AutosubmitCritical, IOError):
            self.connected = False
            raise
        except Exception as e:
            self.connected = False
            raise AutosubmitCritical(str(e), 7051)
        return None

    def restore_connection(self, as_conf: Optional['AutosubmitConfig'], log_recovery_process: bool = False) -> None:
        """Restores the SSH connection to the platform.

        :param as_conf: Autosubmit configuration.
        :param log_recovery_process: Indicates that the call is made from the log retrieval process.
        """
        try:
            self.connected = False
            retries = 2
            retry = 0
            try:
                self.connect(as_conf, log_recovery_process=log_recovery_process)
            except Exception as e:
                if ',' in self.host:
                    Log.printlog(f"Connection Failed to {self.host.split(',')[0]}, will test another host", 6002)
                else:
                    raise AutosubmitCritical(f"First connection to {self.host} is failed, check host configuration"
                                             f" or try another login node ", 7050, str(e))
            while self.connected is False and retry < retries:
                with suppress(Exception):
                    self.connect(as_conf, True, log_recovery_process=log_recovery_process)
                retry += 1
            if not self.connected:
                trace = ('Can not create ssh or sftp connection to {self.host}: Connection could not be established to'
                         ' platform {self.name}\n Please, check your expid on the PLATFORMS definition in YAML to see'
                         ' if there are mistakes in the configuration\n Also Ensure that the login node listed on HOST'
                         ' parameter is available(try to connect via ssh on a terminal)\n Also you can put more than'
                         ' one host using a comma as separator')
                raise AutosubmitCritical(
                    'Experiment cant no continue without unexpected behaviour, Stopping Autosubmit', 7050, trace)

        except AutosubmitCritical:
            raise
        except SSHException:
            raise
        except Exception as e:
            raise AutosubmitCritical(f'Unknown error connection to platform {self.name}: {str(e)}', 7050, str(e))

    def agent_auth(self, port: int) -> bool:
        """Attempt to authenticate to the given SSH server using the most common authentication methods available.
            This will always try to use the SSH agent first, and will fall back to using the others methods if
            that fails.

        :parameter port: port to connect
        :return: True if authentication was successful, False otherwise
        """
        try:
            self._ssh._agent = Agent()
            for key in self._ssh._agent.get_keys():
                if not hasattr(key, "public_blob"):
                    key.public_blob = None
            self._ssh.connect(self._host_config['hostname'], port=port, username=self.user, timeout=60,
                              banner_timeout=60)
        except BaseException as e:
            Log.debug(f'Failed to authenticate with ssh-agent due to {e}')
            Log.debug('Trying to authenticate with other methods')
            return False
        return True

    # NOTE: do not remove title, instructions, as these are in the callback signature for 2FA
    # noinspection PyUnusedLocal
    def interactive_auth_handler(self, title, instructions, prompt_list):
        answers = []
        # Walk the list of prompts that the server sent that we need to answer
        twofactor_nonpush = None
        two_factor_prompts = ["token", "2fa", "otp", "code"]

        for prompt_, _ in prompt_list:
            prompt = str(prompt_).strip().lower()
            # str() used to make sure that we're dealing with a string rather than a unicode string
            # strip() used to get rid of any padding spaces sent by the server
            if "password" in prompt:
                answers.append(self.pw)
            elif any(token in prompt for token in two_factor_prompts):
                if self.two_factor_method == "push":
                    answers.append("")
                elif self.two_factor_method == "token":
                    # Sometimes the server may ask for the 2FA code more than once this is to avoid asking the
                    # user again. If it is wrong, just run again autosubmit run because the issue could be in
                    # the password step.
                    if twofactor_nonpush is None:
                        twofactor_nonpush = input("Please type the 2FA/OTP/token code: ")
                    answers.append(twofactor_nonpush)
        return tuple(answers)

    def map_user_config_file(self, as_conf: 'AutosubmitConfig') -> None:
        """Maps the shared account user ssh config file to the current user config file.
        Defaults to ~/.ssh/config if the mapped file does not exist.
        Defaults to ~/.ssh/config_%AS_ENV_CURRENT_USER% if %AS_ENV_SSH_CONFIG_PATH% is not defined.
        param as_conf: Autosubmit configuration
        return: None
        """
        self._user_config_file = os.path.expanduser("~/.ssh/config")
        if not as_conf.is_current_real_user_owner:  # Using shared account
            if 'AS_ENV_SSH_CONFIG_PATH' not in self.config:
                # if not defined in the ENV variables, use the default + current user
                mapped_config_file = os.path.expanduser(f"~/.ssh/config_{self.config['AS_ENV_CURRENT_USER']}")
            else:
                mapped_config_file = self.config['AS_ENV_SSH_CONFIG_PATH']
            if mapped_config_file.startswith("~"):
                mapped_config_file = os.path.expanduser(mapped_config_file)
            if not Path(mapped_config_file).exists():
                Log.debug(f"{mapped_config_file} not found")
            else:
                Log.info(f"Using {mapped_config_file} as ssh config file")
                self._user_config_file = mapped_config_file

        if Path(self._user_config_file).exists():
            Log.info(f"Using {self._user_config_file} as ssh config file")
            with open(self._user_config_file) as f:
                self._ssh_config.parse(f)
        else:
            Log.warning(f"SSH config file {self._user_config_file} not found")

    def write_jobid(self, jobid: str, complete_path: str) -> None:
        try:
            lang = locale.getlocale()[1]
            if lang is None:
                lang = locale.getdefaultlocale()[1]
                if lang is None:
                    lang = "UTF-8"
            title_job = b"[INFO] JOBID=" + str(jobid).encode(lang)

            if self.check_absolute_file_exists(complete_path):
                file_type = complete_path[-3:]
                if file_type == "out" or file_type == "err":
                    with self._ftpChannel.file(complete_path, "rb+") as f:
                        # Reading into memory (Potentially slow)
                        first_line: bytes = f.readline()
                        # Not rewrite
                        if not first_line.startswith(b"[INFO] JOBID="):
                            content = f.read()
                            f.seek(0, 0)
                            f.write(title_job + b"\n\n" + first_line + content)
                        f.close()

        except Exception as exc:
            Log.error("Writing Job Id Failed : " + str(exc))

    def connect(
            self,
            as_conf: Optional['AutosubmitConfig'],
            reconnect: bool = False,
            log_recovery_process: bool = False
    ) -> None:
        """Establishes an SSH connection to the host.

        :param as_conf: The Autosubmit configuration object.
        :param reconnect: Indicates whether to attempt reconnection if the initial connection fails.
        :param log_recovery_process: Specifies if the call is made from the log retrieval process.
        """
        try:
            self._init_local_x11_display()
            self._ssh = _create_ssh_client()
            self._ssh_config = paramiko.SSHConfig()
            if as_conf:
                self.map_user_config_file(as_conf)
            else:
                with open(os.path.expanduser("~/.ssh/config"), "r") as fd:
                    self._ssh_config.parse(fd)

            self._host_config = self._ssh_config.lookup(self.host)
            if "," in self._host_config['hostname']:
                if reconnect:
                    self._host_config['hostname'] = random.choice(
                        self._host_config['hostname'].split(',')[1:])
                else:
                    self._host_config['hostname'] = self._host_config['hostname'].split(',')[0]
            if 'identityfile' in self._host_config:
                self._host_config_id = self._host_config['identityfile']
            port = int(self._host_config.get('port', 22))
            if not self.two_factor_auth:
                # Agent Auth
                if not self.agent_auth(port):
                    # Public Key Auth
                    if 'proxycommand' in self._host_config:
                        self._proxy = paramiko.ProxyCommand(self._host_config['proxycommand'])
                        try:
                            self._ssh.connect(self._host_config['hostname'], port, username=self.user,
                                              key_filename=self._host_config_id, sock=self._proxy, timeout=60,
                                              banner_timeout=60)
                        except Exception as e:
                            Log.warning(f'Failed to SSH connect to {self._host_config["hostname"]}: {e}')
                            Log.warning('Will try disabling the rsa-sha2-256 and rsa-sha2-512 SSH '
                                        'public key algorithms...')
                            self._ssh.connect(self._host_config['hostname'], port, username=self.user,
                                              key_filename=self._host_config_id, sock=self._proxy, timeout=60,
                                              banner_timeout=60, disabled_algorithms={'pubkeys': ['rsa-sha2-256',
                                                                                                  'rsa-sha2-512']})
                    else:
                        try:
                            self._ssh.connect(self._host_config['hostname'], port, username=self.user,
                                              key_filename=self._host_config_id, timeout=60, banner_timeout=60)
                        except Exception as e:
                            Log.warning(f'Failed to SSH connect to {self._host_config["hostname"]}: {e}')
                            Log.warning('Will try disabling the rsa-sha2-256 and rsa-sha2-512 SSH '
                                        'public key algorithms...')
                            self._ssh.connect(self._host_config['hostname'], port, username=self.user,
                                              key_filename=self._host_config_id, timeout=60, banner_timeout=60,
                                              disabled_algorithms={'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']})
                self.transport = self._ssh.get_transport()
                self.transport.banner_timeout = 60
            else:
                Log.warning("2FA is enabled, this is an experimental feature and it may not work as expected")
                Log.warning("nohup can't be used as the password will be asked")
                Log.warning("If you are using a token, please type the token code when asked")
                if self.pw is None:
                    self.pw = getpass.getpass(f"Password for {self.name}: ")
                if self.two_factor_method == "push":
                    Log.warning("Please check your phone to complete the 2FA PUSH authentication")
                self.transport = paramiko.Transport((self._host_config['hostname'], port))
                self.transport.start_client()
                try:
                    self.transport.auth_interactive(self.user, self.interactive_auth_handler)
                except Exception as e:
                    Log.printlog(f"2FA authentication failed: {str(e)}", 7000)
                    raise
                if self.transport.is_authenticated():
                    self._ssh._transport = self.transport
                    self.transport.banner_timeout = 60
                else:
                    self.transport.close()
                    raise SSHException
            self._ftpChannel = paramiko.SFTPClient.from_transport(self.transport, window_size=pow(4, 12),
                                                                  max_packet_size=pow(4, 12))
            self._ftpChannel.get_channel().settimeout(120)
            self.connected = True
            if not log_recovery_process:
                self.spawn_log_retrieval_process(as_conf)
        except SSHException:
            raise
        except IOError as e:
            if "refused" in str(e.strerror).lower():
                raise SSHException(f" {self.host} doesn't accept remote connections. "
                                   f"Check if there is an typo in the hostname")
            elif "name or service not known" in str(e.strerror).lower():
                raise SSHException(f" {self.host} doesn't accept remote connections. "
                                   f"Check if there is an typo in the hostname")
            else:
                raise AutosubmitError("File can't be located due an slow or timeout connection", 6016, str(e))
        except BaseException as e:
            self.connected = False
            if "Authentication failed." in str(e):
                raise AutosubmitCritical(f"Authentication Failed, please check the definition of PLATFORMS in YAML of "
                                         f"{self._host_config['hostname']}", 7050, str(e))
            if not reconnect and "," in self._host_config['hostname']:
                self.restore_connection(as_conf)
            else:
                raise AutosubmitError(
                    "Couldn't establish a connection to the specified host, wrong configuration?", 6003, str(e))

    def check_completed_files(self, sections=None) -> Optional[str]:
        if self.host == 'localhost':
            return None
        command = f"find {self.remote_log_dir} "
        if sections:
            for i, section in enumerate(sections.split()):
                command += f" -name {section}_COMPLETED"
                if i < len(sections.split()) - 1:
                    command += " -o "
        else:
            command += " -name *_COMPLETED"

        if self.send_command(command, True):
            return self._ssh_output
        return None

    def remove_multiple_files(self, filenames):
        log_dir = os.path.join(self.tmp_path, f'LOG_{self.expid}')
        multiple_delete_previous_run = os.path.join(
            log_dir, "multiple_delete_previous_run.sh")
        if os.path.exists(log_dir):
            lang = locale.getlocale()[1]
            if lang is None:
                lang = locale.getdefaultlocale()[1]
                if lang is None:
                    lang = 'UTF-8'
            open(multiple_delete_previous_run, 'wb+').write(("rm -f" + filenames).encode(lang))
            os.chmod(multiple_delete_previous_run, 0o770)
            self.send_file(multiple_delete_previous_run, False)
            command = os.path.join(self.get_files_path(),
                                   "multiple_delete_previous_run.sh")
            if self.send_command(command, ignore_log=True):
                return self._ssh_output
        return ""

    def send_file(self, filename, check=True) -> bool:
        if check:
            self.check_remote_log_dir()
            self.delete_file(filename)
        local_path = os.path.join(self.tmp_path, filename)
        remote_path = os.path.join(self.get_files_path(), os.path.basename(filename))
        try:
            self._ftpChannel.put(local_path, remote_path)
            self._ftpChannel.chmod(remote_path, os.stat(local_path).st_mode)
            return True
        except socket.error as e:
            raise AutosubmitError(f'Cannot send file {local_path} to {remote_path}. '
                                  f'Connection does not appear to be active: {str(e)}', 6004)
        except Exception as e:
            raise AutosubmitError(f'Cannot send file {local_path} to {remote_path}. '
                                  f'An unexpected error occurred: {str(e)}', 6004)

    def get_logs_files(self, exp_id: str, remote_logs: tuple[str, str]) -> None:
        (job_out_filename, job_err_filename) = remote_logs
        self.get_files(
            [job_out_filename, job_err_filename], False, "LOG_{0}".format(exp_id)
        )

    def get_list_of_files(self):
        return self._ftpChannel.get(self.get_files_path)
    
    def _chunked_md5(self, file_buffer: BufferedReader) -> str:
        """Calculate the MD5 checksum of a file in chunks to avoid high memory usage.

        :param file: A file-like object opened in binary mode.
        :return: The MD5 checksum as a hexadecimal string.
        """
        CHUNK_SIZE = 64 * 1024  # 64KB
        md5_hash = hashlib.md5()
        for chunk in iter(lambda: file_buffer.read(CHUNK_SIZE), b""):
            md5_hash.update(chunk)
        return md5_hash.hexdigest()
    
    def _checksum_validation(self, local_path: str, remote_path: str) -> bool:
        """Validates that the checksum of the local file matches the checksum of the remote file.

        :param local_path: Path to the local file.
        :param remote_path: Path to the remote file.
        """
        try:
            with open(local_path, "rb") as local_file:
                local_md5 = self._chunked_md5(local_file)
            with self._ftpChannel.file(remote_path, "rb") as remote_file:
                remote_md5 = self._chunked_md5(remote_file)
            return local_md5 == remote_md5
        except Exception as exc:
            Log.warning(f"Checksum validation failed: {exc}")
            return False

    # Gets .err and .out
    def get_file(self, filename, must_exist=True, relative_path='', ignore_log=False, wrapper_failed=False) -> bool:
        """Copies a file from the current platform to experiment's tmp folder

        :param wrapper_failed:
        :param ignore_log:
        :param filename: file name
        :type filename: str
        :param must_exist: If True, raises an exception if file can not be copied
        :type must_exist: bool
        :param relative_path: path inside the tmp folder
        :type relative_path: str
        :return: True if file is copied successfully, false otherwise
        :rtype: bool
        """
        local_path = os.path.join(self.tmp_path, relative_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        file_path = os.path.join(local_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
        remote_path = os.path.join(self.get_files_path(), filename)
        try:
            self._ftpChannel.get(remote_path, file_path)

            # Remove file from remote if configured and checksum matches
            is_log_file = bool(re.match(r".*\.(out|err)(\.(xz|gz))?$", filename))
            if (
                is_log_file
                and self.remove_log_files_on_transfer
                and self._checksum_validation(file_path, remote_path)
            ):
                try:
                    self._ftpChannel.remove(remote_path)
                except Exception as e:
                    Log.warning(f"Failed to remove remote file {remote_path}: {e}")

            return True
        except Exception as e:
            Log.debug(f"Could not retrieve file {filename} from platform {self.name}: {str(e)}")
            with suppress(Exception):
                os.remove(file_path)
            # FIXME: Huh, probably a bug here? See unit/test_paramiko_platform function test_get_file_errors
            if str(e) in "Garbage":
                if not ignore_log:
                    Log.printlog(f"File {filename} seems to no exists (skipping)", 5004)
            if must_exist:
                if not ignore_log:
                    Log.printlog(f"File {filename} does not exists", 6004)
            else:
                if not ignore_log:
                    Log.printlog(f"Log file couldn't be retrieved: {filename}", 5000)
        return False

    def delete_file(self, filename: str) -> bool:
        """Deletes a file from this platform

        :param filename: file name
        :type filename: str
        :return: True if successful or file does not exist
        :rtype: bool
        """
        remote_file = Path(self.get_files_path()) / filename
        try:
            self._ftpChannel.remove(str(remote_file))
            return True
        except IOError as e:
            Log.warning(f'IOError while trying to remove a remote file {str(remote_file)}: {str(e)}')
            return False
        except Exception as e:
            # Change to Path
            Log.error(f'Could not remove file {str(remote_file)}, something went wrong with the platform',
                      6004, str(e))

            if str(e).lower().find("garbage") != -1:
                raise AutosubmitCritical(
                    "Wrong User or invalid .ssh/config. Or invalid user in the definition of PLATFORMS "
                    "in YAML or public key not set ", 7051, str(e))
        return False

    def move_file(self, src, dest, must_exist=False):
        """Moves a file on the platform (includes .err and .out).

        :param src: source name
        :type src: str
        :param dest: destination name
        :param must_exist: ignore if file exist or not
        :type dest: str
        """
        path_root = ""
        try:
            path_root = self.get_files_path()
            src = os.path.join(path_root, src)
            dest = os.path.join(path_root, dest)
            try:
                self._ftpChannel.stat(dest)
            except IOError:
                self._ftpChannel.rename(src, dest)
            return True
        except IOError as e:
            if str(e) in "Garbage":
                raise AutosubmitError(f'File {os.path.join(path_root, src)} does not exists, something went '
                                      f'wrong with the platform', 6004, str(e))
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

    def submit_job(self, job, script_name, hold=False, export="none"):
        """Submit a job from a given job object.

        :param export:
        :param job: job object
        :type job: autosubmit.job.job.Job
        :param script_name: job script's name
        :rtype script_name: str
        :param hold: send job hold
        :type hold: boolean
        :return: job id for the submitted job
        :rtype: int
        """
        if job is None or not job:
            x11 = False
        else:
            x11 = job.x11

        cmd = self.get_submit_cmd(script_name, job, hold=hold, export=export)
        Log.debug(f"Submitting job with the command: {cmd}")
        if cmd is None:
            return None
        if self.send_command(cmd, x11=x11):
            x11 = False if job is None else job.x11
            job_id = self.get_submitted_job_id(self.get_ssh_output(), x11)
            if job:
                Log.result(f"Job: {job.name} submitted with job_id: {str(job_id).strip()} and workflow commit: "
                           f"{job.workflow_commit}")
            return int(job_id)
        else:
            return None

    def get_job_energy_cmd(self, job_id):
        return self.get_ssh_output()

    def check_job_energy(self, job_id):
        """Checks job energy and return values. Defined in child classes.

        :param job_id: ID of Job.
        :type job_id: int
        :return: submit time, start time, finish time, energy.
        :rtype: (int, int, int, int)
        """
        check_energy_cmd = self.get_job_energy_cmd(job_id)
        self.send_command(check_energy_cmd)
        return self.get_ssh_output()

    def submit_script(self, hold=False):
        """Sends a Submit file Script, exec in platform and retrieve the Jobs_ID.

        :param hold: send job hold
        :type hold: boolean
        :return: job id for the submitted job
        :rtype: int
        """
        raise NotImplementedError  # pragma: no cover

    def get_estimated_queue_time_cmd(self, job_id):
        """Returns command to get estimated queue time on remote platforms

        :param job_id: id of job to check
        :param job_id: str
        :return: command to get estimated queue time
        """
        raise NotImplementedError  # pragma: no cover

    def parse_estimated_time(self, output):
        """Parses estimated queue time from output of get_estimated_queue_time_cmd

        :param output: output of get_estimated_queue_time_cmd
        :type output: str
        :return: estimated queue time
        :rtype:
        """
        raise NotImplementedError  # pragma: no cover

    def job_is_over_wallclock(self, job, job_status, cancel=False):
        if job.is_over_wallclock():
            try:
                job.platform.get_completed_files(job.name)
                job_status = job.check_completion(over_wallclock=True)
            except Exception as e:
                job_status = Status.FAILED
                Log.debug(f"Unexpected error checking completed files for a job over wallclock: {str(e)}")

            if cancel and job_status is Status.FAILED:
                try:
                    if self.cancel_cmd is not None:
                        Log.warning(f"Job {job.id} is over wallclock, cancelling job")
                        job.platform.send_command(self.cancel_cmd + " " + str(job.id))
                except Exception as e:
                    Log.debug(f"Error cancelling job {job.id}: {str(e)}")
        return job_status

    def check_job(self, job, default_status=Status.COMPLETED, retries=5, submit_hold_check=False, is_wrapper=False):
        """Checks job running status

        :param is_wrapper:
        :param submit_hold_check:
        :param retries: retries
        :param job: job
        :type job: autosubmit.job.job.Job
        :param default_status: default status if job is not found
        :type job: class(job)
        :param default_status: status to assign if it can be retrieved from the platform
        :type default_status: autosubmit.job.job_common.Status
        :return: current job status
        :rtype: autosubmit.job.job_common.Status

        """
        for event in job.platform.worker_events:  # keep alive log retrieval workers.
            if not event.is_set():
                event.set()
        job_id = job.id
        job_status = Status.UNKNOWN
        if type(job_id) is not int and type(job_id) is not str:
            Log.error(
                f'check_job() The job id ({job_id}) is not an integer neither a string.')
            job.new_status = job_status
        sleep_time = 5
        sleep(2)
        self.send_command(self.get_check_job_cmd(job_id))
        while self.get_ssh_output().strip(" ") == "" and retries > 0:
            retries = retries - 1
            Log.debug(f'Retrying check job command: {self.get_check_job_cmd(job_id)}')
            Log.debug(f'retries left {retries}')
            Log.debug(f'Will be retrying in {sleep_time} seconds')
            sleep(sleep_time)
            sleep_time = sleep_time + 5
            self.send_command(self.get_check_job_cmd(job_id))
        if retries >= 0:
            Log.debug(f'Successful check job command: {self.get_check_job_cmd(job_id)}')
            job_status = self.parse_job_output(
                self.get_ssh_output()).strip("\n")
            # URi: define status list in HPC Queue Class
            if job_status in self.job_status['COMPLETED'] or retries == 0:
                # The Local platform has only 0 or 1, so it necessary to look for the completed file.
                if self.type == "local":
                    if not job.is_wrapper:
                        # Not sure why it is called over_wallclock but is the only way to return a value
                        job_status = job.check_completion(over_wallclock=True)
                    else:
                        # wrapper has a different file name
                        if Path(f"{self.remote_log_dir}/WRAPPER_FAILED").exists():
                            job_status = Status.FAILED
                        else:
                            job_status = Status.COMPLETED
                else:
                    job_status = Status.COMPLETED

            elif job_status in self.job_status['RUNNING']:
                job_status = Status.RUNNING
                if not is_wrapper:
                    if job.status != Status.RUNNING:
                        job.start_time = datetime.datetime.now()  # URi: start time
                    if job.start_time is not None and str(job.wrapper_type).lower() == "none":
                        wallclock = job.wallclock
                        if job.wallclock == "00:00" or job.wallclock is None:
                            wallclock = job.platform.max_wallclock
                        if wallclock != "00:00" and wallclock != "00:00:00" and wallclock != "":
                            job_status = self.job_is_over_wallclock(job, job_status, cancel=False)
            elif job_status in self.job_status['QUEUING'] and (not job.hold or job.hold.lower() != "true"):
                job_status = Status.QUEUING
            elif job_status in self.job_status['QUEUING'] and (job.hold or job.hold.lower() == "true"):
                job_status = Status.HELD
            elif job_status in self.job_status['FAILED']:
                job_status = Status.FAILED
            else:
                job_status = Status.UNKNOWN
        else:
            Log.error(
                f" check_job(), job is not on the queue system. Output was: {self.get_check_job_cmd(job_id)}" )
            job_status = Status.UNKNOWN
            Log.error(
                f'check_job() The job id ({job_id}) status is {job_status}.')

        if job_status in [Status.FAILED, Status.COMPLETED, Status.UNKNOWN]:
            job.updated_log = False
            if not job.start_time_timestamp:  # QUEUING -> COMPLETED ( under safetytime )
                job.start_time_timestamp = int(time.time())
            # Estimate Time for failed jobs, as they won't have the timestamp in the stat file
            job.finish_time_timestamp = int(time.time())
        if job_status in [Status.RUNNING, Status.COMPLETED] and job.new_status in [Status.QUEUING, Status.SUBMITTED]:
            # backup for start time in case that the stat file is not found
            job.start_time_timestamp = int(time.time())

        if submit_hold_check:
            return job_status
        else:
            job.new_status = job_status

    def _check_jobid_in_queue(self, ssh_output, job_list_cmd):
        """

        :param ssh_output: ssh output
        :type ssh_output: str
        """
        for job in job_list_cmd[:-1].split(','):
            if job not in ssh_output:
                return False
        return True

    def parse_job_list(self, job_list: list[list['Job']]) -> str:
        """Convert a list of job_list to job_list_cmd

        If a job in the provided list is missing its ID, this function will initialize
        it to a string containing the digit zero,``"0"``.

        :param job_list: A list of jobs.
        :return: A comma-separated string containing the job IDs.
        """
        job_list_cmd: list[str] = []
        # TODO: second item in tuple, _, is a ``job_prev_status``? What for?
        for job, _ in job_list:
            if job.id is None:
                job_str = "0"
            else:
                job_str = str(job.id)
            job_list_cmd.append(job_str)

        return ','.join(job_list_cmd)

    def check_all_jobs(self, job_list: list[list['Job']], as_conf, retries=5):
        """Checks jobs running status

        :param job_list: list of jobs
        :type job_list: list
        :param as_conf: config
        :type as_conf: as_conf
        :param retries: retries
        :type retries: int
        :return: current job status
        :rtype: autosubmit.job.job_common.Status
        """
        as_conf.get_copy_remote_logs()
        job_list_cmd = self.parse_job_list(job_list)
        cmd = self.get_check_all_jobs_cmd(job_list_cmd)
        sleep_time = 5
        sleep(sleep_time)
        slurm_error = False
        e_msg = ""
        try:
            self.send_command(cmd)
        except AutosubmitError as e:
            e_msg = e.error_message
            slurm_error = True
        if not slurm_error:
            while not self._check_jobid_in_queue(self.get_ssh_output(), job_list_cmd) and retries > 0:
                try:
                    self.send_command(cmd)
                except AutosubmitError as e:
                    e_msg = e.error_message
                    slurm_error = True
                    break
                Log.debug(f'Retrying check job command: {cmd}')
                Log.debug(f'retries left {retries}')
                Log.debug(f'Will be retrying in {sleep_time} seconds')
                retries -= 1
                sleep(sleep_time)
                sleep_time = sleep_time + 5

        job_list_status = self.get_ssh_output()
        if retries >= 0:
            Log.debug('Successful check job command')
            in_queue_jobs = []
            list_queue_jobid = ""
            for job, job_prev_status in job_list:
                if not slurm_error:
                    job_id = job.id
                    job_status = self.parse_all_jobs_output(job_list_status, job_id)
                    while len(job_status) <= 0 <= retries:
                        retries -= 1
                        self.send_command(cmd)
                        job_list_status = self.get_ssh_output()
                        job_status = self.parse_all_jobs_output(job_list_status, job_id)
                        if len(job_status) <= 0:
                            Log.debug(f'Retrying check job command: {cmd}')
                            Log.debug(f'retries left {retries}')
                            Log.debug(f'Will be retrying in {sleep_time} seconds')
                            sleep(sleep_time)
                            sleep_time = sleep_time + 5
                    # URi: define status list in HPC Queue Class
                else:
                    job_status = job.status
                if job.status != Status.RUNNING:
                    job.start_time = datetime.datetime.now()  # URi: start time
                if job.start_time is not None and str(job.wrapper_type).lower() == "none":
                    wallclock = job.wallclock
                    if job.wallclock == "00:00":
                        wallclock = job.platform.max_wallclock
                    if wallclock != "00:00" and wallclock != "00:00:00" and wallclock != "":
                        job_status = self.job_is_over_wallclock(job, job_status, cancel=True)
                if job_status in self.job_status['COMPLETED']:
                    job_status = Status.COMPLETED
                elif job_status in self.job_status['RUNNING']:
                    job_status = Status.RUNNING
                elif job_status in self.job_status['QUEUING']:
                    if job.hold:
                        job_status = Status.HELD  # release?
                    else:
                        job_status = Status.QUEUING
                    list_queue_jobid += str(job.id) + ','
                    in_queue_jobs.append(job)
                elif job_status in self.job_status['FAILED']:
                    job_status = Status.FAILED
                elif retries == 0:
                    job_status = Status.COMPLETED
                    job.update_status(as_conf)
                else:
                    job_status = Status.UNKNOWN
                    Log.error(
                        f'check_job() The job id ({job.id}) status is {job_status}.')
                job.new_status = job_status
            self.get_queue_status(in_queue_jobs, list_queue_jobid, as_conf)
        else:
            for job, job_prev_status in job_list:
                job_status = Status.UNKNOWN
                Log.warning(f'check_job() The job id ({job.id}) from platform {self.name} has '
                            f'an status of {job_status}.')
            raise AutosubmitError("Some Jobs are in Unknown status", 6008)
            # job.new_status=job_status
        if slurm_error:
            raise AutosubmitError(e_msg, 6000)

    def get_jobid_by_jobname(self, job_name, retries=2):
        """Get job id by job name

        :param job_name:
        :param retries: retries
        :type retries: int
        :return: job id
        """
        job_ids = ""
        cmd = self.get_jobid_by_jobname_cmd(job_name)
        self.send_command(cmd)
        job_id_name = self.get_ssh_output()
        while len(job_id_name) <= 0 < retries:
            self.send_command(cmd)
            job_id_name = self.get_ssh_output()
            retries -= 1
            sleep(2)
        if retries >= 0:
            # get id last line
            job_ids_names = job_id_name.split('\n')[1:-1]
            # get all ids by job-name
            job_ids = [job_id.split(',')[0] for job_id in job_ids_names]
        return job_ids

    def get_queue_status(self, in_queue_jobs, list_queue_jobid, as_conf):
        """Get queue status for a list of jobs.

        The job statuses are normally found via a command sent to the remote platform.

        Each ``job`` in ``in_queue_jobs`` must be updated. Implementations may check
        for the reason for queueing cancellation, or if the job is held, and update
        the ``job`` status appropriately.
        """
        raise NotImplementedError  # pragma: no cover

    def get_check_job_cmd(self, job_id: str) -> str:
        """Returns command to check job status on remote platforms.

        :param job_id: id of job to check
        :return: command to check job status
        """
        raise NotImplementedError  # pragma: no cover

    def get_check_all_jobs_cmd(self, jobs_id: str):
        """Returns command to check jobs status on remote platforms.

        :param jobs_id: id of jobs to check
        :param jobs_id: str
        :return: command to check job status
        :rtype: str
        """
        raise NotImplementedError  # pragma: no cover

    def get_jobid_by_jobname_cmd(self, job_name):
        """Returns command to get job id by job name on remote platforms

        :param job_name:
        :return: str
        """
        return NotImplementedError  # pragma: no cover

    def get_queue_status_cmd(self, job_name):
        """Returns command to get queue status on remote platforms

        :return: str
        """
        return NotImplementedError  # pragma: no cover

    def x11_handler(self, channel, xxx_todo_changeme):
        """Handler for incoming x11 connections.

        For each x11 incoming connection:

        - get a connection to the local display
        - maintain bidirectional map of remote x11 channel to local x11 channel
        - add the descriptors to the poller
        - queue the channel (use transport.accept())

        Incoming connections come from the server when we open an actual GUI application.
        """
        (_, _) = xxx_todo_changeme  # TODO: addr, port, but never used?
        x11_chanfd = channel.fileno()
        local_x11_socket = xlib_connect.get_socket(*self.local_x11_display[:4])
        local_x11_socket_fileno = local_x11_socket.fileno()
        self.channels[x11_chanfd] = channel, local_x11_socket
        self.channels[local_x11_socket_fileno] = local_x11_socket, channel
        self.poller.register(x11_chanfd, select.POLLIN)
        self.poller.register(local_x11_socket, select.POLLIN)
        self.transport._queue_incoming_channel(channel)

    def flush_out(self, session):
        while session.recv_ready():
            sys.stdout.write(session.recv(4096).decode(locale.getlocale()[1]))
        while session.recv_stderr_ready():
            sys.stderr.write(session.recv_stderr(4096).decode(locale.getlocale()[1]))

    @threaded
    def x11_status_checker(self, session, session_fileno):
        poller = None
        self.transport.accept()
        while not session.exit_status_ready():
            with suppress(Exception):
                if type(self.poller) is not list:
                    if sys.platform != "linux":
                        poller = self.poller.kqueue()
                    else:
                        poller = self.poller.poll()
                # accept subsequent x11 connections if any
                if len(self.transport.server_accepts) > 0:
                    self.transport.accept()
                if not poller:  # this should not happen, as we don't have a timeout.
                    break
                for fd, event in poller:
                    if fd == session_fileno:
                        self.flush_out(session)
                    # data either on local/remote x11 socket
                    if fd in list(self.channels.keys()):
                        channel, counterpart = self.channels[fd]
                        try:
                            # forward data between local/remote x11 socket.
                            data = channel.recv(4096)
                            counterpart.sendall(data)
                        except socket.error:
                            channel.close()
                            counterpart.close()
                            del self.channels[fd]

    def exec_command(
            self, command, bufsize=-1, timeout=30, get_pty=False, retries=3, x11=False
    ) -> Union[tuple[paramiko.Channel, paramiko.Channel, paramiko.Channel], tuple[bool, bool, bool]]:
        """Execute a command on the SSH server.  A new `.Channel` is opened and
        the requested command is executed.  The command's input and output
        streams are returned as Python ``file``-like objects representing
        stdin, stdout, and stderr.

        :param x11:
        :param retries:
        :param command: the command to execute.
        :type command: str
        :param bufsize: interpreted the same way as by the built-in ``file()`` function in Python.
        :type bufsize: int
        :param timeout: set command's channel timeout. See ``Channel.settimeout``.
        :type timeout: int
        :return: the stdin, stdout, and stderr of the executing command
        :raises SSHException: if the server fails to execute the command
        """
        while retries > 0:
            try:
                if x11:
                    self._init_local_x11_display()
                    chan = self.transport.open_session()
                    if not chan.request_x11(single_connection=False, handler=self.x11_handler):
                        # FIXME: Apparently, the SSH session closes before reaching here?
                        #        If so, we can just remove this check?
                        raise AutosubmitCritical("Remote platform does not support X11!")
                else:
                    chan = self.transport.open_session()
                if x11:
                    if "timeout" in command:
                        timeout_command = command.split("timeout ")[1].split(" ")[0]
                        if timeout_command == 0:
                            timeout_command = "infinity"
                        command = f'{command} ; sleep {timeout_command} 2>/dev/null'
                    Log.info(command)
                    # TODO: With X11, we catch any exception and raise AutosubmitCritical, but not without X11?
                    #       In that case we catch only paramiko.SSHException, and let the rest bubble up?
                    try:
                        chan.exec_command(command)
                    except Exception as e:
                        raise AutosubmitCritical(f"Failed to execute command '{command}': {e}")
                    chan_fileno = chan.fileno()
                    self.poller.register(chan_fileno, select.POLLIN)
                    self.x11_status_checker(chan, chan_fileno)
                else:
                    chan.exec_command(command)
                stdin = chan.makefile('wb', bufsize)
                stdout = chan.makefile('rb', bufsize)
                stderr = chan.makefile_stderr('rb', bufsize)
                return stdin, stdout, stderr
            except paramiko.SSHException as e:
                if str(e) in "SSH session not active":
                    self._ssh = None
                    self.restore_connection(None)
                timeout = timeout + 60
                retries = retries - 1
        if retries <= 0:
            return False, False, False

    def send_command_non_blocking(self, command, ignore_log):
        thread = threading.Thread(target=self.send_command, args=(command, ignore_log))
        thread.start()
        return thread

    def send_command(self, command, ignore_log=False, x11=False) -> bool:
        """Sends given command to HPC

        :param x11:
        :param ignore_log:
        :param command: command to send
        :type command: str
        :return: True if executed, False if failed
        :rtype: bool
        """
        lang = locale.getlocale()[1] or locale.getdefaultlocale()[1] or 'UTF-8'
        if "rsync" in command or "find" in command or "convertLink" in command:
            timeout = None  # infinite timeout on migrate command
        elif "rm" in command:
            timeout = 60
        else:
            timeout = 60 * 2
        if not ignore_log:
            Log.debug(f"send_command timeout used: {timeout} seconds (None = infinity)")
        stderr_readlines = []
        stdout_chunks = []

        try:
            stdin, stdout, stderr = self.exec_command(command, x11=x11)
            channel = stdout.channel
            if not x11:
                channel.settimeout(timeout)
                stdin.close()
                channel.shutdown_write()
                stdout_chunks.append(stdout.channel.recv(len(stdout.channel.in_buffer)))

            # In X11, apparently we may get multiple errors related to X client and server communication,
            # not directly related to a job. So, we accumulate all the errors in the ``aux_stderr``, and
            # look for errors related to a platform like Slurm (at the moment ignores PBS/PS/etc.). When
            # we find platform errors, like those that contain a ``job_id`` in the log, then we will use
            # this to copy this output from err to out (do not ask me why...).
            aux_stderr = []
            x11_exit = False

            while (not channel.closed or channel.recv_ready() or channel.recv_stderr_ready()) and not x11_exit:
                # stop if the channel was closed prematurely, and there is no data in the buffers.
                got_chunk = False
                readq, _, _ = select.select([stdout.channel], [], [], 2)
                for c in readq:
                    if c.recv_ready():
                        stdout_chunks.append(
                            stdout.channel.recv(len(c.in_buffer)))
                        got_chunk = True
                    if c.recv_stderr_ready():
                        # make sure to read stderr to prevent stall
                        stderr_readlines.append(stderr.channel.recv_stderr(len(c.in_stderr_buffer)))
                        got_chunk = True
                if x11:
                    if len(stderr_readlines) > 0:
                        aux_stderr.extend(stderr_readlines)
                        for stderr_line in stderr_readlines:
                            stderr_line = stderr_line.decode(lang)
                            # ``salloc`` is the command to allocate resources in Slurm, for PJM it is different.
                            if "salloc" in stderr_line:
                                job_id = re.findall(r'\d+', stderr_line)
                                if job_id:
                                    stdout_chunks.append(job_id[0].encode(lang))
                                    x11_exit = True
                    else:
                        x11_exit = True
                    if not x11_exit:
                        stderr_readlines = []
                    else:
                        stderr_readlines = aux_stderr
                must_close_channels = (
                        stdout.channel.exit_status_ready() and
                        not stderr.channel.recv_stderr_ready() and
                        not stdout.channel.recv_ready()
                )
                if not got_chunk and must_close_channels:
                    # indicate that we're not going to read from this channel anymore
                    stdout.channel.shutdown_read()
                    # close the channel
                    stdout.channel.close()
                    break
            # check if we have X11 errors
            if x11:
                if len(aux_stderr) > 0:
                    stderr_readlines = aux_stderr
            else:
                # close all the pseudo files
                stdout.close()
                stderr.close()

            self._ssh_output = ""
            self._ssh_output_err = ""
            for s in stdout_chunks:
                if s.decode(lang) != '':
                    self._ssh_output += s.decode(lang)
            for error_line_case in stderr_readlines:
                self._ssh_output_err += error_line_case.decode(lang)
                error_line = error_line_case.lower().decode(lang)
                # TODO: To be simplified in the future in a function and using in.
                #        The errors should be inside the class of the platform not here
                if "not active" in error_line:
                    raise AutosubmitError(
                        'SSH Session not active, will restart the platforms', 6005)
                if error_line.find("command not found") != -1:
                    raise AutosubmitError(
                        f"A platform command was not found. This may be a temporary issue. "
                        f"Please verify that the correct scheduler is specified for this platform: "
                        f"'{self.name}.{self.type}'.",
                        7052,
                        self._ssh_output_err
                    )
                elif error_line.find("syntax error") != -1:
                    raise AutosubmitCritical("Syntax error", 7052, self._ssh_output_err)
                elif (
                        error_line.find("refused") != -1
                        or error_line.find("slurm_persist_conn_open_without_init") != -1
                        or error_line.find("slurmdbd") != -1
                        or error_line.find("submission failed") != -1
                        or error_line.find("git clone") != -1
                        or error_line.find("sbatch: error: ") != -1
                        or error_line.find("not submitted") != -1
                        or error_line.find("invalid") != -1
                        or "[ERR.] PJM".lower() in error_line
                ):
                    # TODO: if conditions above and below could be simplified?
                    if (
                            "salloc: error" in error_line
                            or "salloc: unrecognized option" in error_line
                            or "[ERR.] PJM".lower() in error_line
                            or (
                            self._submit_command_name == "sbatch"
                            and (
                                    error_line.find("policy") != -1
                                    or error_line.find("invalid") != -1)
                    )
                            or (
                            self._submit_command_name == "sbatch"
                            and error_line.find("argument") != -1
                    )
                            or (
                            self._submit_command_name == "bsub"
                            and error_line.find("job not submitted") != -1
                    )
                            or self._submit_command_name == "ecaccess-job-submit"
                            or self._submit_command_name == "qsub "
                    ):
                        raise AutosubmitError(error_line, 7014, "Bad Parameters.")
                    raise AutosubmitError(f'Command {command} in {self.host} warning: {self._ssh_output_err}', 6005)
            if not ignore_log:
                if len(stderr_readlines) > 0:
                    Log.printlog(f'Command {command} in {self.host} warning: {self._ssh_output_err}', 6006)
                else:
                    pass
            return True
        except AttributeError as e:
            raise AutosubmitError(f'Session not active: {str(e)}', 6005)
        except AutosubmitCritical:
            raise
        except AutosubmitError:
            raise
        except IOError as e:
            raise AutosubmitError(str(e), 6016)
        except BaseException as e:
            if type(stderr_readlines) is str:
                stderr_readlines = '\n'.join(stderr_readlines)
            raise AutosubmitError(f'Command {command} in {self.host} warning: {stderr_readlines}', 6005, str(e))

    def parse_job_output(self, output):
        """Parses check job command output, so it can be interpreted by autosubmit

        :param output: output to parse
        :type output: str
        :return: job status
        :rtype: str
        """
        raise NotImplementedError  # pragma: no cover

    def parse_all_jobs_output(self, output, job_id):
        """Parses check jobs command output, so it can be interpreted by autosubmit

        :param output: output to parse
        :param job_id: select the job to parse
        :type output: str
        :return: job status
        :rtype: str
        """
        raise NotImplementedError  # pragma: no cover

    def generate_submit_script(self):
        pass  # pragma: no cover

    def get_submit_script(self):
        pass  # pragma: no cover

    def get_submit_cmd(self, job_script: str, job, hold: bool = False, export: str = "") -> str:
        """Get command to add job to scheduler

        :param job:
        :param job_script: path to job script
        :param job_script: str
        :param hold: submit a job in a held status
        :param hold: boolean
        :param export: modules that should've downloaded
        :param export: string
        :return: command to submit job to platforms
        :rtype: str
        """
        raise NotImplementedError  # pragma: no cover

    def get_mkdir_cmd(self):
        """Gets command to create directories on HPC

        :return: command to create directories on HPC
        :rtype: str
        """
        raise NotImplementedError  # pragma: no cover

    def parse_queue_reason(self, output, job_id):
        raise NotImplementedError  # pragma: no cover

    def get_ssh_output(self):
        """Gets output from last command executed.

        :return: output from last command
        :rtype: str
        """
        if self._ssh_output is None or not self._ssh_output:
            self._ssh_output = ""
        return self._ssh_output

    def get_ssh_output_err(self):
        return self._ssh_output_err

    def get_call(self, job_script: str, job: Optional['Job'], export="none", timeout=-1) -> str:
        """Gets execution command for given job.

        :param job_script: script to run
        :param job: job
        :param export:
        :param timeout:
        :return: command to execute script
        """
        # If job is None, it is a wrapper. (TODO: 0 clarity there, to be improved in a rework)
        if job:
            if job.executable != '':
                executable = ''  # Alternative: use job.executable with substituted placeholders
            else:
                executable = Language.get_executable(job.type)
            remote_logs = (job.script_name + ".out." + str(job.fail_count),
                           job.script_name + ".err." + str(job.fail_count))
        else:
            executable = Language.get_executable(Language.EMPTY)  # wrappers are always python3
            remote_logs = (f"{job_script}.out", f"{job_script}.err")

        if timeout < 1:
            command = export + ' nohup ' + executable + (f' {os.path.join(self.remote_log_dir, job_script)} > '
                                                         f'{os.path.join(self.remote_log_dir, remote_logs[0])} 2> '
                                                         f'{os.path.join(self.remote_log_dir, remote_logs[1])} & '
                                                         f'echo $!')
        else:
            command = (export + f"timeout {timeout}" + ' nohup ' + executable +
                       f' {os.path.join(self.remote_log_dir, job_script)} > '
                       f'{os.path.join(self.remote_log_dir, remote_logs[0])} 2> '
                       f'{os.path.join(self.remote_log_dir, remote_logs[1])} & echo $!')
        return command

    @staticmethod
    def get_pscall(job_id):
        """Gets command to check if a job is running given process identifier

        :param job_id: process identifier
        :type job_id: int
        :return: command to check job status script
        :rtype: str
        """
        return f'nohup kill -0 {job_id} > /dev/null 2>&1; echo $?'

    def get_submitted_job_id(self, output: str, x11: bool = False) -> Union[list[int], int]:
        """Parses submit command output to extract job id.

        :param x11:
        :param output: output to parse
        :type output: str
        :return: job id
        :rtype: str
        """
        raise NotImplementedError  # pragma: no cover

    def get_header(self, job: 'Job', parameters: dict) -> str:
        """Gets the header to be used by the job.

        :param job: The job.
        :param parameters: Parameters dictionary.
        :return: Job header.
        """
        if not job.packed or str(job.wrapper_type).lower() != "vertical":
            out_filename = f"{job.name}.cmd.out.{job.fail_count}"
            err_filename = f"{job.name}.cmd.err.{job.fail_count}"
        else:
            out_filename = f"{job.name}.cmd.out"
            err_filename = f"{job.name}.cmd.err"

        if len(job.het) > 0:
            header = self.header.calculate_het_header(job, parameters)
        elif str(job.processors) == '1':
            header = self.header.SERIAL
        else:
            header = self.header.PARALLEL

        header = header.replace('%OUT_LOG_DIRECTIVE%', out_filename)
        header = header.replace('%ERR_LOG_DIRECTIVE%', err_filename)
        if job.het.get("HETSIZE", 0) <= 1:
            if hasattr(self.header, 'get_queue_directive'):
                header = header.replace(
                    '%QUEUE_DIRECTIVE%', self.header.get_queue_directive(job, parameters))
            if hasattr(self.header, 'get_processors_directive'):
                header = header.replace(
                    '%NUMPROC_DIRECTIVE%', self.header.get_processors_directive(job, parameters))
            if hasattr(self.header, 'get_partition_directive'):
                header = header.replace(
                    '%PARTITION_DIRECTIVE%', self.header.get_partition_directive(job, parameters))
            if hasattr(self.header, 'get_tasks_per_node'):
                header = header.replace(
                    '%TASKS_PER_NODE_DIRECTIVE%', self.header.get_tasks_per_node(job, parameters))
            if hasattr(self.header, 'get_threads_per_task'):
                header = header.replace(
                    '%THREADS_PER_TASK_DIRECTIVE%', self.header.get_threads_per_task(job, parameters))
            if job.x11:
                header = header.replace(
                    '%X11%', "SBATCH --x11=batch")
            else:
                header = header.replace(
                    '%X11%', "")
            if hasattr(self.header, 'get_scratch_free_space'):
                header = header.replace(
                    '%SCRATCH_FREE_SPACE_DIRECTIVE%', self.header.get_scratch_free_space(job, parameters))
            if hasattr(self.header, 'get_custom_directives'):
                header = header.replace(
                    '%CUSTOM_DIRECTIVES%', self.header.get_custom_directives(job, parameters))
            if hasattr(self.header, 'get_exclusive_directive'):
                header = header.replace(
                    '%EXCLUSIVE_DIRECTIVE%', self.header.get_exclusive_directive(job, parameters))
            if hasattr(self.header, 'get_account_directive'):
                header = header.replace(
                    '%ACCOUNT_DIRECTIVE%', self.header.get_account_directive(job, parameters))
            if hasattr(self.header, 'get_shape_directive'):
                header = header.replace(
                    '%SHAPE_DIRECTIVE%', self.header.get_shape_directive(job, parameters))
            if hasattr(self.header, 'get_nodes_directive'):
                header = header.replace(
                    '%NODES_DIRECTIVE%', self.header.get_nodes_directive(job, parameters))
            if hasattr(self.header, 'get_reservation_directive'):
                header = header.replace(
                    '%RESERVATION_DIRECTIVE%', self.header.get_reservation_directive(job, parameters))
            if hasattr(self.header, 'get_memory_directive'):
                header = header.replace(
                    '%MEMORY_DIRECTIVE%', self.header.get_memory_directive(job, parameters))
            if hasattr(self.header, 'get_memory_per_task_directive'):
                header = header.replace(
                    '%MEMORY_PER_TASK_DIRECTIVE%', self.header.get_memory_per_task_directive(job, parameters))
            if hasattr(self.header, 'get_hyperthreading_directive'):
                header = header.replace(
                    '%HYPERTHREADING_DIRECTIVE%', self.header.get_hyperthreading_directive(job, parameters))
        return header

    # noinspection PyProtectedMember
    def close_connection(self):
        # Ensure to delete all references to the ssh connection, so that it frees all the file descriptors
        with suppress(Exception):
            if self._ftpChannel:
                self._ftpChannel.close()
        with suppress(Exception):
            if self._ssh._agent:  # May not be in all runs
                self._ssh._agent.close()
        with suppress(Exception):
            if self._ssh._transport:
                self._ssh._transport.close()
                self._ssh._transport.stop_thread()
        with suppress(Exception):
            if self._ssh:
                self._ssh.close()
        with suppress(Exception):
            if self.transport:
                self.transport.close()
                self.transport.stop_thread()

    def check_remote_permissions(self) -> bool:
        """Check remote permissions on a platform.

        This is needed for Paramiko and PS and other platforms.

        It uses the platform scratch project directory to create a subdirectory, and then
        removes it. It does it that way to verify that the user running Autosubmit has the
        minimum permissions required to run Autosubmit.

        It does not check Slurm, queues, modules, software, etc., only the file system
        permissions required.

        :return: ``True`` on success, ``False`` otherwise.
        """
        try:
            path = os.path.join(self.scratch, self.project_dir, self.user, "permission_checker_azxbyc")
            try:
                self._ftpChannel.mkdir(path)
                self._ftpChannel.rmdir(path)
            except IOError as e:
                Log.warning(f'Failed checking remote permissions (1): {str(e)}')
                # TODO: Writing the test, it become confusing as to why we are removing,
                #       then trying again -- if it failed on the first try, we cannot really
                #       assume mkdir or rmdir failed, but yes that there is an I/O problem,
                #       then maybe try again ``mkdir -p path``; or if we cannot do it because
                #       it's SFTP, then maybe break down the operations and capture which one
                #       failed.... or try something else? Quite hard to test this, we will not
                #       cover everything unless we mock (which could hide that this needs to
                #       be reviewed...).
                self._ftpChannel.rmdir(path)
                self._ftpChannel.mkdir(path)
                self._ftpChannel.rmdir(path)
            return True
        except Exception as e:
            Log.warning(f'Failed checking remote permissions (2): {str(e)}')
        return False

    def check_remote_log_dir(self):
        """Creates log dir on remote host. """
        try:
            if self.send_command(self.get_mkdir_cmd()):
                Log.debug(f'{self.remote_log_dir} has been created on {self.host} .')
            else:
                Log.debug(f'Could not create the DIR {self.remote_log_dir} to HPC {self.host}')
        except BaseException as e:
            raise AutosubmitError(f"Couldn't send the file {self.remote_log_dir} to HPC {self.host}", 6004, str(e))

    def check_absolute_file_exists(self, src) -> bool:
        with suppress(Exception):
            return self._ftpChannel.stat(src)
        return False

    def get_file_size(self, src: str) -> Union[int, None]:
        """Get file size in bytes
        :param src: file path
        """
        try:
            return self._ftpChannel.stat(str(src)).st_size
        except Exception as e:
            Log.debug(f"Error getting file size for {src}: {str(e)}")
            return None

    def read_file(self, src: str, max_size: int = None) -> Union[bytes, None]:
        """Read file content as bytes. If max_size is set, only the first max_size bytes are read.

        :param src: file path
        :param max_size: maximum size to read
        """
        try:
            with self._ftpChannel.file(str(src), "r") as file:
                return file.read(size=max_size)
        except Exception as e:
            Log.debug(f"Error reading file {src}: {str(e)}")
            return None
        
    def compress_file(self, file_path):
        Log.debug(f"Compressing file {file_path} using {self.remote_logs_compress_type}")
        try:
            if self.remote_logs_compress_type == "xz":
                output = file_path + ".xz"
                compression_level = self.compression_level
                self.send_command(f"xz -{compression_level} -e -c {file_path} > {output}", ignore_log=True)
            else:
                output = file_path + ".gz"
                compression_level = self.compression_level
                self.send_command(f"gzip -{compression_level} -c {file_path} > {output}", ignore_log=True)

            # Validate and remove the input file if compression succeeded
            if self.check_absolute_file_exists(output):
                self.delete_file(file_path)

                Log.debug(f"File {file_path} compressed successfully to {output}")
                return output
            else:
                Log.error(f"Compression failed for file {file_path}")
        except Exception as exc:
            Log.error(f"Error compressing file {file_path}: {exc}")

        return None

    def _init_local_x11_display(self) -> None:
        """Initialize the X11 display on this platform. """
        display = os.getenv('DISPLAY', 'localhost:0')
        try:
            self.local_x11_display = xlib_connect.get_display(display)
        except Exception as e:
            Log.warning(f"X11 display not found: {e}")
            self.local_x11_display = None

    def _init_poller(self):
        """Initialize the platform file descriptor poller. """
        if sys.platform != "linux":
            self.poller = select.kqueue()
        else:
            self.poller = select.poll()

    def update_cmds(self):
        """Updates commands for this platform. """
        pass  # pragma: no cover


class ParamikoPlatformException(Exception):
    """Exception raised from HPC queues."""

    def __init__(self, msg):
        self.message = msg
