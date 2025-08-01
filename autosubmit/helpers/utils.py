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

import os
import pwd
import re
from contextlib import suppress
from itertools import zip_longest
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig


def check_jobs_file_exists(as_conf: 'AutosubmitConfig', current_section_name: Optional[str] = None):
    """Raise an error if the jobs file does not exist.

    By default, it will search all jobs sections. Alternatively, callers can pass
    ``current_section_name`` to limit the section that is checked.

    :raise: AutosubmitCritical if the templates directory is a file or does not exist,
            or if the job file (templates) cannot be found.
    """
    if as_conf.get_project_type() != 'none':
        templates_dir = Path(as_conf.get_project_dir())

        if not templates_dir.exists():
            raise AutosubmitCritical(f"Templates directory {templates_dir} does not exist", 7011)

        if not templates_dir.is_dir():
            raise AutosubmitCritical(f"Templates directory {templates_dir} is not a directory", 7011)

        # Check if all files in jobs_data exist or only current section
        if current_section_name:
            jobs_data = [as_conf.jobs_data.get(current_section_name, {})]
        else:
            jobs_data = as_conf.jobs_data.values()

        # List of files that doesn't exist.
        missing_files: list[str] = []

        for data in jobs_data:
            if "SCRIPT" not in data and "FILE" in data:
                job_file = Path(templates_dir, data['FILE'])
                if job_file.exists() and job_file.is_file():
                    Log.result(f"File {job_file} exists")
                else:
                    missing_files.append(str(job_file))

        if missing_files:
            missing_files_text = ' \n'.join(missing_files)
            raise AutosubmitCritical(f"Templates not found:\n{missing_files_text}", 7011)


def check_experiment_ownership(
        expid: str, basic_config: BasicConfig, raise_error=False, logger: Log = None
) -> tuple[bool, bool, str]:
    # [A-Za-z09]+ variable is not needed, LOG is global thus it will be read if available
    my_user_id = os.getuid()
    current_owner_id = 0
    current_owner_name = "NA"
    try:
        current_owner_id = os.stat(os.path.join(basic_config.LOCAL_ROOT_DIR, expid)).st_uid
        current_owner_name = pwd.getpwuid(os.stat(os.path.join(basic_config.LOCAL_ROOT_DIR, expid)).st_uid).pw_name
    except Exception as e:
        if logger:
            logger.info(f"Error while trying to get the experiment's owner information: {str(e)}")
    finally:
        if current_owner_id <= 0 and logger:
            logger.info(f"Current owner '{current_owner_name}' of experiment {expid} does not exist anymore.")
    is_owner = current_owner_id == my_user_id
    # If eadmin no exists, it would be "" so INT() would fail.
    eadmin_user = os.popen('id -u eadmin').read().strip()
    if eadmin_user != "":
        is_eadmin = my_user_id == int(eadmin_user)
    else:
        is_eadmin = False
    if not is_owner and raise_error:
        raise AutosubmitCritical("You don't own the experiment {0}.".format(expid), 7012)
    return is_owner, is_eadmin, current_owner_name


def restore_platforms(platform_to_test, mail_notify=False, as_conf=None, expid=None):
    Log.info("Checking the connection to all platforms in use")
    issues = ""
    ssh_config_issues = ""
    private_key_error = ("Please, add your private key to the ssh-agent ( ssh-add <path_to_key> )"
                         " or use a non-encrypted key\nIf ssh agent is not initialized, prompt "
                         "first eval `ssh-agent -s`")
    for platform in platform_to_test:
        platform_issues = ""
        try:
            message = platform.test_connection(as_conf)
            if message is None:
                message = "OK"
            if message != "OK":
                if message.find("doesn't accept remote connections") != -1:
                    ssh_config_issues += message
                elif message.find("Authentication failed") != -1:
                    ssh_config_issues += message + (". Please, check the user and project of this platform\n"
                                                    "If it is correct, try another host")
                elif message.find("private key file is encrypted") != -1:
                    if private_key_error not in ssh_config_issues:
                        ssh_config_issues += private_key_error
                elif message.find("Invalid certificate") != -1:
                    ssh_config_issues += message + ".Please, the eccert expiration date"
                else:
                    ssh_config_issues += message + (" this is an PARAMIKO SSHEXCEPTION: indicates that there is "
                                                    "something incompatible in the ssh_config for host:{0}\n maybe "
                                                    "you need to contact your sysadmin").format(
                        platform.host)
        except Exception as e:
            with suppress(Exception):
                if mail_notify:
                    email = as_conf.get_mails_to()
                    if "@" in email[0]:
                        Notifier.notify_experiment_status(MailNotifier(BasicConfig), expid, email, platform)
            platform_issues += f"\n[{platform.name}] Connection Unsuccessful to host {platform.host} "
            issues += platform_issues
            Log.warning(f"Error restoring platform [{platform.name}] host [{platform.host}]: {str(e)}")
            continue
        if platform.check_remote_permissions():
            Log.result(f"[{platform.name}] Correct user privileges for host {platform.host}")
        else:
            platform_issues += (
                f"\n[{platform.name}] has configuration issues.\n Check that the connection is passwd-less."
                f"(ssh {platform.user}@{platform.host})\n Check the parameters that build the root_path are "
                f"correct:{{scratch_dir/project/user}} = {{{platform.scratch}/{platform.project}/{platform.user}}}"
            )
            issues += platform_issues
        if platform_issues == "":

            Log.printlog("[{1}] Connection successful to host {0}".format(platform.host, platform.name), Log.RESULT)
        else:
            if platform.connected:
                platform.connected = False
                Log.printlog(
                    f"[{platform.name}] Connection successful to host {platform.host}, "
                    f"however there are issues with %HPCROOT%",
                    Log.WARNING
                )
            else:
                Log.printlog("[{1}] Connection failed to host {0}".format(platform.host, platform.name), Log.WARNING)

    if issues != "":
        if ssh_config_issues.find(private_key_error[:-2]) != -1:
            raise AutosubmitCritical(
                "Private key is encrypted, Autosubmit does not run in interactive mode.\n"
                "Please, add the key to the ssh agent(ssh-add <path_to_key>).\n"
                "It will remain open as long as session is active, "
                "for force clean you can prompt ssh-add -D",
                7073,
                issues + "\n" + ssh_config_issues
            )
        else:
            raise AutosubmitCritical(
                "Issues while checking the connectivity of platforms.",
                7010,
                issues + "\n" + ssh_config_issues
            )


# Source: https://github.com/cylc/cylc-flow/blob/a722b265ad0bd68bc5366a8a90b1dbc76b9cd282/cylc/flow/tui/util.py#L226
class NaturalSort:
    """An object to use as a sort key for sorting strings as a human would.

    This recognises numerical patterns within strings.

    Examples:
        >>> N = NaturalSort

        String comparisons work as normal:
        >>> N('') < N('')
        False
        >>> N('a') < N('b')
        True
        >>> N('b') < N('a')
        False

        Integer comparisons work as normal:
        >>> N('9') < N('10')
        True
        >>> N('10') < N('9')
        False

        Integers rank higher than strings:
        >>> N('1') < N('a')
        True
        >>> N('a') < N('1')
        False

        Integers within strings are sorted numerically:
        >>> N('a9b') < N('a10b')
        True
        >>> N('a10b') < N('a9b')
        False

        Lexicographical rules apply when substrings match:
        >>> N('a1b2') < N('a1b2c3')
        True
        >>> N('a1b2c3') < N('a1b2')
        False

        Equality works as per regular string rules:
        >>> N('a1b2c3') == N('a1b2c3')
        True
        >>> N('a1b2c3') is None
        False
    """

    PATTERN = re.compile(r'(\d+)')

    def __init__(self, value: str):
        self.value = tuple(
            int(item) if item.isdigit() else item
            for item in self.PATTERN.split(value)
            # remove empty strings if value ends with a digit
            if item
        )

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        for this, that in zip_longest(self.value, other.value):
            if this is None:
                return True
            if that is None:
                return False
            this_is_str = isinstance(this, str)
            that_is_str = isinstance(that, str)
            if this_is_str and that_is_str:
                if this == that:
                    continue
                return this < that
            this_isint = isinstance(this, int)
            that_is_int = isinstance(that, int)
            if this_isint and that_is_int:
                if this == that:
                    continue
                return this < that
            # For sorting integers before strings
            if this_isint and that_is_str:
                return True
            if this_is_str and that_is_int:
                return False
        return False


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.

    Original code: from distutils.util import strtobool
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


def get_rc_path(machine: bool, local: bool) -> Path:
    """Get the ``.autosubmit.rc`` path.

    If the environment variable ``AUTOSUBMIT_CONFIGURATION`` is specified in the
    system, this function will return a ``Path`` pointing to that value.

    If ``machine`` is ``True``, it will use the file from ``/etc/.autosubmitrc``
    (pay attention to the dot prefix).

    Else, if ``local`` is ``True``, it will use the file from  ``./.autosubmitrc``
    (i.e. it will use the current working directory for the process).

    Otherwise, it will load the file from ``~/.autosubmitrc``, for the user
    currently running Autosubmit.
    """
    if 'AUTOSUBMIT_CONFIGURATION' in os.environ:
        return Path(os.environ['AUTOSUBMIT_CONFIGURATION'])

    if machine:
        rc_path = '/etc'
    elif local:
        rc_path = '.'
    else:
        rc_path = Path.home()

    return Path(rc_path) / '.autosubmitrc'
