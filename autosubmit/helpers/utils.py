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
import sys
from collections import defaultdict
from contextlib import suppress
from itertools import zip_longest
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Union

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.paramiko_submitter import _get_host, get_platform_by_type
from autosubmit.platforms.platform import Platform
from autosubmit.platforms.platform_type import PlatformType

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
        jobs_data: Iterable
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
        expid: str, basic_config: BasicConfig, raise_error=False, logger: Optional[Log] = None
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
        raise AutosubmitCritical(f"You don't own the experiment {expid}.", 7012)
    return is_owner, is_eadmin, current_owner_name


def restore_platforms(platforms_to_test, mail_notify=False, as_conf=None, expid=None):
    Log.info("Checking the connection to all platforms in use")
    issues = ""
    ssh_config_issues = ""
    private_key_error = (
        "Please, add your private key to the ssh-agent ( ssh-add <path_to_key> ) or use "
        "a non-encrypted key\nIf ssh agent is not initialized, prompt first eval `ssh-agent -s`"
    )

    for platform_to_test in platforms_to_test:
        platform_issues = ""
        try:
            message = platform_to_test.test_connection(as_conf)
            if message is None:
                message = "OK"
            if message != "OK":
                if message.find("doesn't accept remote connections") != -1:
                    ssh_config_issues += message
                elif message.find("Authentication failed") != -1:
                    ssh_config_issues += message + (
                        ". Please, check the user and project of this platform\n"
                        "If it is correct, try another host"
                    )
                elif message.find("private key file is encrypted") != -1:
                    if private_key_error not in ssh_config_issues:
                        ssh_config_issues += private_key_error
                elif message.find("Invalid certificate") != -1:
                    ssh_config_issues += message + ".Please, the eccert expiration date"
                else:
                    ssh_config_issues += message + (
                        f" this is an PARAMIKO SSHEXCEPTION: indicates that there is "
                        f"something incompatible in the ssh_config for host:{platform_to_test.host}\n maybe"
                        f" you need to contact your sysadmin"
                    )
        except Exception:
            try:
                if mail_notify:
                    email = as_conf.get_mails_to()
                    if "@" in email[0]:
                        Notifier.notify_experiment_status(
                            MailNotifier(BasicConfig), expid, email, platform_to_test
                        )
            except Exception as e2:
                Log.debug(f"Unexpected exception sending email notification: {str(e2)}")
            platform_issues += f"\n[{platform_to_test.name}] Connection Unsuccessful to host {platform_to_test.host} "
            issues += platform_issues
            continue
        if platform_to_test.check_remote_permissions():
            Log.result(
                f"[{platform_to_test.name}] Correct user privileges for host {platform_to_test.host}"
            )
        else:
            platform_issues += (
                f"\n[{platform_to_test.name}] has configuration issues.\n Check that the connection is"
                f" passwd-less.(ssh {platform_to_test.user}@{platform_to_test.host})\n Check the parameters that"
                f" build the root_path are correct:{{scratch_dir/project/user}} ="
                f" {{{platform_to_test.scratch}/{platform_to_test.project}/{platform_to_test.user}}}"
            )
            issues += platform_issues
        if platform_issues == "":
            Log.printlog(
                f"[{platform_to_test.name}] Connection successful to host {platform_to_test.host}",
                Log.RESULT,
            )
        else:
            if platform_to_test.connected:
                platform_to_test.connected = False
                Log.printlog(
                    f"[{platform_to_test.name}] Connection successful to host {platform_to_test.host}, "
                    f"however there are issues with %HPCROOT%",
                    Log.WARNING,
                )
            else:
                Log.printlog(
                    f"[{platform_to_test.name}] Connection failed to host {platform_to_test.host}",
                    Log.WARNING,
                )
    if issues != "":
        if ssh_config_issues.find(private_key_error[:-2]) != -1:
            raise AutosubmitCritical(
                "Private key is encrypted, Autosubmit does not run in "
                "interactive mode.\nPlease, add the key to the ssh agent(ssh-add "
                "<path_to_key>).\nIt will remain open as long as session is active, "
                "for force clean you can prompt ssh-add -D",
                7073,
                issues + "\n" + ssh_config_issues,
            )
        else:
            raise AutosubmitCritical(
                "Issues while checking the connectivity of platforms.",
                7010,
                issues + "\n" + ssh_config_issues,
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


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to ``True`` or ``False``.

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.

    Original code: from distutils.util import strtobool
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


def get_rc_path(machine: bool, local: bool) -> Path:
    """Get the ``.autosubmit.rc`` path.

    If the environment variable ``AUTOSUBMIT_CONFIGURATION`` is specified in the
    system, this function will return a ``Path`` pointing to that value.

    If ``machine`` is ``True``, it will use the file from ``/etc/autosubmitrc``.

    Else, if ``local`` is ``True``, it will use the file from  ``./.autosubmitrc``
    (i.e. it will use the current working directory for the process).

    Otherwise, it will load the file from ``~/.autosubmitrc``, for the user
    currently running Autosubmit.
    """
    if "AUTOSUBMIT_CONFIGURATION" in os.environ:
        return Path(os.environ["AUTOSUBMIT_CONFIGURATION"])

    rc_path: Union[str, Path]
    if machine:
        return Path("/etc/autosubmitrc")  # Higher priority than /etc/.autosubmitrc
    elif local:
        rc_path = "."
    else:
        rc_path = Path.home()

    return Path(rc_path) / ".autosubmitrc"


def user_yes_no_query(question: str) -> bool:
    """Utility function to ask user a yes/no question.

    :param question: question to ask
    :return: True if answer is yes, False if it is no
    """
    sys.stdout.write(f'{question} [y/n]\n')
    while True:
        try:
            answer = input()
            return strtobool(answer.lower())
        except ValueError:
            sys.stdout.write('Please respond with \'y\' or \'n\'.\n')
        except Exception as e:
            raise AutosubmitCritical("No input detected, the experiment will not be erased.", 7011, str(e))


def build_and_connect_platform(platform_name: str, as_conf: 'AutosubmitConfig', expid: str) -> Platform:
    """Build a minimal platform object and connect to it for STAT recovery.

    :param platform_name: Name of the platform in the experiment configuration.
    :param as_conf: Autosubmit config object.
    :param expid: Experiment identifier.
    :return: Connected platform instance.
    """
    if platform_name.lower() == PlatformType.LOCAL:
        config = {
            "LOCAL_ROOT_DIR": BasicConfig.LOCAL_ROOT_DIR,
            "LOCAL_TMP_DIR": BasicConfig.LOCAL_TMP_DIR,
        }
        plat = LocalPlatform(expid, platform_name, config=config)
    else:
        platforms_data = as_conf.experiment_data.get('PLATFORMS', {})
        platform_config = platforms_data.get(platform_name.upper(), {})
        platform_type = platform_config.get('TYPE', '').lower()
        platform_version = platform_config.get('VERSION', '')

        plat = get_platform_by_type(
            platform_type, expid, platform_name,
            as_conf.experiment_data, platform_version, None
        )

        if plat is None:
            raise AutosubmitCritical(
                f"PLATFORMS.{platform_name.upper()}.TYPE: {platform_type} is not supported", 7012
            )
        plat._version = platform_version

        add_project_to_host = str(platform_config.get('ADD_PROJECT_TO_HOST', False)).lower() != "false"
        section_project = platform_config.get('PROJECT', "")
        section_host = platform_config.get('HOST', "")
        plat.host = _get_host(section_host, add_project_to_host, section_project)
        plat.user = platform_config.get('USER', "")
        plat.scratch = platform_config.get('SCRATCH_DIR', "")
        plat.temp_dir = platform_config.get('TEMP_DIR', "")
        plat.root_dir = str(Path(plat.scratch) /
                             (plat.project if hasattr(plat, 'project') and plat.project else section_project) /
                             (plat.user if hasattr(plat, 'user') and plat.user else "") /
                             expid)

        with suppress(Exception):
            plat.update_cmds()

    plat.restore_connection(as_conf)
    return plat


def recover_stale_job_data(
        expid: str,
        as_conf: 'AutosubmitConfig',
        platforms: Optional[dict[str, Platform]] = None
) -> None:
    """Fetch STAT files for rows with submit>0 and (start=0 or finish=0)
    and update job_data directly. Uses existing platform connections when
    available (e.g. during run_experiment).

    :param expid: Experiment identifier.
    :param as_conf: Autosubmit config object.
    :param platforms: Optional dict of name -> connected platform to reuse.
    """
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid
    db_path = Path(BasicConfig.JOBDATA_DIR) / f"job_data_{expid}.db"
    if not db_path.exists():
        return

    exp_history = ExperimentHistory(expid, force_sql_alchemy=True)
    stale = exp_history.get_stale_rows()
    if not stale:
        return

    Log.info(f"Found {len(stale)} stale job_data rows — connecting to platforms")
    by_platform = defaultdict(list)
    for row in stale:
        by_platform[row.platform].append((row.job_name, int(row.fail_count)))

    for pn, jobs in by_platform.items():
        plat = platforms.get(pn) if platforms else None
        if not plat or not getattr(plat, 'connected', False):
            try:
                plat = build_and_connect_platform(pn, as_conf, expid)
            except Exception as e:
                Log.warning(f"Cannot connect to {pn}: {e}")
                continue

        for jn, fail_count in jobs:
            try:
                start, finish = _fetch_stat_timestamps(plat, exp_path, jn, fail_count)
                if start or finish:
                    exp_history.update_job_data_values(jn, fail_count, start, finish)
            except Exception as e:
                Log.warning(f"Could not recover {jn} fail_count={fail_count}: {e}")


def _fetch_stat_timestamps(
        plat: Platform,
        exp_path: Path,
        job_name: str,
        fail_count: int
) -> tuple[int, int]:
    """Download STAT file from platform and return (start, finish) timestamps.

    :param plat: Connected platform instance.
    :param exp_path: Experiment root path.
    :param job_name: Full job name.
    :param fail_count: Retry attempt number.
    :return: Tuple of (start, finish) epoch integers.
    """
    stat = f"{job_name}_STAT_{fail_count}"
    local = exp_path / BasicConfig.LOCAL_TMP_DIR / stat
    if plat.check_file_exists(stat):
        plat.get_file(stat, True)
        if local.exists():
            return _parse_stat_file(local)
    return 0, 0


def _parse_stat_file(path: Path) -> tuple[int, int]:
    """Read a STAT file and return (start, finish) epoch integers."""
    lines = [x.strip() for x in path.read_text().splitlines() if x.strip()]
    try:
        values = [int(x) for x in lines[:2]]
    except ValueError:
        Log.warning(f"STAT file {path} contains non-integer data, skipping")
        return 0, 0
    return (values[0], values[1]) if len(values) >= 2 else (values[0], 0) if values else (0, 0)
