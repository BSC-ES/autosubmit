import os
import pwd
import re
from itertools import zip_longest

from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier
from log.log import AutosubmitCritical, Log


def check_jobs_file_exists(as_conf, current_section_name=None):
    if str(as_conf.experiment_data.get("PROJECT", {}).get("PROJECT_TYPE", "none")).lower() != "none":
        templates_dir = f"{as_conf.experiment_data.get('ROOTDIR','')}/proj/{as_conf.experiment_data.get('PROJECT', {}).get('PROJECT_DESTINATION', '')}"
        if not os.path.exists(templates_dir):
            raise AutosubmitCritical(f"Templates directory {templates_dir} does not exist", 7011)

        # List of files that doesn't exist.
        missing_files = ""
        # Check if all files in jobs_data exist or only current section
        if current_section_name:
            jobs_data = [as_conf.jobs_data.get(current_section_name, {})]
        else:
            jobs_data = as_conf.jobs_data.values()
        for data in jobs_data:
            if "SCRIPT" not in data:
                if "FILE" in data:
                    if not os.path.exists(f"{templates_dir}/{data['FILE']}"):
                        missing_files += f"{templates_dir}/{data['FILE']} \n"
                    else:
                        Log.result(f"File {templates_dir}/{data['FILE']} exists")
        if missing_files:
            raise AutosubmitCritical(f"Templates not found:\n{missing_files}", 7011)


def check_experiment_ownership(expid, basic_config, raise_error=False, logger=None):
    # [A-Za-z09]+ variable is not needed, LOG is global thus it will be read if available
    ## type: (str, BasicConfig, bool, Log) -> Tuple[bool, bool, str]
    my_user_ID = os.getuid()
    current_owner_ID = 0
    current_owner_name = "NA"
    try:
        current_owner_ID = os.stat(os.path.join(basic_config.LOCAL_ROOT_DIR, expid)).st_uid
        current_owner_name = pwd.getpwuid(os.stat(os.path.join(basic_config.LOCAL_ROOT_DIR, expid)).st_uid).pw_name
    except Exception as e:
        if logger:
            logger.info("Error while trying to get the experiment's owner information.")
    finally:
        if current_owner_ID <= 0 and logger:
            logger.info("Current owner '{0}' of experiment {1} does not exist anymore.", current_owner_name, expid)
    is_owner = current_owner_ID == my_user_ID
    eadmin_user = os.popen('id -u eadmin').read().strip() # If eadmin no exists, it would be "" so INT() would fail.
    if eadmin_user != "":
        is_eadmin = my_user_ID == int(eadmin_user)
    else:
        is_eadmin = False
    if not is_owner and raise_error:
        raise AutosubmitCritical("You don't own the experiment {0}.".format(expid), 7012)
    return is_owner, is_eadmin, current_owner_name

def restore_platforms(platform_to_test, mail_notify=False, as_conf=None, expid=None):
    Log.info("Checking the connection to all platforms in use")
    issues = ""
    platform_issues = ""
    ssh_config_issues = ""
    private_key_error = "Please, add your private key to the ssh-agent ( ssh-add <path_to_key> ) or use a non-encrypted key\nIf ssh agent is not initialized, prompt first eval `ssh-agent -s`"
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
                    ssh_config_issues += message + ". Please, check the user and project of this platform\nIf it is correct, try another host"
                elif message.find("private key file is encrypted") != -1:
                    if private_key_error not in ssh_config_issues:
                        ssh_config_issues += private_key_error
                elif message.find("Invalid certificate") != -1:
                    ssh_config_issues += message + ".Please, the eccert expiration date"
                else:
                    ssh_config_issues += message + " this is an PARAMIKO SSHEXCEPTION: indicates that there is something incompatible in the ssh_config for host:{0}\n maybe you need to contact your sysadmin".format(
                        platform.host)
        except BaseException as e:
            try:
                if mail_notify:
                    email = as_conf.get_mails_to()
                    if "@" in email[0]:
                        Notifier.notify_experiment_status(MailNotifier(BasicConfig), expid, email, platform)
            except Exception as e:
                pass
            platform_issues += "\n[{1}] Connection Unsuccessful to host {0} ".format(
                platform.host, platform.name)
            issues += platform_issues
            continue
        if platform.check_remote_permissions():
            Log.result("[{1}] Correct user privileges for host {0}",
                       platform.host, platform.name)
        else:
            platform_issues += "\n[{0}] has configuration issues.\n Check that the connection is passwd-less.(ssh {1}@{4})\n Check the parameters that build the root_path are correct:{{scratch_dir/project/user}} = {{{3}/{2}/{1}}}".format(
                platform.name, platform.user, platform.project, platform.scratch, platform.host)
            issues += platform_issues
        if platform_issues == "":

            Log.printlog("[{1}] Connection successful to host {0}".format(platform.host, platform.name), Log.RESULT)
        else:
            if platform.connected:
                platform.connected = False
                Log.printlog("[{1}] Connection successful to host {0}, however there are issues with %HPCROOT%".format(platform.host, platform.name),
                             Log.WARNING)
            else:
                Log.printlog("[{1}] Connection failed to host {0}".format(platform.host, platform.name), Log.WARNING)
    if issues != "":
        if ssh_config_issues.find(private_key_error[:-2]) != -1:
            raise AutosubmitCritical("Private key is encrypted, Autosubmit does not run in interactive mode.\nPlease, add the key to the ssh agent(ssh-add <path_to_key>).\nIt will remain open as long as session is active, for force clean you can prompt ssh-add -D",7073, issues + "\n" + ssh_config_issues)
        else:
            raise AutosubmitCritical("Issues while checking the connectivity of platforms.", 7010, issues + "\n" + ssh_config_issues)

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

    """

    PATTERN = re.compile(r'(\d+)')

    def __init__(self, value):
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
            this_isstr = isinstance(this, str)
            that_isstr = isinstance(that, str)
            if this_isstr and that_isstr:
                if this == that:
                    continue
                return this < that
            this_isint = isinstance(this, int)
            that_isint = isinstance(that, int)
            if this_isint and that_isint:
                if this == that:
                    continue
                return this < that
            if this_isint and that_isstr:
                return True
            if this_isstr and that_isint:
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
