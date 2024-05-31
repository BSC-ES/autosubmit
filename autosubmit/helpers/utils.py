import signal

import os
import pwd
from autosubmit.job.job_list_persistence import JobListPersistencePkl, JobListPersistenceDb

from autosubmit.notifications.mail_notifier import MailNotifier

from autosubmit.notifications.notifier import Notifier

from autosubmit.job.job_list import JobList
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from log.log import AutosubmitCritical, Log

import subprocess
import locale


def terminate_child_process(expid, platform=None):
    # get pid of the main process
    pid = os.getpid()
    # In case someone used 4.1.6 or 4.1.5
    process_ids = proccess_id(expid, "run", single_instance=False, platform=platform)
    if process_ids:
        for process_id in [process_id for process_id in process_ids if process_id != pid]:
            # force kill
            os.kill(process_id, signal.SIGKILL)
    process_ids = proccess_id(expid, "log", single_instance=False, platform=platform)
    # 4.1.7 +
    if process_ids:
        for process_id in [process_id for process_id in process_ids if process_id != pid]:
            # force kill
            os.kill(process_id, signal.SIGKILL)

def proccess_id(expid=None, command="run", single_instance=True, platform=None):
    # Retrieve the process id of the autosubmit process
    # Bash command: ps -ef | grep "$(whoami)" | grep "autosubmit" | grep "run" | grep "expid" | awk '{print $2}'
    try:
        if not platform:
            command = f'ps -ef | grep "$(whoami)" | grep "autosubmit" | grep "{command}" | grep "{expid}" '
        else:
            command = f'ps -ef | grep "$(whoami)" | grep "autosubmit" | grep "{command}" | grep "{expid}" | grep " {platform.lower()} " '
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        output = output.decode(locale.getlocale()[1])
        output = output.split('\n')
        # delete noise
        if output:
            output = [int(x.split()[1]) for x in output if x and "grep" not in x]

    except Exception as e:
        raise AutosubmitCritical(
            "An error occurred while retrieving the process id", 7011, str(e))
    if single_instance:
        return output[0] if output else ""
    else:
        return output if output else ""

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