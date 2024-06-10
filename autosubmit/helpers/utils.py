import subprocess
import os
import pwd
import signal
import locale
from autosubmit.notifications.mail_notifier import MailNotifier

from autosubmit.notifications.notifier import Notifier

from autosubmitconfigparser.config.basicconfig import BasicConfig
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


