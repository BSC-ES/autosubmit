import collections

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
    eadmin_user = os.popen('id -u eadmin').read().strip()  # If eadmin no exists, it would be "" so INT() would fail.
    if eadmin_user != "":
        is_eadmin = my_user_ID == int(eadmin_user)
    else:
        is_eadmin = False
    if not is_owner and raise_error:
        raise AutosubmitCritical("You don't own the experiment {0}.".format(expid), 7012)
    return is_owner, is_eadmin, current_owner_name

def load_job_list(expid, as_conf, notransitive=False, monitor=False, new = True):
    rerun = as_conf.get_rerun()
    job_list = JobList(expid, BasicConfig, YAMLParserFactory(),
                       get_job_list_persistence(expid, as_conf), as_conf)
    run_only_members = as_conf.get_member_list(run_only=True)
    date_list = as_conf.get_date_list()
    date_format = ''
    if as_conf.get_chunk_size_unit() == 'hour':
        date_format = 'H'
    for date in date_list:
        if date.hour > 1:
            date_format = 'H'
        if date.minute > 1:
            date_format = 'M'
    wrapper_jobs = dict()
    for wrapper_section, wrapper_data in as_conf.experiment_data.get("WRAPPERS", {}).items():
        if isinstance(wrapper_data, collections.abc.Mapping):
            wrapper_jobs[wrapper_section] = wrapper_data.get("JOBS_IN_WRAPPER", "")

    job_list.generate(as_conf, date_list, as_conf.get_member_list(), as_conf.get_num_chunks(), as_conf.get_chunk_ini(),
                      as_conf.experiment_data, date_format, as_conf.get_retrials(),
                      as_conf.get_default_job_type(), wrapper_jobs,
                      new=new, run_only_members=run_only_members,monitor=monitor)

    if str(rerun).lower() == "true":
        rerun_jobs = as_conf.get_rerun_jobs()
        job_list.rerun(rerun_jobs,as_conf, monitor=monitor)
    else:
        job_list.remove_rerun_only_jobs(notransitive)

    return job_list

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

def get_submitter(as_conf):
    """
    Returns the submitter corresponding to the communication defined on autosubmit's config file
    :param as_conf: AutosubmitConfigParser
    :return: Submitter
    """
    try:
        communications_library = as_conf.get_communications_library()
    except Exception as e:
        communications_library = 'paramiko'
    if communications_library == 'paramiko':
        return ParamikoSubmitter()
    else:
        # only paramiko is available right now.
        return ParamikoSubmitter()

def get_job_list_persistence(expid, as_conf):
    """
    Returns the JobListPersistence corresponding to the storage type defined on autosubmit's config file

    :return: job_list_persistence
    :rtype: JobListPersistence
    """
    storage_type = as_conf.get_storage_type()
    if storage_type == 'pkl':
        return JobListPersistencePkl()
    elif storage_type == 'db':
        return JobListPersistenceDb(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                    "job_list_" + expid)
    raise AutosubmitCritical('Storage type not known', 7014)