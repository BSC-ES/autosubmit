# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Code to manage Autosubmit platforms."""

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier


def restore_platforms(
    platforms_to_test, mail_notify=False, as_conf=None, expid=None
) -> None:
    """Try to restore each platform used in an experiment.

    :param platforms_to_test: List of platforms to test.
    :param mail_notify: The email to send notifications.
    :param as_conf: Autosubmit configuration.
    :param expid: Experiment identifier.
    :raises AutosubmitCritical: If an error occurs trying to restore a platform.
    """
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
