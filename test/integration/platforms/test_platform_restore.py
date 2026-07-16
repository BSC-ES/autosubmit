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

import pytest

from autosubmit.helpers.utils import restore_platforms
from autosubmit.log.log import AutosubmitCritical


@pytest.mark.parametrize(
    "message_fragment, expected_log, expected_cm",
    [
        ("doesn't accept remote connections", "Connection failed to host", "Issues while checking the connectivity of platforms."),
        ("Authentication failed", "Connection failed to host", "Issues while checking the connectivity of platforms."),
        ("private key file is encrypted", "Connection failed to host", "Private key is encrypted, Autosubmit does not run in interactive mode."),
        ("Invalid certificate", "Connection failed to host", "Issues while checking the connectivity of platforms."),
        ("some other failure", "Connection failed to host", "Issues while checking the connectivity of platforms."),
    ],
)
def test_restore_platforms_raises_on_issues(
    get_next_expid, autosubmit_exp, mocker, message_fragment: str, expected_log, expected_cm: str
) -> None:
    """When a platform yields a problematic message, an AutosubmitCritical is raised."""
    exp = autosubmit_exp(get_next_expid(), experiment_data={})
    exp.platform = mocker.Mock()
    exp.platform.test_connection.return_value = message_fragment
    exp.platform.check_remote_permissions.return_value = False
    exp.platform.connected = False

    mock_log_printlog = mocker.patch("autosubmit.log.log.Log.printlog")

    with pytest.raises(AutosubmitCritical) as cm:
        restore_platforms([exp.platform], mail_notify=False, as_conf=None, expid=None)

    assert expected_cm in str(cm.value.message)
    assert expected_log in mock_log_printlog.call_args.args[0]


def test_restore_platforms(get_next_expid, autosubmit_exp):
    expid = get_next_expid()
    exp = autosubmit_exp(expid,
        experiment_data={
            "DEFAULT": {"CUSTOM_CONFIG": "test"},
            "MAIL": {
                "NOTIFICATIONS": "True",
                "TO": "uhu@uhu.com",
                "ATTACHMENT": "True",
            },
            "JOBS": {
                "LOCAL_SEND_INITIAL": {
                    "CHUNKS_FROM": {"1": {"CHUNKS_TO": "1"}},
                    "PLATFORM": "DUMMY_PLATFORM",
                    "SCRIPT": "sleep 0",
                }
            },
            "LOCAL": {"PROJECT_PATH": "path"},
            "GIT": {"PROJECT_ORIGIN": "origin_test", "PROJECT_BRANCH": "branch_test"},
            "PLATFORMS": {
                "dummy_platform": {
                    "type": "ps",
                    "whatever": "dummy_value",
                    "whatever2": "dummy_value2",
                    "CUSTOM_DIRECTIVES": ["$SBATCH directive1", "$SBATCH directive2"],
                },
            },
        },
        include_jobs=True,
    )

    with pytest.raises(AutosubmitCritical) as cm:
        restore_platforms(
            platforms_to_test=[exp.platform],
            mail_notify=True,
            as_conf=exp.as_conf,
            expid=expid,
        )
    assert cm.value.args[0] == "Issues while checking the connectivity of platforms."
