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

"""Unit tests for ``Autosubmit.online_recovery`` per-platform resilience."""

import paramiko
import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.log.log import AutosubmitCritical, AutosubmitError

PLATFORM_ERRORS = [
    AutosubmitError(
        "SSH transport issue while sending command 'find ...' on MN5_PS: "
        "Key-exchange timed out waiting for key negotiation", 6005),
    AutosubmitError('SSH transport issue', 6005),
    OSError('connection reset by peer'),
    paramiko.SSHException('Key-exchange timed out waiting for key negotiation'),
]


def _connected_platform(mocker, name='MN5_PS'):
    platform = mocker.Mock()
    platform.name = name
    platform.test_connection.return_value = None
    platform.connected = True
    return platform


def _unreachable_platform(mocker, name='MN5_PS', message='Timeout connection'):
    platform = mocker.Mock()
    platform.name = name
    platform.test_connection.return_value = message
    platform.connected = False
    return platform


@pytest.mark.parametrize('exc', PLATFORM_ERRORS, ids=['key_exchange', 'autosubmit_error', 'os_error', 'raw_ssh_exception'])
def test_online_recovery_offline_falls_back_to_history_on_platform_error(mocker, exc):
    platform = _connected_platform(mocker)
    platform.get_completed_job_names.side_effect = exc
    job_list = mocker.Mock()
    job_list.recover_all_completed_jobs_from_exp_history.return_value = ['job1', 'job2']

    result = Autosubmit.online_recovery(mocker.Mock(), [platform], job_list, offline=True)

    assert set(result) == {'job1', 'job2'}
    job_list.recover_all_completed_jobs_from_exp_history.assert_called_once_with(platform)


@pytest.mark.parametrize('exc', PLATFORM_ERRORS, ids=['key_exchange', 'autosubmit_error', 'os_error', 'raw_ssh_exception'])
def test_online_recovery_keeps_processing_other_platforms_when_offline(mocker, exc):
    healthy = _connected_platform(mocker, name='MN5_SLURM')
    healthy.get_completed_job_names.return_value = ['done1']
    broken = _connected_platform(mocker, name='MN5_PS')
    broken.get_completed_job_names.side_effect = exc
    job_list = mocker.Mock()
    job_list.recover_all_completed_jobs_from_exp_history.return_value = ['from_history']

    result = Autosubmit.online_recovery(mocker.Mock(), [broken, healthy], job_list, offline=True)

    assert sorted(result) == ['done1', 'from_history']


@pytest.mark.parametrize('exc', PLATFORM_ERRORS, ids=['key_exchange', 'autosubmit_error', 'os_error', 'raw_ssh_exception'])
def test_online_recovery_raises_critical_naming_platform_when_not_offline(mocker, exc):
    platform = _connected_platform(mocker)
    platform.get_completed_job_names.side_effect = exc
    job_list = mocker.Mock()

    with pytest.raises(AutosubmitCritical) as cm:
        Autosubmit.online_recovery(mocker.Mock(), [platform], job_list, offline=False)

    assert 'MN5_PS' in str(cm.value.message)


@pytest.mark.parametrize('message', [
    'Timeout connection',
    "host doesn't accept remote connections",
], ids=['timeout', 'refused'])
def test_online_recovery_offline_falls_back_to_history_when_platform_unreachable(mocker, message):
    platform = _unreachable_platform(mocker, message=message)
    job_list = mocker.Mock()
    job_list.recover_all_completed_jobs_from_exp_history.return_value = ['job1', 'job2']

    result = Autosubmit.online_recovery(mocker.Mock(), [platform], job_list, offline=True)

    assert set(result) == {'job1', 'job2'}
    job_list.recover_all_completed_jobs_from_exp_history.assert_called_once_with(platform)


@pytest.mark.parametrize('message', [
    'Timeout connection',
    "host doesn't accept remote connections",
], ids=['timeout', 'refused'])
def test_online_recovery_raises_critical_when_platform_unreachable_and_not_offline(mocker, message):
    platform = _unreachable_platform(mocker, message=message)
    job_list = mocker.Mock()

    with pytest.raises(AutosubmitCritical) as cm:
        Autosubmit.online_recovery(mocker.Mock(), [platform], job_list, offline=False)

    assert 'MN5_PS' in str(cm.value.message)
