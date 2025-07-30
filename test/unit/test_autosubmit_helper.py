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

"""Test file for ``autosubmit_helper.py``."""

from datetime import datetime, timedelta
from typing import Callable, Union

import pytest

import autosubmit.helpers.autosubmit_helper as helper
from autosubmit.log.log import AutosubmitCritical

_EXPID = 't000'


@pytest.mark.parametrize(
    'time',
    [
        '04-00-00',
        '04:00:00',
        '2020:01:01 04:00:00',
        '2020-01-01 04:00:00',
        datetime.now() + timedelta(seconds=5),
    ],
    ids=[
        'wrong format hours',
        'right format hours',
        'fulldate wrong format',
        'fulldate right format',
        'execute in 5 seconds'
    ]
)
def test_handle_start_time(time):
    """Test the function handle_start_time inside autosubmit_helper."""
    if not isinstance(time, str):
        time = time.strftime("%Y-%m-%d %H:%M:%S")
    assert helper.handle_start_time(time) is None


@pytest.mark.parametrize(
    'ids,return_list_value,result',
    [
        (None, [''], []),
        ('', [''], AutosubmitCritical),
        (_EXPID, ['a001'], AutosubmitCritical),
        (_EXPID, [_EXPID], [_EXPID]),
        (f'{_EXPID} a001', [_EXPID, 'a001'], [_EXPID, 'a001']),
        (f'{_EXPID} a001', [_EXPID, 'a001', 'a002'], [_EXPID, 'a001']),
    ], ids=[
        'None',
        'expected AScritical members',
        'expected AScritical run_members',
        'one ids',
        'multiple sent ids',
        'multiple return ids'
    ]
)
def test_get_allowed_members(
        ids,
        return_list_value,
        result: Union[str, Exception],
        autosubmit_config: Callable,
        mocker
) -> None:
    """Test the function get_allowed_members inside autosubmit_helper."""
    mocker.patch(
        'autosubmit.helpers.autosubmit_helper.AutosubmitConfig.get_member_list',
        return_value=return_list_value
    )

    as_config = autosubmit_config(_EXPID, experiment_data={})

    if type(result) is str or type(result) is list:
        assert helper.get_allowed_members(ids, as_config) == result
    else:
        with pytest.raises(result):  # type: ignore
            helper.get_allowed_members(ids, as_config)



@pytest.mark.parametrize(
    'time,header_skip,experiment_exists',
    [
        (_EXPID, False, False),
        ('04-00-00', False, False),
        ('04-00-00', False, True),
        ('04:00:00', True, False),
        ('04:00:00', True, True),
    ],
    ids=[
        'expid instead of time',
        'wrong format hours',
        'right format hours',
        'fulldate wrong format',
        'fulldate wrong format false'
    ]
)
def test_handle_start_after(mocker, autosubmit_config: Callable, time: str,
                             header_skip: bool, experiment_exists: bool):
    """Test the function handle_start_time inside autosubmit_helper"""
    autosubmit_helper = mocker.patch('autosubmit.helpers.autosubmit_helper.check_experiment_exists')
    mock_experiment_history = mocker.patch('autosubmit.helpers.autosubmit_helper.ExperimentHistory')
    mocked_sleep = mocker.patch('autosubmit.helpers.autosubmit_helper.sleep')

    autosubmit_config(_EXPID, experiment_data={})
    experiment_history = mocker.Mock()
    experiment_history2 = mocker.Mock()

    attr: str
    for attr in ['finish', 'total', 'completed', 'suspended', 'queuing', 'running', 'failed']:
        setattr(experiment_history, attr, 0)
        setattr(experiment_history2, attr, 1)

    experiment_history.total = 1
    experiment_history.__bool__ = lambda _: True

    experiment_history2.total = 2
    experiment_history2.__bool__ = lambda _: True

    mocked_exp_history = mocker.Mock()
    mocked_exp_history.manager.get_experiment_run_dc_with_max_id.side_effect = [experiment_history, experiment_history2,
                                                                                None]
    mocked_exp_history.is_header_ready.return_value = header_skip
    mock_experiment_history.return_value = mocked_exp_history

    autosubmit_helper.return_value = experiment_exists
    mocked_sleep.return_value = 0

    helper.handle_start_after(time, _EXPID)
    if header_skip is True and experiment_exists is True:
        assert mocked_exp_history.manager.get_experiment_run_dc_with_max_id.called
    assert mocked_sleep.has_been_called()
