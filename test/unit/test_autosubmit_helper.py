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

import datetime
from datetime import timedelta
from typing import Callable

import pytest

import autosubmit.helpers.autosubmit_helper as helper
from log.log import AutosubmitCritical


@pytest.mark.parametrize('time', [
    '04-00-00',
    '04:00:00',
    '2020:01:01 04:00:00',
    '2020-01-01 04:00:00',
    datetime.datetime.now() + timedelta(seconds=5),
], ids=['wrong format hours', 'right format hours', 'fulldate wrong format', 'fulldate right format',
        'execute in 5 seconds']
                         )
def teste_handle_start_time(time):
    """
    function to test the function handle_start_time inside autosubmit_helper
    """
    if not isinstance(time, str):
        time = time.strftime("%Y-%m-%d %H:%M:%S")
    assert helper.handle_start_time(time) is None


@pytest.mark.parametrize('ids, return_list_value, result', [
    (None, [''], []),
    ('', [''], ''),
    ('a000', ['a001'], ''),
    ('a000', ['a000'], ['a000']),
    ('a000 a001', ['a000', 'a001'], ['a000', 'a001']),
    ('a000 a001', ['a000', 'a001', 'a002'], ['a000', 'a001']),
], ids=['None', 'expected AScritical members', 'expected AScritical rmembers',
        'one ids', 'multiple sent ids', 'multiple return ids']
                         )
@pytest.mark.xfail(raise_stmt=AutosubmitCritical)
def test_get_allowed_members(mocker, ids, return_list_value, result,
                             autosubmit_config: Callable):
    """
        function to test the function get_allowed_members inside autosubmit_helper
    """
    expid = 'a000'

    as_member_list = mocker.patch('autosubmit.helpers.autosubmit_helper.AutosubmitConfig.get_member_list')

    as_config = autosubmit_config(expid, experiment_data={})
    as_member_list.return_value = return_list_value

    assert helper.get_allowed_members(ids, as_config) == result


@pytest.mark.parametrize('time, header_skip, experiment_exists', [
    ('a000', False, False),
    ('04-00-00', False, False),
    ('04-00-00', False, True),
    ('04:00:00', True, False),
    ('04:00:00', True, True),
], ids=['expid instead of time', 'wrong format hours', 'right format hours', 'fulldate wrong format',
        'fulldate wrong format false']
                         )
def teste_handle_start_after(mocker, autosubmit_config: Callable, time: str,
                             header_skip: bool, experiment_exists: bool):
    """
    function to test the function handle_start_time inside autosubmit_helper
    """
    autosubmit_helper = mocker.patch('autosubmit.helpers.autosubmit_helper.check_experiment_exists')
    mock_experiment_history = mocker.patch('autosubmit.helpers.autosubmit_helper.ExperimentHistory')
    mocked_sleep = mocker.patch('autosubmit.helpers.autosubmit_helper.sleep')

    expid = 'a000'

    autosubmit_config(expid, experiment_data={})
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

    helper.handle_start_after(time, expid)
    if header_skip is True and experiment_exists is True:
        assert mocked_exp_history.manager.get_experiment_run_dc_with_max_id.called
    assert mocked_sleep.has_been_called()
