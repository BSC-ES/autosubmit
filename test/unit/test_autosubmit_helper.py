#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
"""
Test file for autosubmit_help.py
"""
import datetime
from collections.abc import Callable
from datetime import timedelta

import pytest
from mock.mock import patch, MagicMock

import autosubmit.helpers.autosubmit_helper as helper

@pytest.mark.parametrize('time',[
        '04-00-00',
        '04:00:00',
        '2020:01:01 04:00:00',
        '2020-01-01 04:00:00',
        datetime.datetime.now() + timedelta(seconds=5),
],ids=['wrong format hours','right format hours','fulldate wrong format','fulldate right format',
       'execute in 5 seconds']
)
def teste_handle_start_time(time):
    """
    function to test the function handle_start_time inside autosubmit_helper
    """
    if not isinstance(time, str) :
        time = time.strftime("%Y-%m-%d %H:%M:%S")
    assert helper.handle_start_time(time) is None


@patch('autosubmit.helpers.autosubmit_helper.sleep')
@patch('autosubmit.helpers.autosubmit_helper.ExperimentHistory.is_header_ready')
@patch('autosubmit.helpers.autosubmit_helper.ExperimentHistory')
@patch('autosubmit.helpers.autosubmit_helper.check_experiment_exists')
@pytest.mark.parametrize('time, header_skip',[
        ('a000', False),
        ('04-00-00', False),
        ('04:00:00', True),
        ('2020:01:01 04:00:00', True)
],ids=['expid instead of time','wrong format hours','right format hours','fulldate wrong format']
)
def teste_handle_start_after(autosubmit_helper, mock_experiment_history, mock_header_ready,
                             mocked_sleep, autosubmit_config: Callable, time: str,
                             header_skip: bool):
    """
    function to test the function handle_start_time inside autosubmit_helper
    """
    expid = 'a000'
    autosubmit_config(expid, experiment_data = {})

    experiment_history = experiment_history2 = MagicMock(spec='experiment_history')

    experiment_history.finish = experiment_history.total = experiment_history.completed = (
        experiment_history).suspended = experiment_history.queuing = experiment_history.running = (
        experiment_history).failed = 0

    experiment_history2.finish = experiment_history2.total = experiment_history2.completed = (
        experiment_history2).suspended = experiment_history2.queuing = experiment_history2.running \
        = experiment_history2.failed = 1

    experiment_history.total = experiment_history2.total = 2

    mock_experiment_history.return_value.manager.get_experiment_run_dc_with_max_id.side_effect = \
        [experiment_history, experiment_history2]
    mock_header_ready.return_value = header_skip
    autosubmit_helper.return_value = True
    mocked_sleep.return_value = 0

    assert helper.handle_start_after(time, expid) is None
