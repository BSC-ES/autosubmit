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
from datetime import timedelta
from unittest.mock import patch

from typing import Callable
import pytest

import autosubmit.helpers.autosubmit_helper as helper
from log.log import AutosubmitCritical


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


@pytest.mark.parametrize('ids, return_list_value, result',[
        (None, [''], []),
        ('', [''], ''),
        ('a000', ['a001'], ''),
        ('a000', ['a000'], ['a000']),
        ('a000 a001', ['a000', 'a001'], ['a000', 'a001']),
        ('a000 a001', ['a000', 'a001', 'a002'], ['a000', 'a001']),
],ids=['None','expected AScritical members','expected AScritical rmembers',
       'one ids','multiple sent ids','multiple return ids']
)
@patch('autosubmit.helpers.autosubmit_helper.AutosubmitConfig.get_member_list')
@pytest.mark.xfail(raise_stmt=AutosubmitCritical)
def test_get_allowed_members(as_member_list, ids, return_list_value, result,
                             autosubmit_config: Callable):
    """
        function to test the function get_allowed_members inside autosubmit_helper
    """
    expid = 'a000'

    as_config = autosubmit_config(expid, experiment_data={})
    as_member_list.return_value = return_list_value

    assert helper.get_allowed_members(ids, as_config) == result
