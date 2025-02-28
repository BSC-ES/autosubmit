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

import pytest

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
