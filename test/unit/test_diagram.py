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
Test file for autosubmit/monitor/diagram.py
"""
import tempfile
from pathlib import Path

from mock.mock import patch

from autosubmit.job.job import Job
from autosubmit.monitor import diagram

from autosubmit.monitor.diagram import JobData, JobAggData


@patch('autosubmit.monitor.diagram._aggregate_jobs_by_section')
@patch('autosubmit.monitor.diagram._create_table')
@patch('autosubmit.monitor.diagram._create_csv')
def test_create_stats_report(jobs_by_association, create_table, create_csv):
    """
    function to test the function create_stats_report inside autosubmit/monitor/diagram.py
    """
    expid = "a000"
    jobs_data = [
        Job('test', "a29z", "COMPLETED", 200),
        Job('test', "a28z", "COMPLETED", 200),
        Job('test', "a27z", "COMPLETED", 200),
        Job('test', "a26z", "FAILED", 10)
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        if Path(temp_dir).exists() is False:
            Path(temp_dir).mkdir(parents=True, mode=0o777)

    jobs_by_association.return_value = [JobData(),JobAggData()]
    create_status = diagram.create_stats_report(expid, jobs_data, {}, temp_dir,True,True,False,None,
                                                None,None)
    assert create_status is False
