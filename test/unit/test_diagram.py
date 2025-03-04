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

""" Test file for autosubmit/monitor/diagram.py """

import datetime

from autosubmit.job.job import Job
from autosubmit.monitor import diagram
from autosubmit.monitor.diagram import JobData, JobAggData

def test_seq():
    """ function to test the Class JobData inside autosubmit/monitor/diagram.py """
    for value in diagram._seq(100, 2, 10):
        assert value is not None
        assert isinstance(value, int)

def test_populate_statistics():
    """ function to test the Class JobData inside autosubmit/monitor/diagram.py """
    jobs_data = [
        Job('test', "a29z", "COMPLETED", 200),
        Job('test', "a28z", "COMPLETED", 200),
        Job('test', "a27z", "COMPLETED", 200),
        Job('test', "a26z", "FAILED", 10)
    ]

    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(0.10)
    queue_time_fixes = ['test', 5]

    statistics = diagram.populate_statistics(jobs_data,date_ini,date_fin, queue_time_fixes)
    assert statistics is not None

def test_job_data():
    """ function to test the Class JobData inside autosubmit/monitor/diagram.py """
    job_data = JobData()

    assert job_data.headers() == ['Job Name', 'Queue Time', 'Run Time', 'Status']
    assert job_data.values() == ['', datetime.timedelta(0), datetime.timedelta(0), '']
    assert job_data.number_of_columns() == 4

def test_job_agg_data():
    """ function to test the Class JobAggData inside autosubmit/monitor/diagram.py """
    job_agg = JobAggData()
    assert job_agg.headers() == ['Section', 'Count', 'Queue Sum', 'Avg Queue', 'Run Sum', 'Avg Run']
    assert job_agg.values() == [{}, 0, datetime.timedelta(0), datetime.timedelta(0),
                                datetime.timedelta(0), datetime.timedelta(0)]
    assert job_agg.number_of_columns() == 6
