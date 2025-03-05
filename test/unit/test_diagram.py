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
import tempfile
import datetime
from pathlib import Path

import pytest
from mock.mock import patch

from autosubmit.job.job import Job
from autosubmit.monitor import diagram
from autosubmit.monitor.diagram import JobData, JobAggData


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


@patch('autosubmit.monitor.diagram._aggregate_jobs_by_section')
@patch('autosubmit.monitor.diagram._create_table')
@patch('autosubmit.monitor.diagram._create_csv')
def test_create_stats_report(jobs_by_association, create_table, create_csv):
    """ function to test the function create_stats_report inside autosubmit/monitor/diagram.py """
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


def test_create_csv_stats(tmpdir):
    """ function to test the Function create_csv_stats inside autosubmit/monitor/diagram.py """
    jobs_data = [
        Job('test', "a000", "COMPLETED", 200),
        Job('test', "a000", "COMPLETED", 200),
        Job('test', "a000", "COMPLETED", 200),
        Job('test', "a000", "FAILED", 10)
    ]

    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(0.10)
    queue_time_fixes = ['test', 5]

    statistics = diagram.populate_statistics(jobs_data, date_ini, date_fin, queue_time_fixes)
    file_tmpdir = tmpdir + '.pdf'
    diagram.create_csv_stats(statistics, jobs_data, str(file_tmpdir))

    tmpdir += '.csv'
    assert tmpdir.exists()


@pytest.mark.parametrize("job_stats, failed_jobs, failed_jobs_dict, num_plots, result", [
    (
            ["COMPLETED", "COMPLETED", "COMPLETED", "FAILED"],
            [0, 0, 0, 1],
            {"a26z": 1},
            40,
            True
    ), (
            ["COMPLETED", "COMPLETED", "COMPLETED", "FAILED"],
            [0, 0, 0, 1],
            {"a26z": 1},
            0,
            False
    ), (
            ["COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "COMPLETED", "COMPLETED", "COMPLETED",
             "FAILED", "COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "COMPLETED", "COMPLETED",
             "COMPLETED", "FAILED", "COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "COMPLETED",
             "COMPLETED", "COMPLETED", "FAILED", "COMPLETED", "COMPLETED", "COMPLETED", "FAILED",
             "COMPLETED", "COMPLETED", "COMPLETED", "FAILED"],
            [0, 0, 0, 1],
            {"a26z": 1},
            10,
            True
    ), (
            [],
            [0, 0, 0, 1],
            {},
            40,
            True
    ), (
            [],
            [],
            {},
            40,
            True
    ),
],
ids=['all run', 'divided by zero', 'run with continue', 'fail job_dict', 'no run'])
def test_create_bar_diagram(job_stats, failed_jobs, failed_jobs_dict, num_plots, result):
    """ function to test the function create_bar_diagram inside autosubmit/monitor/diagram.py """
    jobs_data = [
        Job('test', "a000", "COMPLETED", 200),
        Job('test', "a000", "COMPLETED", 200),
        Job('test', "a000", "COMPLETED", 200),
        Job('test', "a000", "FAILED", 10)
    ]
    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(0.10)
    queue_time_fixes = {'test': 5}

    status = ["COMPLETED", "COMPLETED", "COMPLETED", "FAILED"]
    statistics = diagram.populate_statistics(jobs_data, date_ini, date_fin, queue_time_fixes)
    statistics.jobs_stat = job_stats
    statistics.failed_jobs = failed_jobs
    statistics.failed_jobs_dict = failed_jobs_dict

    with patch('autosubmit.monitor.diagram.MAX_NUM_PLOTS', num_plots):  # 1
        assert result == diagram.create_bar_diagram("a000", statistics, jobs_data, status)
