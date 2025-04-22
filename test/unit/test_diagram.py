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

"""Tests for the Autosubmit diagram creator class."""

import datetime

import pytest
from matplotlib.patches import Rectangle

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.monitor.diagram import (
    JobData, JobAggData, build_legends, create_bar_diagram, create_csv_stats, create_stats_report, populate_statistics,
    _get_status, _filter_by_status, _seq
)

_EXPID = 't001'


def test_job_data():
    """Function to test the Class JobData inside autosubmit/monitor/diagram.py"""
    job_data = JobData()

    assert job_data.headers() == ['Job Name', 'Queue Time', 'Run Time', 'Status']
    assert job_data.values() == ['', datetime.timedelta(0), datetime.timedelta(0), '']
    assert job_data.number_of_columns() == 4


def test_job_agg_data():
    """Function to test the Class JobAggData inside autosubmit/monitor/diagram.py"""
    job_agg = JobAggData()
    assert job_agg.headers() == ['Section', 'Count', 'Queue Sum', 'Avg Queue', 'Run Sum', 'Avg Run']
    assert job_agg.values() == [{}, 0, datetime.timedelta(0), datetime.timedelta(0),
                                datetime.timedelta(0), datetime.timedelta(0)]
    assert job_agg.number_of_columns() == 6


def test_seq():
    """Function to test the Class JobData inside autosubmit/monitor/diagram.py"""
    seq = [x for x in _seq(100, 2, 10)]
    assert len(seq) == 9
    assert all([x for x in seq if isinstance(x, int)])


@pytest.mark.parametrize("create_jobs", [[5, 20]], indirect=True)
def test_populate_statistics(create_jobs):
    """Function to test the Class JobData inside autosubmit/monitor/diagram.py"""

    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(10.10)
    queue_time_fixes = {'test': 5, 'test1': 50, 'test2': 500, 'test3': 5000, 'test4': 50000}

    statistics = populate_statistics(create_jobs, date_ini, date_fin, queue_time_fixes)
    for job_stat in statistics.jobs_stat:
        assert ('example_name_' in job_stat.name and
                'example_member_' in job_stat.member)
    assert len(statistics._jobs) == 5
    assert len(statistics.summary.get_as_list()) == 13
    assert statistics.failed_jobs_dict == {'example_name_0': 1, 'example_name_1': 1,
                                           'example_name_2': 1, 'example_name_3': 1,
                                           'example_name_4': 1}


@pytest.mark.parametrize("create_jobs", [[5, 20]], indirect=True)
def test_create_stats_report(create_jobs, tmp_path, mocker):
    """Function to test the function create_stats_report inside autosubmit/monitor/diagram.py"""

    period_ini = datetime.datetime.now()
    period_fi = period_ini + datetime.timedelta(10)
    tmp_path_pdf = tmp_path / "report.pdf"
    tmp_path_csv = tmp_path / "report.csv"

    mocker.patch('autosubmit.monitor.diagram._create_table')
    create_stats_report(
        expid=_EXPID,
        jobs_list=create_jobs,
        output_file=str(tmp_path_pdf),
        section_summary=True,
        jobs_summary=True,
        period_ini=period_ini,
        period_fi=period_fi,
        queue_fix_times={'test': 1, 'test1': 5, 'test2': 50, 'test3': 500, 'test4': 5000})
    assert tmp_path.exists()
    assert tmp_path_pdf.exists()
    assert tmp_path_csv.exists()


def test_create_csv_stats(tmpdir):
    """Function to test the Function create_csv_stats inside autosubmit/monitor/diagram.py"""

    jobs_data = [
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "FAILED", 10)
    ]

    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(0.10)
    queue_time_fixes = {
        'test': 5
    }

    statistics = populate_statistics(jobs_data, date_ini, date_fin, queue_time_fixes)
    file_tmpdir = tmpdir + '.pdf'
    create_csv_stats(statistics, jobs_data, str(file_tmpdir))

    tmpdir += '.csv'
    assert tmpdir.exists()


def test_build_legends(mocker):
    """Function to test the function build_legends inside autosubmit/monitor/diagram.py"""

    jobs_data = [
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "FAILED", 10)
    ]
    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(0.10)
    queue_time_fixes = {'test': 5}

    statistics = populate_statistics(jobs_data, date_ini, date_fin, queue_time_fixes)
    react = [[Rectangle((0.0, 0.0), 0, 0)], [None], [Rectangle((0.0, 0.0), 0, 0)]]
    plot = mocker.Mock()

    number_of_legends = build_legends(plot, react, statistics)
    assert plot.legend.call_count == number_of_legends


@pytest.mark.parametrize(
    'job_stats,failed_jobs,failed_jobs_dict,num_plots,result',
    [
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
                False
        ),
    ],
    ids=[
        'all run',
        'divided by zero',
        'run with continue',
        'fail job_dict',
        'no run'
    ]
)
def test_create_bar_diagram(job_stats, failed_jobs, failed_jobs_dict, num_plots, result, mocker):
    """Function to test the function create_bar_diagram inside autosubmit/monitor/diagram.py"""

    jobs_data = [
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "COMPLETED", 200),
        Job('test', _EXPID, "FAILED", 10)
    ]
    date_ini = datetime.datetime.now()
    date_fin = date_ini + datetime.timedelta(0.10)

    queue_time_fixes = {
        'test': 5
    }

    statistics = populate_statistics(jobs_data, date_ini, date_fin, queue_time_fixes)
    statistics.jobs_stat = job_stats
    statistics.failed_jobs = failed_jobs
    statistics.failed_jobs_dict = failed_jobs_dict

    mocker.patch('autosubmit.monitor.diagram.MAX_NUM_PLOTS', num_plots)
    assert result == create_bar_diagram(_EXPID, statistics, jobs_data)


def test_populate_statistics_error_returns_none(mocker):
    """Previously, the function had no ``return None``. This verifies that."""
    mocked_log = mocker.patch('autosubmit.monitor.diagram.Log')
    mocker.patch('autosubmit.monitor.diagram.Statistics', side_effect=ValueError)
    r = populate_statistics([], datetime.datetime.now(), datetime.datetime.now(), {})
    assert r is None
    assert mocked_log.warning.call_count == 1


def test_create_stats_report_empty_jobs(tmp_path):
    """Test that an experiment with no jobs (not created, not executed) returns no report."""
    assert not create_stats_report(_EXPID, jobs_list=[], output_file=str(tmp_path / 'file.txt'),
                                   section_summary=False, jobs_summary=False)


@pytest.mark.parametrize(
    'jobs,job_name,status,expected',
    [
        ([], '', Status.UNKNOWN, None),
        ([Job('dummy')], 'street', Status.UNKNOWN, None),
        ([Job('dummy')], 'dummy', Status.UNKNOWN, 'UNKNOWN'),
    ],
    ids=[
        'no jobs, return None',
        'jobs, but names do not match',
        'jobs, and names match'
    ]
)
def test_get_status(jobs: list[Job], job_name: str, status: int, expected: str):
    """Previously, the function had no ``return None``. This verifies that."""
    for job in jobs:
        job.status = status
    if expected is None:
        assert _get_status(jobs, job_name) is expected
    else:
        assert _get_status(jobs, job_name) == expected


@pytest.mark.parametrize(
    'jobs,expected_length',
    [
        ([], 0),
        ([Job('dummy', 1, Status.UNKNOWN)], 0),
        ([Job('dummy', 1, Status.UNKNOWN), Job('dummy', 1, Status.COMPLETED)], 1),
        ([Job('dummy', 1, Status.UNKNOWN), Job('dummy', 1, Status.RUNNING)], 1),
        ([Job('dummy', 1, Status.COMPLETED), Job('dummy', 1, Status.RUNNING)], 2),
    ],
    ids=[
        'no jobs, empty list',
        'jobs, but none completed or running',
        'jobs, one completed',
        'jobs, one running',
        'jobs, one completed and one running'
    ]
)
def test_filter_by_status(jobs: list[Job], expected_length: int):
    """Test that jobs are filtered by status (only completed and running)."""
    assert len(_filter_by_status(jobs)) == expected_length
