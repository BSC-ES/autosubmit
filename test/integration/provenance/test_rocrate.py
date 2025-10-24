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

"""Integration tests for the RO-Crate generation in Autosubmit."""

import datetime
import json
from pathlib import Path

from autosubmit.job.job import Job

_EXPID = 'zzzz'
"""Experiment ID used in all the tests."""


def test_rocrate_main(
        autosubmit_exp,
        tmp_path: Path,
        mocker
):
    local_proj = tmp_path / 'project'
    local_proj.mkdir(exist_ok=True)
    # some outputs
    for output_file in ['graph_1.png', 'graph_2.gif', 'graph_3.gif', 'graph.jpg']:
        Path(local_proj, output_file).touch()
    # required paths for AS
    for other_required_path in ['conf', 'pkl', 'plot', 'status']:
        Path(local_proj, other_required_path).mkdir()

    as_exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_DESTINATION': '',
            'PROJECT_TYPE': 'LOCAL'
        },
        'LOCAL': {
            'PROJECT_PATH': str(local_proj)
        },
        'APP': {
            'INPUT_1': 1,
            'INPUT_2': 2
        },
        'ROCRATE': {
            'INPUTS': ['APP'],
            'OUTPUTS': [
                'graph_*.gif'
            ],
            'PATCH': json.dumps({
                '@graph': [
                    {
                        '@id': './',
                        "license": "Apache-2.0"
                    }
                ]
            })
        }
    })

    job1 = mocker.Mock(autospec=Job)
    job1_submit_time = datetime.datetime.strptime("21/11/06 16:30", "%d/%m/%y %H:%M")
    job1_start_time = datetime.datetime.strptime("21/11/06 16:40", "%d/%m/%y %H:%M")
    job1_finished_time = datetime.datetime.strptime("21/11/06 16:50", "%d/%m/%y %H:%M")
    job1.get_last_retrials.return_value = [
        [job1_submit_time, job1_start_time, job1_finished_time, 'COMPLETED']]
    job1.name = 'job1'
    job1.date = '2006'
    job1.member = 'fc0'
    job1.section = 'JOB'
    job1.chunk = '1'
    job1.processors = '1'

    job2 = mocker.Mock(autospec=Job)
    job2_submit_time = datetime.datetime.strptime("21/11/06 16:40", "%d/%m/%y %H:%M")
    job2_start_time = datetime.datetime.strptime("21/11/06 16:50", "%d/%m/%y %H:%M")
    job2_finished_time = datetime.datetime.strptime("21/11/06 17:00", "%d/%m/%y %H:%M")
    job2.get_last_retrials.return_value = [
        [job2_submit_time, job2_start_time, job2_finished_time, 'COMPLETED']]
    job2.name = 'job2'
    job2.date = '2006'
    job2.member = 'fc1'
    job2.section = 'JOB'
    job2.chunk = '1'
    job2.processors = '1'

    mocked_job_list = mocker.Mock()
    mocked_job_list.get_job_list.return_value = [job1, job2]
    mocked_job_list.get_ready.return_value = []  # Mock due the new addition in the job_list.load()
    mocked_job_list.get_waiting.return_value = []  # Mocked due the new addition in the job_list.load()
    mocker.patch('autosubmit.autosubmit.JobList', return_value=mocked_job_list)

    project_path = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, as_exp.as_conf.basic_config.LOCAL_PROJ_DIR)
    r = as_exp.autosubmit.rocrate(_EXPID, path=project_path)
    assert r
