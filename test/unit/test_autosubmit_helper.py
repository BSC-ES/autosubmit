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

from collections.abc import Callable
from datetime import datetime, timedelta

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
        result: str | Exception,
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
        with pytest.raises(result):
            helper.get_allowed_members(ids, as_config)



@pytest.mark.parametrize(
    'experiment_exists,db_backend,db_file_exists,status_counts',
    [
        (False, 'sqlite', False, {}),
        (True, 'postgresql', False, [
            {"COMPLETED": 2, "FAILED": 0, "QUEUING": 3, "SUBMITTED": 0, "RUNNING": 0, "SUSPENDED": 0, "TOTAL": 5},
            {"COMPLETED": 5, "FAILED": 0, "QUEUING": 0, "SUBMITTED": 0, "RUNNING": 0, "SUSPENDED": 0, "TOTAL": 5},
        ]),
        (True, 'sqlite', True, [{"COMPLETED": 3, "FAILED": 0, "QUEUING": 0, "SUBMITTED": 0, "RUNNING": 0,
                                 "SUSPENDED": 2, "TOTAL": 5}]),
        (True, 'sqlite', False, {}),
    ],
    ids=[
        'experiment does not exist',
        'polls until completed',
        'completed plus suspended',
        'missing jobs database'
    ]
)
def test_handle_start_after(mocker, capsys, experiment_exists: bool,
                            db_backend: str, db_file_exists: bool, status_counts):
    """Test the function handle_start_after inside autosubmit_helper."""
    mocked_warning = mocker.patch.object(helper.Log, 'warning')
    mocked_critical = mocker.patch.object(helper.Log, 'critical')
    mocker.patch('autosubmit.helpers.autosubmit_helper.check_experiment_exists',
                 return_value=experiment_exists)
    mocked_sleep = mocker.patch('autosubmit.helpers.autosubmit_helper.sleep', return_value=0)
    mocker.patch('autosubmit.helpers.autosubmit_helper.BasicConfig.DATABASE_BACKEND', db_backend)
    mocker.patch('autosubmit.helpers.autosubmit_helper.Path.exists', return_value=db_file_exists)

    mocked_jobs_db = mocker.Mock()
    if isinstance(status_counts, list):
        mocked_jobs_db.get_job_status_counts.side_effect = status_counts
    else:
        mocked_jobs_db.get_job_status_counts.return_value = status_counts
    mocker.patch('autosubmit.helpers.autosubmit_helper.JobsDbManager', return_value=mocked_jobs_db)

    helper.handle_start_after(_EXPID, _EXPID)

    if not experiment_exists:
        mocked_jobs_db.get_job_status_counts.assert_not_called()
        mocked_warning.assert_called_once_with(
            f"Experiment {_EXPID} does not exist. Ignoring the start_after trigger.")
    elif db_backend == 'sqlite' and not db_file_exists:
        mocked_jobs_db.get_job_status_counts.assert_not_called()
        mocked_critical.assert_called_once()
        assert "has no jobs database" in mocked_critical.call_args[0][0]
    else:
        assert mocked_jobs_db.get_job_status_counts.called
        first_poll = status_counts[0] if isinstance(status_counts, list) else status_counts
        first_done = first_poll["COMPLETED"] + first_poll["SUSPENDED"]
        if first_poll["TOTAL"] > first_done:
            assert mocked_sleep.called
            out = capsys.readouterr().out
            assert f"({first_poll['TOTAL']} total jobs" in out
            assert f"{first_done / first_poll['TOTAL'] * 100:.1f}% completed" in out
