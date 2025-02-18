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

import signal
from itertools import chain
from typing import Optional, List

import pytest

from log.log import AutosubmitCritical

"""This file contains tests for the ``autosubmit stop`` command."""


def test_invalid_target_status(autosubmit):
    """Test when the target status is invalid."""
    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.stop('a000', status='banana')

    assert 'Invalid status' in str(cm.value.message)


@pytest.mark.parametrize(
    'current_status',
    [
        'banana',
        'waiting2',
        '1'
    ]
)
def test_invalid_current_status(autosubmit, current_status):
    """Test when the current status is invalid."""
    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.stop('a000', current_status=current_status)

    assert 'Invalid status -fs' in str(cm.value.message)


def test_pid_not_found(autosubmit, mocker):
    """Test when ``process_id`` returns ``None``."""
    mocked_process_id = mocker.patch('autosubmit.helpers.processes.process_id')
    mocked_process_id.return_value = None

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    autosubmit.stop('a000', current_status='waiting', force_all=True)

    assert mocked_log.info.call_count == 1
    assert 'was not running' in mocked_log.info.call_args_list[0][0][0]


@pytest.mark.parametrize('pid', [-1, 0, 1])
def test_ignored_pids(pid, autosubmit, mocker):
    """Test that we do not kill pids lower than init(1)."""
    mocked_process_id = mocker.patch('autosubmit.helpers.processes.process_id')
    mocked_process_id.return_value = pid

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    autosubmit.stop('a000', current_status='waiting', force_all=True)

    assert mocked_log.info.call_count == 1
    assert 'was not running' in mocked_log.info.call_args_list[0][0][0]


def test_os_kill_fails(autosubmit, mocker):
    """Test that if ``os.kill`` fails the code aborts."""
    mocked_process_id = mocker.patch('autosubmit.helpers.processes.process_id')
    mocked_process_id.return_value = 1984

    mocked_kill = mocker.patch('os.kill', side_effect=OSError('chough'))

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    autosubmit.stop('a000', current_status='waiting', force=True, force_all=True)

    kill_signal = mocked_kill.call_args_list[0][0][1]
    assert kill_signal == signal.SIGKILL

    assert mocked_log.warning.call_count == 1
    assert 'An error occurred while stopping the autosubmit process' in mocked_log.warning.call_args_list[0][0][0]


def test_stop_expids_no_cancel(autosubmit, mocker):
    """Test that we can kill an expid without job cancellations."""
    expids = 'a000'
    cancel = False

    mocked_process_id = mocker.patch('autosubmit.helpers.processes.process_id')
    mocked_process_id.side_effect = [42, None]

    mocker.patch(
        'autosubmit.autosubmit.Autosubmit.prepare_run',
        return_value=[None, None, None, None, None, None, None, None]
    )
    mocked_cancel_jobs = mocker.patch('autosubmit.job.job_utils.cancel_jobs')

    mocked_kill = mocker.patch('os.kill')
    mocked_sleep = mocker.patch('autosubmit.autosubmit.sleep')

    autosubmit.stop(
        expids,
        force=True,
        all_expids=False,
        force_all=True,
        cancel=cancel,
        current_status="RUNNING",
        status="FAILED")

    kill_signal = mocked_kill.call_args_list[0][0][1]
    assert kill_signal == signal.SIGKILL

    assert mocked_kill.call_args_list[0][0][0] > 1

    assert mocked_cancel_jobs.call_count == 0
    assert mocked_sleep.call_count == 0


@pytest.mark.parametrize(
    'expids,user_input,expected_killed',
    [
        ['a000', ["y"], 1],
        ['a000 a001', ["y", "True"], 2],
        ['t001, o001', ["Y", "n"], 1],
        ['t001, o001', ["N", "1"], 0]
    ]
)
def test_stop_expids_force_all(autosubmit, mocker, expids: str, user_input: List[str], expected_killed: int):
    """Test that we ask the user for input before stopping experiments."""
    force_all = False

    mocked_process_id = mocker.patch('autosubmit.helpers.processes.process_id')
    mocked_process_id.side_effect = [42 for _ in range(len(expids))] + [None for _ in range(len(expids))]

    mocked_kill = mocker.patch('os.kill')

    mocked_input = mocker.patch('autosubmit.autosubmit.input')
    mocked_input.side_effect = user_input

    autosubmit.stop(
        expids,
        force=True,
        all_expids=False,
        force_all=force_all,
        cancel=False,
        current_status="RUNNING",
        status="FAILED")

    mocked_input.call_count == len(expids)

    kill_signal = mocked_kill.call_args_list[0][0][1]
    assert kill_signal == signal.SIGKILL

    assert mocked_kill.call_args_list[0][0][0] > 1


@pytest.mark.parametrize(
    'expids,num_expids,cancel,sleep',
    [
        ['a000', 1, True, False],
        ['a000 a001', 2, True, True],
        ['a000, zzzz, t030', 3, False, False]
    ],
    ids=[
        'one expid',
        'two expids with spaces',
        'three with commas'
    ]
)
def test_stop_expids(autosubmit, mocker, expids: str, num_expids: int, cancel: bool, sleep: bool):
    """Test that we can successfully stop by expids"""
    # We test here with ``all_expids`` (i.e. return from ``ps`` output) and without (i.e. from cmd line args).
    for all_expids in [False, True]:
        if all_expids:
            list_of_expids = expids.replace(',', ' ').split(' ')
            list_of_expids = [expid.lower() for expid in filter(lambda x: x, list_of_expids)]
            mocked_retrieve_expids = mocker.patch('autosubmit.helpers.processes.retrieve_expids')
            mocked_retrieve_expids.return_value = list_of_expids

        # Start at PID=2, then increase in the mocked side effect.
        pid = 2

        mocked_process_id = mocker.patch('autosubmit.helpers.processes.process_id')
        # Return the PIDs, then return as many empty values as PIDs. That's because for each
        # PID returned, it will check if it's still running after the ``os.kill`` call
        # (as we passed ``force=False``), so we avoid the ``sleep`` and loop by returning
        # a ``None``, which is interpreted as if the process was correctly killed by
        # ``os.kill``.
        returned_pids: List[Optional[int]] = [pid + n for n in range(num_expids)]
        if sleep:
            # To test that we ``sleep`` once, we return not just the list followed by
            # as many ``None``s as expids (see comment above), but instead of return
            # the list, followed by one PID and then a ``None``. This results in ``sleep``
            # being called once for each expid.
            returned_pids.extend(chain.from_iterable(zip(returned_pids, [None for _ in range(num_expids)])))
            # And then we mock ``sleep`` because it does not make sense to sleep in this test.
            mocked_sleep = mocker.patch('autosubmit.autosubmit.sleep')
        else:
            returned_pids.extend([None for _ in range(num_expids)])

        mocked_process_id.side_effect = returned_pids

        mocker.patch(
            'autosubmit.autosubmit.Autosubmit.prepare_run',
            return_value=[None, None, None, None, None, None, None, None]
        )
        mocked_cancel_jobs = mocker.patch('autosubmit.job.job_utils.cancel_jobs')

        mocked_kill = mocker.patch('os.kill')
        autosubmit.stop(
            expids,
            force=False,
            all_expids=all_expids,
            force_all=True,
            cancel=cancel,
            current_status="RUNNING",
            status="FAILED")

        kill_signal = mocked_kill.call_args_list[0][0][1]
        assert kill_signal == signal.SIGINT

        assert mocked_kill.call_args_list[0][0][0] > 1

        # We call ``cancel_jobs`` for as many experiments as we have.
        assert mocked_cancel_jobs.call_count == (num_expids if cancel else 0)

        if sleep:
            assert mocked_sleep.call_count > 0
