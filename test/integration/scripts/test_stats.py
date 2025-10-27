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

"""Test for the ``autosubmit stats`` command."""

from pathlib import Path
from time import time, sleep

from psutil import Process

from autosubmit.scripts.autosubmit import main

_EXPID = 't000'

def test_autosubmit_commands_help(autosubmit_exp, mocker):
    """Test that the monitor is called for stats with a simple job list.

    It must produce three PNG files. One with the job summary, one with the
    section summary, and one with the general statistics.
    """
    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'SIM': {
                'RUNNING': 'once',
                'PLATFORM': 'local',
                'SCRIPT': 'echo "OK"'
            }
        }
    })

    exp.autosubmit._check_ownership_and_set_last_command(
        exp.as_conf,
        exp.expid,
        'run')

    processes_before_run = Process().children(recursive=True)
    assert 0 == exp.autosubmit.run_experiment(_EXPID)
    processes_after_run = Process().children(recursive=True)

    before = time()
    wait_n_seconds = 30
    while time() - before < wait_n_seconds:
        if len(processes_after_run) <= len(processes_before_run):
            break
        # Children processes of ``autosubmit run`` may still be running, and
        # we need those to finish in this test so that we have all the remote
        # data copied, and ``autosubmit stats`` can run and use those files.
        sleep(1)

    mocker.patch('sys.argv', ['autosubmit', 'stats', '-o', 'png', '--section_summary',
                              '--jobs_summary', '--hide', _EXPID])
    assert 0 == main()

    stats_folder = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, 'stats')
    stats_files = list(stats_folder.iterdir())

    assert len(stats_files) == 3

    assert all(
        [
            stat_file.name.endswith('.png') for stat_file in stats_files
        ]
    )
