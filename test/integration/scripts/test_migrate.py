# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Tests for migrating AS experiments."""

from autosubmit.scripts.autosubmit import main


def test_migrate_offer(autosubmit_exp, mocker):
    """Temporary test for AS migrate."""
    # TODO: Write the new test once we have the code working again (maybe not here, maybe not
    #       using ``autosubmit migrate`` directly).
    exp = autosubmit_exp(experiment_data={})

    mocked_log = mocker.patch('autosubmit.scripts.autosubmit.Log')

    mocker.patch('sys.argv', ['autosubmit', 'migrate', exp.expid, '-o'])
    main()

    mocked_log.critical.call_count > 0

    assert any(
        'The command migrate was removed' in log_output
        for log_output in mocked_log.critical.call_args_list[0][0]
    )
