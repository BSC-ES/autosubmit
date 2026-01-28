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

"""Integration tests for ``experiment_common.py``."""

from autosubmit.experiment.experiment_common import delete_experiment


def test_delete_experiment_cancelled(autosubmit_exp, mocker):
    """Test that a user cancellation results in no experiment deleted."""
    exp1 = autosubmit_exp(experiment_data={})
    exp2 = autosubmit_exp(experiment_data={})

    mocker.patch('autosubmit.experiment.experiment_common.user_yes_no_query', side_effect=[False, True])
    mocked_log = mocker.patch('autosubmit.experiment.experiment_common.Log')
    delete_experiment(f'{exp1.expid},{exp2.expid}', force=False)

    assert exp1.exp_path.exists()
    assert mocked_log.info.call_count > 0
    assert mocked_log.info.call_args_list[0][:-1][0][0] == f'Experiment {exp1.expid} deletion cancelled by user'

    assert not exp2.exp_path.exists()


def test_delete_experiment_failed(autosubmit_exp, mocker):
    exp = autosubmit_exp(experiment_data={})
    mocker.patch('autosubmit.experiment.experiment_common._delete_experiment', side_effect=ValueError)

    assert not delete_experiment(exp.expid, force=True)


def test_delete_experiment_that_is_running(autosubmit_exp, mocker):
    exp = autosubmit_exp(experiment_data={})
    mocker.patch('autosubmit.experiment.experiment_common.process_id', return_value=True)

    assert not delete_experiment(exp.expid, force=True)


def test_delete_experiment_fails_db_details(autosubmit_exp, mocker):
    exp = autosubmit_exp(experiment_data={})
    mocker.patch('autosubmit.experiment.experiment_common.ExperimentDetails', side_effect=ValueError)

    assert not delete_experiment(exp.expid, force=True)
