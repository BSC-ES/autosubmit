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

import pytest

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
    """Test that if the experiment deletion fails, the experiment is not deleted."""
    exp = autosubmit_exp(experiment_data={})
    mocker.patch('autosubmit.experiment.experiment_common._delete_experiment', side_effect=ValueError)

    assert not delete_experiment(exp.expid, force=True)


def test_delete_experiment_that_is_running(autosubmit_exp, mocker):
    """Test that if the experiment is running, the experiment is not deleted."""
    exp = autosubmit_exp(experiment_data={})
    mocker.patch('autosubmit.experiment.experiment_common.process_id', return_value=True)

    assert not delete_experiment(exp.expid, force=True)


@pytest.mark.parametrize("update_metadata", [True, False], ids=["metadata update succeeds", "metadata update fails"])
def test_delete_experiment_removes_directory_metadata_update(
    autosubmit_exp, mocker, update_metadata
):
    """Test that the experiment directory is removed after deletion and logs are displayed."""
    exp = autosubmit_exp(experiment_data={})
    mocked_log = mocker.patch("autosubmit.experiment.experiment_common.Log")
    # We can update the metadata successfully, so db_common.delete_experiment should be called without exceptions
    if update_metadata:
        mocked_delete_metadata = mocker.patch(
            "autosubmit.experiment.experiment_common.db_common.delete_experiment"
        )
    # We simulate a failure updating the metadata, so db_common.delete_experiment should be called but raise an exception
    else:
        mocked_delete_metadata = mocker.patch(
            "autosubmit.experiment.experiment_common.db_common.delete_experiment",
            side_effect=Exception("metadata update failed"),
        )

    # Check that the experiment directory is correctly removed
    assert exp.exp_path.exists()
    result = delete_experiment(exp.expid, force=True)
    assert not exp.exp_path.exists()
    mocked_log.info.assert_any_call("Removing experiment directory...")
    mocked_log.info.assert_any_call("Updating experiment status in sqlite database...")

    # Check that metadata update is attempted in both cases
    if update_metadata:
        assert result
        mocked_delete_metadata.assert_called_once_with(exp.expid)
        mocked_log.info.assert_any_call(f"Experiment {exp.expid} has been deleted")
        mocked_log.result.assert_any_call(
            f"Experiment {exp.expid} marked as deleted in database"
        )
    else:
        assert not result
        mocked_delete_metadata.assert_called_once_with(exp.expid)
        mocked_log.error.assert_called()
