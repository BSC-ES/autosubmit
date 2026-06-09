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

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.database_managers.experiment_history_db_manager import (
    SqlAlchemyExperimentHistoryDbManager,
)


@pytest.mark.parametrize("noplot", [True, False])
def test_create_noplot_calls_generate_output(as_exp, mocker, noplot):
    """Test that create calls generate_output when noplot is False and does not call it when noplot is True."""
    db_manager = SqlAlchemyExperimentHistoryDbManager(
        as_exp.expid, BasicConfig.JOBDATA_DIR, f"job_data_{as_exp.expid}.db"
    )

    db_manager.initialize()

    mock_generate_output = mocker.patch(
        "autosubmit.monitor.monitor.Monitor.generate_output"
    )

    as_exp.autosubmit.create(
        as_exp.expid,
        noplot=noplot,
        hide=True,
    )

    if noplot:
        mock_generate_output.assert_not_called()
    else:
        mock_generate_output.assert_called_once()


@pytest.mark.parametrize("noclean,uncompress", [
    (True, True),
    (True, False),
    (False, False),
])
def test_archive_and_unarchive(as_exp, mocker, noclean, uncompress):
    as_exp.autosubmit.create(
        as_exp.expid,
        noplot=True,
        hide=True,
    )

    assert as_exp.autosubmit.archive(as_exp.expid, noclean, uncompress)
    assert as_exp.autosubmit.unarchive(as_exp.expid, uncompress)


def test_archive_noncreated_experiment(as_exp):
    assert as_exp.autosubmit.archive(as_exp.expid)


def test_unarchive_nonarchived_experiment(as_exp):
    assert not as_exp.autosubmit.unarchive(as_exp.expid)


def test_create_cw_calls_generate_scripts_andor_wrappers_without_plt(
    autosubmit_exp, general_data, mocker
):
    """create -cw must call generate_scripts_andor_wrappers even without -plt.

    Before the fix the wrapper generation was nested inside ``if not noplot:``
    so ``create -cw`` without ``-plt`` silently skipped preview-table
    population.
    """
    mock_gen = mocker.patch(
        "autosubmit.autosubmit.Autosubmit.generate_scripts_andor_wrappers"
    )

    exp_data = {
        "EXPERIMENT": {
            "DATELIST": "20200101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": 1,
            "NUMCHUNKS": 1,
            "CALENDAR": "standard",
        },
        "JOBS": {
            "SLURMJOB": {
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "WALLCLOCK": "02:00",
                "PLATFORM": "TEST_SLURM",
            },
        },
        "WRAPPERS": {
            "SIMPLE_WRAPPER": {
                "TYPE": "vertical",
                "JOBS_IN_WRAPPER": "SLURMJOB",
                "MAX_WRAPPED_V": 2,
                "MIN_WRAPPED_V": 2,
            },
        },
    }
    config_data = general_data | exp_data

    exp = autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)

    exp.autosubmit.create(
        exp.expid,
        noplot=True,
        hide=True,
        check_wrappers=True,
    )

    mock_gen.assert_called_once()

