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


@pytest.fixture(scope="function")
def as_exp(autosubmit_exp, general_data, experiment_data, jobs_data):
    config_data = general_data | experiment_data | jobs_data
    return autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)


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
