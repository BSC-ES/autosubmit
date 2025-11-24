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

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.database_managers.experiment_history_db_manager import (
    create_experiment_history_db_manager, SqlAlchemyExperimentHistoryDbManager
)


def test_create_experiment_history_db_manager_invalid():
    with pytest.raises(ValueError):
        create_experiment_history_db_manager('banana')


def test_functions_not_implemented(mocker):
    """Confirm that we do not implement a few functions for Postgres."""
    mocker.patch('autosubmit.history.database_managers.experiment_history_db_manager.get_connection_url')
    mocker.patch('autosubmit.history.database_managers.experiment_history_db_manager.session')
    db_manager = SqlAlchemyExperimentHistoryDbManager(None, BasicConfig.JOBDATA_DIR)
    # NOTE: These are all parameter-less.
    for fn in [
        'is_header_ready_db_version',
        'is_current_version',
        'update_historical_database'
    ]:
        with pytest.raises(NotImplementedError):
            getattr(db_manager, fn)()
