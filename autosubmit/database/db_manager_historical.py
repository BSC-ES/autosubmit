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

"""Contains code to manage a database via SQLAlchemy."""
from pathlib import Path
from typing import Optional, TYPE_CHECKING
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager

if TYPE_CHECKING:
    from autosubmit.database.db_manager_job_list import JobsDbManager


class HistoricalDbManager(DbManager):
    """Manage historical job data, delegating job-list queries to a JobsDbManager.

    Provides access to historical job data while reusing job-list operations
    via an injected ``JobsDbManager`` instance.
    """

    def __init__(self, schema: Optional[str] = None, job_manager: Optional["JobsDbManager"] = None) -> None:
        """Initialize with an optional JobsDbManager for job-list delegation.

        :param schema: Optional schema name for the historical database.
        :param job_manager: An initialized JobsDbManager to delegate job-list queries.
        """

        if BasicConfig.DATABASE_BACKEND == 'sqlite':
            historical_persistence_full_path = Path(BasicConfig.JOBDATA_DIR) / Path(f"job_data_{schema}.db")
        else:
            historical_persistence_full_path = None

        super().__init__(get_connection_url(historical_persistence_full_path), schema, True)
        self._job_manager = job_manager

    def load_current_edges(self):
        """Delegate loading edge jobs to the injected JobsDbManager."""
        if self._job_manager is None:
            raise ValueError("JobsDbManager instance is required for loading edge jobs.")
        return self._job_manager.load_edges(full_load=True, remove_unused_edges=False)

    def get_current_run_id(self):
        table_name: str = "experiment_run"
        columns: list[str] = ["run_id"]
        run_id = self.select_last_with_columns(table_name, columns)
        if run_id is None:
            raise ValueError("No run_id found in the experiment_run table.")
        return run_id

    def save_historical_edges(self):
        """Delegate saving edge jobs to the injected JobsDbManager."""
        graph = self.load_current_edges()
        run_id = self.get_current_run_id()
        self._save_historical_edges(graph, run_id)

    def _save_historical_edges(self, graph, run_id):
        """Save edge jobs to the historical database with the associated run_id."""
        table_name = "structure_data"
        edge_data = [
            {
                "run_id": run_id,
                "e_from": edge["e_from"],
                "e_to": edge["e_to"],
                "min_trigger_status": edge.get("min_trigger_status"),
                "completion_status": edge.get("completion_status"),
                "from_step": edge.get("from_step"),
                "fail_ok": edge.get("fail_ok"),
            }
            for edge in graph
        ]
        self.upsert_many(table_name, edge_data, ["run_id", "e_from", "e_to"])
