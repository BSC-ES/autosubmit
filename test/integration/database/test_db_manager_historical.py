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

"""Integration tests for ``HistoricalDbManager``."""
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_manager_historical import HistoricalDbManager
from autosubmit.database.tables import ExperimentRunTable, StructureDataTable

# Minimal graph edges shared across tests.
_SAMPLE_EDGES: List[Dict[str, Any]] = [
    {
        "e_from": "a001_LOCAL_SETUP",
        "e_to": "a001_REMOTE_SETUP",
        "min_trigger_status": "COMPLETED",
        "completion_status": "WAITING",
        "from_step": 0,
        "fail_ok": True,
    },
    {
        "e_from": "a001_REMOTE_SETUP",
        "e_to": "a001_20000101_fc0_INI",
        "min_trigger_status": "COMPLETED",
        "completion_status": "WAITING",
        "from_step": 0,
        "fail_ok": False,
    },
]


def _make_manager(exp) -> HistoricalDbManager:
    """Create a ``HistoricalDbManager`` backed by the experiment's database.

    :param exp: An experiment created by the ``autosubmit_exp`` fixture.
    :return: An initialised ``HistoricalDbManager`` instance.
    """
    return HistoricalDbManager(schema=exp.expid)


def _insert_run(manager: HistoricalDbManager) -> int:
    """Insert a minimal experiment_run row and return its run_id.

    :param manager: An initialised ``HistoricalDbManager``.
    :return: The run_id of the inserted row.
    """
    manager.create_table(ExperimentRunTable.name)
    run_data = {
        "created": "2026-01-01 00:00:00",
        "modified": "2026-01-01 00:00:00",
        "start": 1000,
        "finish": None,
        "chunk_unit": "month",
        "chunk_size": 1,
        "completed": 0,
        "total": 3,
        "failed": 0,
        "queuing": 0,
        "running": 0,
        "submitted": 0,
        "suspended": 0,
        "metadata": None,
    }
    manager.insert(ExperimentRunTable.name, run_data)
    result = manager.select_first_where(ExperimentRunTable.name, where=None)
    return result[0]  # run_id is the first column (primary key, auto-incremented)


def test_historical_db_manager_uses_sqlite_engine(autosubmit_exp) -> None:
    """HistoricalDbManager must create a SQLite engine when backend is sqlite."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    assert manager.engine_historical is not None
    assert manager.engine_historical.name == "sqlite"


@pytest.mark.parametrize(
    "backend",
    [
        pytest.param("sqlite"),
        pytest.param("postgres", marks=[pytest.mark.docker, pytest.mark.postgres]),
    ],
    ids=["sqlite", "postgres"],
)
def test_historical_db_path_contains_schema(autosubmit_exp, backend: str) -> None:
    """The SQLite database file must be named ``job_data_<schema>.db``."""
    exp = autosubmit_exp()
    schema = exp.expid
    _make_manager(exp)
    expected_db_file = Path(BasicConfig.JOBDATA_DIR) / f"job_data_{schema}.db"
    assert expected_db_file.exists()


def test_historical_db_manager_no_job_manager_by_default(autosubmit_exp) -> None:
    """HistoricalDbManager._job_manager must be None when not provided."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    assert manager._job_manager is None


def test_load_current_edges_raises_without_job_manager(autosubmit_exp) -> None:
    """load_current_edges must raise ValueError when no JobsDbManager is injected."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    with pytest.raises(ValueError, match="JobsDbManager instance is required"):
        manager.load_current_edges()


def test_load_current_edges_delegates_to_job_manager(autosubmit_exp) -> None:
    """load_current_edges must delegate to the injected JobsDbManager."""
    mock_job_manager = MagicMock()
    mock_job_manager.load_edges.return_value = _SAMPLE_EDGES

    exp = autosubmit_exp()
    manager = _make_manager(exp)
    manager._job_manager = mock_job_manager

    result = manager.load_current_edges()

    mock_job_manager.load_edges.assert_called_once_with(full_load=True, remove_unused_edges=False)
    assert result == _SAMPLE_EDGES


def test_get_current_run_id_returns_run_id(autosubmit_exp) -> None:
    """get_current_run_id must return the run_id of the latest experiment_run row."""
    exp = autosubmit_exp(create=False)
    manager = _make_manager(exp)
    expected_run_id = _insert_run(manager)

    run_id = manager.get_current_run_id()

    assert run_id == expected_run_id


def test_get_current_run_id_raises_when_no_rows(autosubmit_exp) -> None:
    """get_current_run_id must raise ValueError when experiment_run table is empty."""
    exp = autosubmit_exp(create=False)
    manager = _make_manager(exp)
    manager.create_table(ExperimentRunTable.name)

    with pytest.raises(ValueError, match="No run_id found"):
        manager.get_current_run_id()


def test_get_current_run_id_returns_latest_run(autosubmit_exp) -> None:
    """get_current_run_id must return the most recently modified run."""
    exp = autosubmit_exp(create=False)
    manager = _make_manager(exp)
    manager.create_table(ExperimentRunTable.name)

    base_row = {
        "start": 1000,
        "finish": None,
        "chunk_unit": "month",
        "chunk_size": 1,
        "completed": 0,
        "total": 3,
        "failed": 0,
        "queuing": 0,
        "running": 0,
        "submitted": 0,
        "suspended": 0,
        "metadata": None,
    }
    manager.insert(ExperimentRunTable.name, {**base_row, "created": "2026-01-01 00:00:00", "modified": "2026-01-01 00:00:00"})
    manager.insert(ExperimentRunTable.name, {**base_row, "created": "2026-01-02 00:00:00", "modified": "2026-01-02 00:00:00"})

    run_id = manager.get_current_run_id()
    assert run_id == 2


def _setup_structure_tables(manager: HistoricalDbManager) -> None:
    """Create experiment_run and structure_data tables for edge persistence tests.

    :param manager: An initialised ``HistoricalDbManager``.
    """
    manager.create_table(ExperimentRunTable.name)
    manager.create_table(StructureDataTable.name)


def test_save_historical_edges_inserts_rows(autosubmit_exp) -> None:
    """_save_historical_edges must persist all edges to structure_data."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    _setup_structure_tables(manager)
    run_id = _insert_run(manager)

    manager._save_historical_edges(_SAMPLE_EDGES, run_id)

    count = manager.count(StructureDataTable.name)
    assert count == len(_SAMPLE_EDGES)


def test_save_historical_edges_stores_correct_values(autosubmit_exp) -> None:
    """_save_historical_edges must store the correct e_from and e_to values."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    _setup_structure_tables(manager)
    run_id = _insert_run(manager)

    manager._save_historical_edges(_SAMPLE_EDGES, run_id)

    row = manager.select_first_where(
        StructureDataTable.name,
        where={"e_from": "a001_LOCAL_SETUP", "e_to": "a001_REMOTE_SETUP"},
    )
    assert row is not None


def test_save_historical_edges_upserts_on_conflict(autosubmit_exp) -> None:
    """_save_historical_edges must update existing rows on conflict, not duplicate."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    _setup_structure_tables(manager)
    run_id = _insert_run(manager)

    manager._save_historical_edges(_SAMPLE_EDGES, run_id)
    modified_edges = [
        {**edge, "fail_ok": not edge.get("fail_ok")} for edge in _SAMPLE_EDGES
    ]
    manager._save_historical_edges(modified_edges, run_id)

    count = manager.count(StructureDataTable.name)
    assert count == len(_SAMPLE_EDGES)


def test_save_historical_edges_handles_optional_fields(autosubmit_exp) -> None:
    """_save_historical_edges must handle edges with missing optional fields."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    _setup_structure_tables(manager)
    run_id = _insert_run(manager)

    minimal_edges = [{"e_from": "a001_LOCAL_SETUP", "e_to": "a001_REMOTE_SETUP"}]
    manager._save_historical_edges(minimal_edges, run_id)

    assert manager.count(StructureDataTable.name) == 1


def test_save_historical_edges_delegates_to_job_manager(autosubmit_exp) -> None:
    """save_historical_edges must load edges via the job manager and persist them."""
    mock_job_manager = MagicMock()
    mock_job_manager.load_edges.return_value = _SAMPLE_EDGES

    exp = autosubmit_exp()
    manager = _make_manager(exp)
    manager._job_manager = mock_job_manager
    _setup_structure_tables(manager)
    _insert_run(manager)

    manager.save_historical_edges()

    mock_job_manager.load_edges.assert_called_once_with(full_load=True, remove_unused_edges=False)
    assert manager.count(StructureDataTable.name) == len(_SAMPLE_EDGES)


def test_save_historical_edges_raises_without_job_manager(autosubmit_exp) -> None:
    """save_historical_edges must raise ValueError when no JobsDbManager is injected."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    _setup_structure_tables(manager)
    _insert_run(manager)

    with pytest.raises(ValueError, match="JobsDbManager instance is required"):
        manager.save_historical_edges()


def test_save_historical_edges_raises_without_run(autosubmit_exp) -> None:
    """save_historical_edges must raise ValueError when experiment_run is empty."""
    mock_job_manager = MagicMock()
    mock_job_manager.load_edges.return_value = _SAMPLE_EDGES

    exp = autosubmit_exp(create=False)
    manager = _make_manager(exp)
    manager._job_manager = mock_job_manager
    manager.create_table(ExperimentRunTable.name)
    manager.create_table(StructureDataTable.name)

    with pytest.raises(ValueError, match="No run_id found"):
        manager.save_historical_edges()


@pytest.mark.parametrize(
    "edges",
    [
        pytest.param([], id="empty_graph"),
        pytest.param(
            [
                {
                    "e_from": "j_A",
                    "e_to": "j_B",
                    "min_trigger_status": "COMPLETED",
                    "completion_status": "WAITING",
                    "from_step": 0,
                    "fail_ok": True,
                }
            ],
            id="single_edge",
        ),
        pytest.param(
            [
                {
                    "e_from": f"j_{i}",
                    "e_to": f"j_{i + 1}",
                    "min_trigger_status": "COMPLETED",
                    "completion_status": "WAITING",
                    "from_step": i,
                    "fail_ok": bool(i % 2),
                }
                for i in range(5)
            ],
            id="multiple_edges",
        ),
    ],
)
def test_save_historical_edges_parametrised(
        autosubmit_exp,
        edges: List[Dict[str, Any]],
) -> None:
    """_save_historical_edges must persist exactly the provided number of edges."""
    exp = autosubmit_exp()
    manager = _make_manager(exp)
    _setup_structure_tables(manager)
    run_id = _insert_run(manager)

    manager._save_historical_edges(edges, run_id)

    assert manager.count(StructureDataTable.name) == len(edges)
