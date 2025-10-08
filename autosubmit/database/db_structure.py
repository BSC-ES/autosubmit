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


"""Code to manage the structure of tables.

It uses the ``db_manager`` code to manage the database.
"""
import traceback
from pathlib import Path
from typing import Optional

from networkx import DiGraph

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentStructureTable
from autosubmit.log.log import Log


def _check_structures_path(db_path: Path):
    if BasicConfig.DATABASE_BACKEND == 'sqlite' and db_path and not db_path.exists():
        raise ValueError(f'Structures folder not found {str(db_path)}!')


def _get_db_manager(expid: str, sqlite_db_file: Optional[Path]) -> DbManager:
    """Create a ``db_manager`` with the given parameters."""
    connection_url = get_connection_url(db_path=sqlite_db_file)
    if BasicConfig.DATABASE_BACKEND == "postgres":
        _schema = expid
    else:
        _schema = None
    return DbManager(connection_url=connection_url, schema=_schema)


def get_structure(expid: str, structures_path: Path) -> Optional[dict[str, list[str]]]:
    """Return the current structure for the experiment identified by the given ``expid``.

    If the database used is SQLite, the structure database file will be created.
    However, if the SQLIte database file parent directory does not exist, it will
    raise an error instead.

    For Postgres or other database systems, it will simply create the table if it
    does not exist yet.

    :param expid: The experiment identifier.
    :param structures_path: The path to the database structure file (only used for SQLite).
    :return: The experiment graph structure (from=>to) or ``None`` if there is no
        structure persisted in the database.
    """
    try:
        _check_structures_path(structures_path)
        db_manager = _get_db_manager(expid, None if not structures_path else structures_path / f"structure_{expid}.db")

        db_manager.create_table(ExperimentStructureTable.name)

        current_structure = db_manager.select_all('experiment_structure')

        current_table_structure = {}
        for item in current_structure:
            _from, _to = item
            current_table_structure.setdefault(_from, []).append(_to)
            current_table_structure.setdefault(_to, [])

        return current_table_structure
    except Exception as exp:
        Log.printlog("Get structure error: {0}".format(str(exp)), 6014)
        Log.debug(traceback.format_exc())
    return None


def save_structure(graph: DiGraph, expid: str, structures_path: Optional[Path]):
    """Save the experiment structure into the database."""
    _check_structures_path(structures_path)
    db_manager = _get_db_manager(expid, structures_path / f"structure_{expid}.db")

    # Create table if it doesn't exist
    db_manager.create_table(ExperimentStructureTable.name)

    # Delete all rows in the table
    db_manager.delete_all(ExperimentStructureTable.name)

    # Save structure
    nodes_edges = {u for u, v in graph.edges()}
    nodes_edges.update({v for u, v in graph.edges()})
    independent_nodes = {
        u for u in graph.nodes() if u not in nodes_edges}
    data = {(u, v) for u, v in graph.edges()}
    data.update({(u, u) for u in independent_nodes})
    # save
    edges = [{"e_from": e[0], "e_to": e[1]} for e in data]
    db_manager.insert_many(ExperimentStructureTable.name, edges)
