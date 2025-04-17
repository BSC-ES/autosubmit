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

"""Integration tests for Autosubmit ``DbManager``."""

import pytest
from typing import cast, TYPE_CHECKING

from autosubmit.database.db_manager import DatabaseManager, DbManager, SqlAlchemyDbManager
from autosubmit.database.tables import DBVersionTable

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def _create_db_manager(root_path: "LocalPath", engine: str) -> DatabaseManager:
    if engine == 'sqlite':
        return cast(DatabaseManager, DbManager(str(root_path), 'test-db', 1))
    return cast(DatabaseManager, SqlAlchemyDbManager(schema=''))


def test_db_manager_has_made_correct_initialization(tmp_path: "LocalPath", as_db_sqlite) -> None:
    # TODO: Only SQLite has ``db_name`` and ``db_version``, is that correct?
    db_manager = cast(DbManager, _create_db_manager(tmp_path, 'sqlite'))
    name = db_manager.select_first_where('db_options', ['option_name="name"'])[1]
    version = db_manager.select_first_where('db_options', ['option_name="version"'])[1]
    assert db_manager.db_name == name
    assert db_manager.db_version == int(version)


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres, pytest.mark.docker]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_after_create_table_command_then_it_returns_0_rows(tmp_path: "LocalPath", db_engine: str, request):
    request.getfixturevalue(f"as_db_{db_engine}")
    db_manager = _create_db_manager(tmp_path, db_engine)
    db_manager.create_table(DBVersionTable.name, ['version'])
    count = db_manager.count(DBVersionTable.name)
    assert 0 == count


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_after_3_inserts_into_a_table_then_it_has_3_rows(
        tmp_path: "LocalPath", db_engine: str, request):
    request.getfixturevalue(f"as_db_{db_engine}")
    db_manager = _create_db_manager(tmp_path, db_engine)
    columns = ['version']
    db_manager.create_table(DBVersionTable.name, columns)
    for i in range(3):
        db_manager.insert(DBVersionTable.name, columns, [str(i)])
    count = db_manager.count(DBVersionTable.name)
    assert 3 == count
