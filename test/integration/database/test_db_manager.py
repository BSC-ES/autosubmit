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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import DBVersionTable

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def _create_db_manager(db_path: Path):
    connection_url = get_connection_url(db_path=db_path)
    return DbManager(connection_url=connection_url)


def test_db_manager_has_made_correct_initialization(tmp_path: "LocalPath") -> None:
    db_manager = _create_db_manager(Path(tmp_path, f'{__name__}.db'))
    assert db_manager.engine.name.startswith('sqlite')


@pytest.mark.docker
@pytest.mark.postgres
def test_after_create_table_command_then_it_returns_1_row(tmp_path: "LocalPath", as_db: str):
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(DBVersionTable.name)
    count = db_manager.count(DBVersionTable.name)
    assert 1 == count


@pytest.mark.docker
@pytest.mark.postgres
def test_after_3_inserts_into_a_table_then_it_has_4_rows(tmp_path: "LocalPath", as_db: str):
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(DBVersionTable.name)
    # It already has the first version, so we are adding versions 2, 3, 4...
    for i in range(2, 5):
        db_manager.insert(DBVersionTable.name, {'version': str(i)})
    count = db_manager.count(DBVersionTable.name)
    assert 4 == count


@pytest.mark.docker
@pytest.mark.postgres
def test_select_first_where(tmp_path: "LocalPath", as_db: str):
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(DBVersionTable.name)
    # It already has the first version, so we are adding versions 2, 3, 4...
    for i in range(2, 5):
        db_manager.insert(DBVersionTable.name, {'version': str(i)})
    first_value = db_manager.select_first_where(DBVersionTable.name, where=None)
    # We are getting the first version, that was already in the database
    assert first_value[0] == 1
    
    last_value = db_manager.select_first_where(DBVersionTable.name, where={'version': '4'})
    assert last_value[0] == 4
