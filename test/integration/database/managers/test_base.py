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

"""Integration tests for the base ``DbManager``."""
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.database.managers.base import DbManager
from autosubmit.database.models.tables import ExperimentTable

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def _create_db_manager(db_path: Path) -> DbManager:
    return DbManager(db_path=str(db_path))


def test_db_manager_has_made_correct_initialization(tmp_path: "LocalPath") -> None:
    db_manager = _create_db_manager(Path(tmp_path, f'{__name__}.db'))
    assert db_manager.engine is not None
    assert db_manager.engine.name.startswith('sqlite')


@pytest.mark.docker
@pytest.mark.postgres
def test_after_create_table_then_it_is_empty(tmp_path: "LocalPath", as_db: str) -> None:
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(ExperimentTable.name)
    assert 0 == db_manager.count(ExperimentTable.name)


@pytest.mark.docker
@pytest.mark.postgres
def test_after_3_inserts_into_a_table_then_it_has_3_rows(tmp_path: "LocalPath", as_db: str) -> None:
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(ExperimentTable.name)
    for i in range(1, 4):
        db_manager.insert(
            ExperimentTable.name,
            {'name': f'exp{i}', 'description': f'description {i}', 'autosubmit_version': '4.0.0'},
        )
    assert 3 == db_manager.count(ExperimentTable.name)


@pytest.mark.docker
@pytest.mark.postgres
def test_select_first_where(tmp_path: "LocalPath", as_db: str) -> None:
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(ExperimentTable.name)
    for i in range(1, 5):
        db_manager.insert(
            ExperimentTable.name,
            {'name': f'exp{i}', 'description': f'description {i}', 'autosubmit_version': '4.0.0'},
        )

    first_value = db_manager.select_first_where(ExperimentTable.name, where=None)
    assert first_value is not None
    assert first_value[1] == 'exp1'

    last_value = db_manager.select_first_where(ExperimentTable.name, where={'name': 'exp4'})
    assert last_value is not None
    assert last_value[1] == 'exp4'


def test_delete_experiment_db(monkeypatch, tmp_path) -> None:
    """Deleting an experiment removes the row from the database."""
    db_path = tmp_path / "tests.db"
    db_manager = _create_db_manager(db_path)
    db_manager.create_table(ExperimentTable.name)
    db_manager.insert(
        ExperimentTable.name,
        {'name': 'test_experiment', 'description': 'description', 'autosubmit_version': '4.0.0'},
    )
    assert 1 == db_manager.count(ExperimentTable.name)

    db_manager.delete_where(ExperimentTable.name, {'name': 'test_experiment'})

    assert 0 == db_manager.count(ExperimentTable.name)


@pytest.mark.postgres
def test_invalid_dialect(monkeypatch, tmp_path) -> None:
    """``upsert_many`` rejects unknown dialects."""
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(ExperimentTable.name)

    assert db_manager.engine is not None
    db_manager.engine.dialect.name = 'other_db'
    with pytest.raises(ValueError):
        db_manager.upsert_many(
            ExperimentTable.name,
            [
                {'name': 'exp1', 'description': 'First experiment', 'autosubmit_version': '4.0.0'},
                {'name': 'exp2', 'description': 'Second experiment', 'autosubmit_version': '4.0.0'},
            ],
            ['id'],
        )


@pytest.mark.parametrize(
    "where_type",
    [
        "simple",
        "in",
    ],
)
def test_update_where(monkeypatch, tmp_path, where_type: str) -> None:
    """``update_where`` supports simple and ``IN`` filters."""
    db_path = tmp_path / "tests.db"
    db_manager = _create_db_manager(db_path)
    db_manager.create_table(ExperimentTable.name)
    db_manager.insert(
        ExperimentTable.name,
        {'name': 'test_experiment', 'description': 'description', 'autosubmit_version': '4.0.0'},
    )

    if where_type == "simple":
        db_manager.update_where(
            ExperimentTable.name,
            {'description': 'Updated description'},
            {'name': 'test_experiment'},
        )
    elif where_type == "in":
        db_manager.update_where(
            ExperimentTable.name,
            {'description': 'Updated description'},
            {'name': ['test_experiment']},
        )

    result = db_manager.select_first_where(ExperimentTable.name, where={'name': 'test_experiment'})
    assert result is not None
    assert result[2] == 'Updated description'
