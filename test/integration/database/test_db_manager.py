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

"""Integration tests for Autosubmit ``DbManager`` and ``db_common``."""
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import MetaData, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import (
    get_autosubmit_version,
    get_experiment_description,
    last_name_used,
    save_experiment,
    update_experiment_description_version,
)
from autosubmit.database.db_manager import DbManager
from autosubmit.database.migrations import (
    ensure_schema_migrations_table,
    record_migration,
    schema_migrations_table,
)
from autosubmit.database.session import get_engine
from autosubmit.database.tables import ExperimentTable
from autosubmit.log.log import AutosubmitCritical

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath

MIGRATIONS_TABLE_NAME = "schema_migrations_it"


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


@pytest.mark.docker
@pytest.mark.postgres
def test_get_autosubmit_version(as_db: str) -> None:
    """``get_autosubmit_version`` returns the stored Autosubmit version."""
    save_experiment('test_experiment', 'description', '4.0.0')

    assert get_autosubmit_version('test_experiment') == '4.0.0'


@pytest.mark.docker
@pytest.mark.postgres
def test_last_name_used(as_db: str) -> None:
    """``last_name_used`` filters by experiment prefix."""
    save_experiment('oexp1', 'description', '4.0.0')
    save_experiment('texp2', 'description', '4.0.0')
    save_experiment('eexp3', 'description', '4.0.0')
    save_experiment('xp4', 'description', '4.0.0')

    assert last_name_used(operational=True) == 'oexp1'
    assert last_name_used(test=True) == 'texp2'
    assert last_name_used(evaluation=True) == 'eexp3'
    assert last_name_used() == 'xp4'


@pytest.mark.docker
@pytest.mark.postgres
def test_last_name_used_skips_numeric_first_char(as_db: str) -> None:
    """A last experiment whose name starts with a digit is reported as empty."""
    save_experiment('9exp', 'description', '4.0.0')

    assert last_name_used() == 'empty'


@pytest.mark.docker
@pytest.mark.postgres
def test_update_experiment_description_version(as_db: str) -> None:
    """Updating the description and version is reflected in the database."""
    save_experiment('test_exp', 'Initial description', '1.0.0')

    assert update_experiment_description_version(
        'test_exp', description='Updated description', version='2.0.0'
    ) is True

    assert get_autosubmit_version('test_exp') == '2.0.0'
    assert get_experiment_description('test_exp') == [['Updated description']]


@pytest.mark.docker
@pytest.mark.postgres
def test_update_experiment_description_version_no_params(as_db: str) -> None:
    """Updating without description or version raises."""
    save_experiment('test_exp', 'Initial description', '1.0.0')

    with pytest.raises(AutosubmitCritical):
        update_experiment_description_version('test_exp')


@pytest.mark.docker
@pytest.mark.postgres
def test_update_experiment_description_version_nonexistent(as_db: str) -> None:
    """Updating a nonexistent experiment raises."""
    with pytest.raises(AutosubmitCritical):
        update_experiment_description_version('nonexistent_exp', description='New description')


@pytest.mark.docker
@pytest.mark.postgres
def test_record_migration_is_idempotent(as_db: str) -> None:
    """Recording the same version in separate transactions keeps a single row."""
    engine = get_engine(db_path=BasicConfig.DB_PATH)
    table = schema_migrations_table(MetaData(), MIGRATIONS_TABLE_NAME)
    ensure_schema_migrations_table(engine, table)

    for _ in range(3):
        with engine.begin() as conn:
            record_migration(conn, table, 1)

    with engine.connect() as conn:
        rows = conn.execute(select(table.c.version)).fetchall()
    assert rows == [(1,)]
