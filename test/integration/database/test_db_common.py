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

"""Integration tests for ``autosubmit.database.db_common``."""

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
from autosubmit.database.schema_version import (
    ensure_schema_migrations_table,
    record_schema_migration,
    schema_migrations_table,
)
from autosubmit.database.session import get_engine
from autosubmit.log.log import AutosubmitCritical

MIGRATIONS_TABLE_NAME = "schema_migrations_it"


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
def test_record_schema_migration_is_idempotent(as_db: str) -> None:
    """Recording the same version in separate transactions keeps a single row."""
    engine = get_engine(db_path=BasicConfig.DB_PATH)
    table = schema_migrations_table(MetaData(), MIGRATIONS_TABLE_NAME)
    ensure_schema_migrations_table(engine, table)

    for _ in range(3):
        with engine.begin() as conn:
            record_schema_migration(conn, table, 1)

    with engine.connect() as conn:
        rows = conn.execute(select(table.c.version)).fetchall()
    assert rows == [(1,)]
