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

from uuid import uuid4

import pytest
from sqlalchemy import MetaData, select, text
from sqlalchemy.schema import CreateSchema

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import (
    delete_experiment,
    get_autosubmit_version,
    get_experiment_description,
    last_name_used,
    save_experiment,
    update_experiment_description_version,
)
from autosubmit.database.models import tables
from autosubmit.database.schema_version import (
    ensure_schema_version_table,
    record_schema_version,
    schema_version_table,
)
from autosubmit.database.session import get_engine
from autosubmit.log.log import AutosubmitCritical

VERSION_TABLE_NAME = "schema_version_it"


@pytest.mark.docker
@pytest.mark.postgres
def test_get_autosubmit_version(as_db: str) -> None:
    """``get_autosubmit_version`` returns the stored Autosubmit version."""
    save_experiment("test_experiment", "description", "4.0.0")

    assert get_autosubmit_version("test_experiment") == "4.0.0"


@pytest.mark.docker
@pytest.mark.postgres
def test_last_name_used(as_db: str) -> None:
    """``last_name_used`` filters by experiment prefix."""
    save_experiment("oexp1", "description", "4.0.0")
    save_experiment("texp2", "description", "4.0.0")
    save_experiment("eexp3", "description", "4.0.0")
    save_experiment("xp4", "description", "4.0.0")

    assert last_name_used(operational=True) == "oexp1"
    assert last_name_used(test=True) == "texp2"
    assert last_name_used(evaluation=True) == "eexp3"
    assert last_name_used() == "xp4"


@pytest.mark.docker
@pytest.mark.postgres
def test_last_name_used_skips_numeric_first_char(as_db: str) -> None:
    """A last experiment whose name starts with a digit is reported as empty."""
    save_experiment("9exp", "description", "4.0.0")

    assert last_name_used() == "empty"


@pytest.mark.docker
@pytest.mark.postgres
def test_update_experiment_description_version(as_db: str) -> None:
    """Updating the description and version is reflected in the database."""
    save_experiment("test_exp", "Initial description", "1.0.0")

    assert (
        update_experiment_description_version(
            "test_exp", description="Updated description", version="2.0.0"
        )
        is True
    )

    assert get_autosubmit_version("test_exp") == "2.0.0"
    assert get_experiment_description("test_exp") == [["Updated description"]]


@pytest.mark.docker
@pytest.mark.postgres
def test_update_experiment_description_version_no_params(as_db: str) -> None:
    """Updating without description or version raises."""
    save_experiment("test_exp", "Initial description", "1.0.0")

    with pytest.raises(AutosubmitCritical):
        update_experiment_description_version("test_exp")


@pytest.mark.docker
@pytest.mark.postgres
def test_update_experiment_description_version_nonexistent(as_db: str) -> None:
    """Updating a nonexistent experiment raises."""
    with pytest.raises(AutosubmitCritical):
        update_experiment_description_version(
            "nonexistent_exp", description="New description"
        )


@pytest.mark.docker
@pytest.mark.postgres
def test_record_schema_version_is_idempotent(as_db: str) -> None:
    """Recording the same version in separate transactions keeps a single row."""
    engine = get_engine(db_path=BasicConfig.DB_PATH)
    table = schema_version_table(MetaData(), VERSION_TABLE_NAME)
    ensure_schema_version_table(engine, table)

    for _ in range(3):
        with engine.begin() as conn:
            record_schema_version(conn, table, 1)

    with engine.connect() as conn:
        rows = conn.execute(select(table.c.version)).fetchall()
    assert rows == [(1,)]


@pytest.mark.docker
@pytest.mark.postgres
@pytest.mark.parametrize(
    "status_table_exists",
    [True, False],
    ids=["with-experiment-status", "without-experiment-status"],
)
def test_delete_experiment(status_table_exists: bool, as_db: str) -> None:
    """Deleting an experiment removes its schema, with or without experiment status.

    SQLite doesn't use schemas like Postgres, so those tests are skipped (the
    ``as_db`` creates tests for both backends).
    """
    experiment_id = str(uuid4())

    engine = get_engine(db_path=BasicConfig.DB_PATH)

    if engine.dialect.name != "postgresql":
        pytest.skip("PostgreSQL-specific schema deletion test")

    save_experiment(experiment_id, "description", "4.0.0")

    with engine.begin() as conn:
        # Create the experiment-specific PostgreSQL schema.
        conn.execute(CreateSchema(experiment_id))

        # Ensure the experiment status table exists (or doesn't).
        if status_table_exists:
            tables.ExperimentStatusTable.create(conn, checkfirst=True)
        else:
            tables.ExperimentStatusTable.drop(conn, checkfirst=True)

        schema_exists = conn.execute(
            text(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name = :schema_name)"
            ),
            {"schema_name": experiment_id},
        ).scalar()

        assert schema_exists is True

    assert delete_experiment(experiment_id) is True

    with engine.connect() as conn:
        schema_exists = conn.execute(
            text(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name = :schema_name)"
            ),
            {"schema_name": experiment_id},
        ).scalar()

    assert schema_exists is False
