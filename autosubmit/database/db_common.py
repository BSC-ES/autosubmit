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

"""Module containing functions to manage autosubmit's general database.

All access goes through SQLAlchemy Core, so the same code works with both the
SQLite and the PostgreSQL backends.
"""

import os
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, func, insert, select, text, update
from sqlalchemy.schema import CreateTable

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session, tables
from autosubmit.database.db_utils import batch_size_for, chunked
from autosubmit.database.migrations import record_migration, schema_migrations_table
from autosubmit.log.log import AutosubmitCritical, Log

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.sql.elements import ColumnElement


CURRENT_DATABASE_VERSION = 1

# The general database records its schema version in ``schema_migrations``.
_SCHEMA_MIGRATIONS_TABLE = schema_migrations_table(
    tables.metadata_obj, name="general_schema_migrations"
)


def _get_engine(*, create: bool = False) -> "Engine":
    """Return the general database engine.

    :param create: When ``True``, do not require the database to exist yet (used by ``create_db``).
    :return: A SQLAlchemy engine for the general database.
    :raises AutosubmitCritical: If the SQLite database file does not exist.
    """
    if not create:
        check_db()
    return session.get_engine(db_path=Path(BasicConfig.DB_PATH))


def check_db() -> None:
    """Check that the general database is available.

    Only SQLite is checked, as its absence means the file was not created and
    SQLAlchemy would silently create an empty one. PostgreSQL surfaces a missing
    database or tables as a query error.

    :raises AutosubmitCritical: If the SQLite database file does not exist.
    """
    if BasicConfig.DATABASE_BACKEND == "sqlite" and not Path(BasicConfig.DB_PATH).exists():
        raise AutosubmitCritical(f'DB path does not exist: {BasicConfig.DB_PATH}', 7003)


def create_db() -> bool:
    """Create the general database tables and record the schema version.

    :return: ``True`` if the database was created successfully.
    :raises AutosubmitCritical: If the database could not be created.
    """
    tables_to_create = [
        tables.ExperimentTable,
        tables.DetailsTable,
        _SCHEMA_MIGRATIONS_TABLE,
    ]
    try:
        with _get_engine(create=True).begin() as conn:
            for table in tables_to_create:
                conn.execute(CreateTable(table, if_not_exists=True))
            record_migration(conn, _SCHEMA_MIGRATIONS_TABLE, CURRENT_DATABASE_VERSION)
    except Exception as exc:
        raise AutosubmitCritical(f"Database can not be created: {exc}", 7004, str(exc))
    return True


def save_experiment(name: str, description: str | None, version: str | None) -> bool:
    """Store an experiment in the database.

    :param name: Experiment name.
    :param description: Experiment description.
    :param version: Autosubmit version.
    :return: ``True`` if the experiment was stored.
    :raises AutosubmitCritical: If the experiment could not be stored.
    """
    try:
        with _get_engine().begin() as conn:
            conn.execute(
                insert(tables.ExperimentTable).values(
                    name=name, description=description, autosubmit_version=version
                )
            )
    except Exception as exc:
        raise AutosubmitCritical("Could not register experiment", 7005, str(exc))
    return True


def get_experiment_expids(expids: list[str] | None = None) -> set[str]:
    """Get the expids of all experiments in the database, or only the requested ones.

    :param expids: Optional list of expids to look up. When ``None``, all are returned.
    :return: A set of experiment expids.
    """
    experiment_table = tables.ExperimentTable
    with _get_engine().connect() as conn:
        if expids is None:
            return set(conn.execute(select(experiment_table.c.name)).scalars())

        result: set[str] = set()
        requested = list(expids)
        for batch in chunked(requested, batch_size_for(1, conn.dialect.name)):
            query = select(experiment_table.c.name).where(experiment_table.c.name.in_(batch))
            result.update(conn.execute(query).scalars())
        return result


def check_experiment_exists(name: str, error_on_inexistence: bool = True) -> bool:
    """Check whether an experiment with the given name exists.

    :param name: Experiment name.
    :param error_on_inexistence: When ``True``, raise if the experiment does not exist.
    :return: ``True`` if the experiment exists, ``False`` otherwise.
    :raises AutosubmitCritical: If it does not exist and ``error_on_inexistence`` is set.
    """
    query = select(tables.ExperimentTable.c.id).where(tables.ExperimentTable.c.name == name)
    with _get_engine().connect() as conn:
        exists = conn.execute(query).first() is not None

    if exists:
        return True
    if error_on_inexistence:
        raise AutosubmitCritical(f'The experiment name "{name}" does not exist yet!!!', 7005)
    # FIXME(#883): the local filesystem check is unreliable when the command runs on another
    #              server/VM (shared database but non-shared filesystem).
    if Path(BasicConfig.LOCAL_ROOT_DIR, name).exists():
        with suppress(Exception):
            save_experiment(name, "No description", "3.14.0")
        return True
    return False


def update_experiment_description_version(
    name: str, description: str | None = None, version: str | None = None
) -> bool:
    """Update an experiment's description and/or version.

    :param name: Experiment name (expid).
    :param description: New experiment description.
    :param version: New Autosubmit version.
    :return: ``True`` if the experiment was updated.
    :raises AutosubmitCritical: If there is not enough data or the update fails.
    """
    if description is None and version is None:
        raise AutosubmitCritical(f"Not enough data to update {name}.", 7005)

    values: dict[str, str] = {}
    if isinstance(description, str):
        values["description"] = description
    if isinstance(version, str):
        values["autosubmit_version"] = version

    query = (
        update(tables.ExperimentTable)
        .where(tables.ExperimentTable.c.name == name)
        .values(values)
    )
    with _get_engine().begin() as conn:
        result = conn.execute(query)

    if result.rowcount == 0:
        raise AutosubmitCritical(f"Update on experiment {name} failed.", 7005)
    return True


def get_autosubmit_version(expid: str) -> str | None:
    """Get the minimum Autosubmit version needed for the experiment.

    :param expid: Experiment name.
    :return: The Autosubmit version for the experiment.
    :raises AutosubmitCritical: If the experiment does not exist.
    """
    query = select(tables.ExperimentTable.c.autosubmit_version).where(
        tables.ExperimentTable.c.name == expid
    )
    with _get_engine().connect() as conn:
        row = conn.execute(query).first()

    if row is None:
        raise AutosubmitCritical(f'The experiment "{expid}" does not exist', 7005)
    return row.autosubmit_version


def last_name_used(test: bool = False, operational: bool = False, evaluation: bool = False) -> str:
    """Get the last experiment identifier used.

    :param test: Flag for test experiments.
    :param operational: Flag for operational experiments.
    :param evaluation: Flag for evaluation experiments.
    :return: The last experiment identifier used, or ``'empty'`` if there is none.
    """
    condition: "ColumnElement[bool]"
    if test:
        condition = tables.ExperimentTable.c.name.like("t%")
    elif operational:
        condition = tables.ExperimentTable.c.name.like("o%")
    elif evaluation:
        condition = tables.ExperimentTable.c.name.like("e%")
    else:
        condition = (
            tables.ExperimentTable.c.name.not_like("t%")
            & tables.ExperimentTable.c.name.not_like("o%")
            & tables.ExperimentTable.c.name.not_like("e%")
        )

    sub_query = (
        select(func.max(tables.ExperimentTable.c.id).label("id"))
        .where(
            condition
            & tables.ExperimentTable.c.autosubmit_version.is_not(None)
            & tables.ExperimentTable.c.autosubmit_version.not_like("%3.0.0b%")
        )
        .scalar_subquery()
    )
    query = select(tables.ExperimentTable.c.name).where(
        tables.ExperimentTable.c.id == sub_query
    )

    with _get_engine().connect() as conn:
        name = conn.execute(query).scalar_one_or_none()

    if name is None or name[:1].isnumeric():
        return "empty"
    return name


def delete_experiment(experiment_id: str) -> bool:
    """Remove an experiment from the database.

    :param experiment_id: Experiment identifier.
    :return: ``True`` if the experiment was deleted.
    """
    if not check_experiment_exists(experiment_id, False):
        return True

    with _get_engine().begin() as conn:
        result = conn.execute(
            delete(tables.ExperimentTable).where(
                tables.ExperimentTable.c.name == experiment_id
            )
        )
        if conn.dialect.name == "postgresql":
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{experiment_id}" CASCADE'))
        try:
            conn.execute(
                delete(tables.ExperimentStatusTable).where(
                    tables.ExperimentStatusTable.c.name == experiment_id
                )
            )
        except Exception as e:
            Log.debug(f"The experiment {experiment_id} has no status: {e}")

        if cast(int, result.rowcount) > 0:
            Log.debug(f"The experiment {experiment_id} has been deleted!!!")
    return True


def get_experiment_id(name: str) -> int:
    """Get the experiment numerical id from the database.

    :param name: Experiment name.
    :return: The experiment numerical id.
    :raises AutosubmitCritical: If the experiment does not exist.
    """
    query = select(tables.ExperimentTable.c.id).where(tables.ExperimentTable.c.name == name)
    with _get_engine().connect() as conn:
        experiment_id = conn.execute(query).scalar_one_or_none()

    if experiment_id is None:
        raise AutosubmitCritical(f'The experiment "{name}" does not exist', 7005)
    return int(experiment_id)


def get_experiment_description(expid: str) -> list[list[str]]:
    """Get an experiment's description.

    :param expid: Experiment name (expid).
    :return: A list with a single list containing the description, or an empty list.
    """
    query = select(tables.ExperimentTable.c.description).where(
        tables.ExperimentTable.c.name == expid
    )
    with _get_engine().connect() as conn:
        description = conn.execute(query).scalar_one_or_none()

    # TODO(#3202): return a plain ``str`` instead of ``list[list[str]]``.
    if description is None:
        return []
    return [[description]]


def database_backup(expid: str) -> None:
    """Back up the experiment's historical database.

    :param expid: Experiment identifier.
    """
    if BasicConfig.DATABASE_BACKEND == "sqlite":
        try:
            database_path = os.path.join(BasicConfig.JOBDATA_DIR, f"job_data_{expid}.db")
            backup_path = os.path.join(BasicConfig.JOBDATA_DIR, f"job_data_{expid}.sql")
            Log.debug("Backing up jobs_data...")
            with open(backup_path, "w") as backup_file:
                subprocess.run(["sqlite3", database_path, ".dump"], stdout=backup_file, check=False)
            Log.debug("Jobs_data database backup completed.")
        except Exception:
            Log.debug("Jobs_data database backup failed.")
    elif BasicConfig.DATABASE_BACKEND == "postgres":
        # TODO(#3179): implement the PostgreSQL backup (see also #3129).
        Log.debug("Postgres database backup not implemented yet.")
