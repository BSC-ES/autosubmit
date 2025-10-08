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

"""Module containing functions to manage autosubmit's database."""

import multiprocessing
import os
import sqlite3
from contextlib import suppress
from pathlib import Path
from typing import List, Optional, cast

from autosubmit.config.basicconfig import BasicConfig
from sqlalchemy import delete, select, Connection, insert, text, update, func
from sqlalchemy.schema import CreateTable

from autosubmit.database import tables, session
from autosubmit.log.log import Log, AutosubmitCritical

Log.get_logger("Autosubmit")


CURRENT_DATABASE_VERSION = 1
TIMEOUT = 15


def create_db(qry):
    """
    Creates a new database for autosubmit

    :param qry: query to create the new database
    :type qry: str    """
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        return _create_db_pg()

    try:
        (conn, cursor) = open_conn(False)
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to database", 7001, str(e))

    try:
        cursor.executescript(qry)
    except sqlite3.Error as e:
        close_conn(conn, cursor)
        raise AutosubmitCritical('Database can not be created', 7004, str(e))

    conn.commit()
    close_conn(conn, cursor)
    return True


def check_db() -> None:
    """Checks if database file exist.

    :return: None if exists, terminates program if not
    """
    if not Path(BasicConfig.DB_PATH).exists():
        raise AutosubmitCritical(f'DB path does not exist: {BasicConfig.DB_PATH}', 7003)


def open_conn(check_version=True):
    """
    Opens a connection to database

    :param check_version: If true, check if the database is compatible with this autosubmit version
    :type check_version: bool
    :return: connection object, cursor object
    :rtype: sqlite3.Connection, sqlite3.Cursor
    """
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        raise AutosubmitCritical('For Postgres databases, connections must be open and managed with SQLAlchemy!')

    conn = sqlite3.connect(BasicConfig.DB_PATH)
    cursor = conn.cursor()

    # Getting database version
    if check_version:
        try:
            cursor.execute('SELECT version FROM db_version;')
            row = cursor.fetchone()
            version = row[0]
        except sqlite3.OperationalError:
            # If this exception is thrown it's because db_version does not exist.
            # Database is from 2.x or 3.0 beta releases
            try:
                cursor.execute('SELECT type FROM experiment;')
                # If type field exists, it's from 2.x
                version = -1
            except sqlite3.Error:
                # If raises and error , it's from 3.0 beta releases
                version = 0

        # If database version is not the expected, update database....
        if version < CURRENT_DATABASE_VERSION:
            if not _update_database(version, cursor):
                raise AutosubmitCritical('Database version does not match', 7001)

        # ... or ask for autosubmit upgrade
        elif version > CURRENT_DATABASE_VERSION:
            raise AutosubmitCritical('Database version is not compatible with this autosubmit version. '
                                     'Please execute pip install autosubmit --upgrade', 7002)
    return conn, cursor


def close_conn(conn, cursor):
    """
    Commits changes and close connection to database

    :param conn: connection to close
    :type conn: sqlite3.Connection
    :param cursor: cursor to close
    :type cursor: sqlite3.Cursor
    """
    conn.commit()
    cursor.close()
    conn.close()


def fn_wrapper(database_fn, queue, *args):
    # TODO: We can also implement the anti-lock mechanism as function decorators in a next iteration.
    result = database_fn(*args)
    queue.put(result)
    queue.close()


def save_experiment(name: str, description: Optional[str], version: Optional[str]):
    """
    Stores experiment in database. Anti-lock version.  

    :param version:
    :type version: str
    :param name: experiment's name
    :type name: str
    :param description: experiment's description
    :type description: str    
    """
    fn = _save_experiment
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _save_experiment_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(fn, queue, name, description, version))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Your experiment {name} couldn't be stored in the database.")
    finally:
        proc.terminate()
    return result


def check_experiment_exists(name, error_on_inexistence=True):
    """ 
    Checks if exist an experiment with the given name. Anti-lock version.  

    :param error_on_inexistence: if True, adds an error log if experiment does not exist
    :type error_on_inexistence: bool
    :param name: Experiment name
    :type name: str
    :return: If experiment exists returns true, if not returns false
    :rtype: bool
    """
    fn = _check_experiment_exists
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _check_experiment_exists_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(fn, queue, name, error_on_inexistence))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Check if experiment {name} exists failed to complete.")
    finally:
        proc.terminate()
    return result


def update_experiment_description_version(name, description=None, version=None):
    """
    Updates the experiment's description and/or version. Anti-lock version.  

    :param name: experiment name (expid)  
    :rtype name: str  
    :param description: experiment new description  
    :rtype description: str  
    :param version: experiment autosubmit version  
    :rtype version: str  
    :return: If description has been update, True; otherwise, False.  
    :rtype: bool
    """
    fn = _update_experiment_description_version
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _update_experiment_description_version_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(fn, queue, name, description, version))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Update experiment {name} version failed to complete.")
    finally:
        proc.terminate()
    return result


def get_autosubmit_version(expid):
    """
    Get the minimum autosubmit version needed for the experiment. Anti-lock version.

    :param expid: Experiment name
    :type expid: str
    :return: If experiment exists returns the autosubmit version for it, if not returns None
    :rtype: str
    """    
    fn = _get_autosubmit_version
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _get_autosubmit_version_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(fn, queue, expid))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Get experiment {expid} version failed to complete.")
    finally:
        proc.terminate()
    return result


def last_name_used(test=False, operational=False, evaluation=False):
    """
    Gets last experiment identifier used. Anti-lock version.  

    :param test: flag for test experiments
    :type test: bool
    :param operational: flag for operational experiments
    :type test: bool
    :param evaluation: flag for evaluation experiments
    :type test: bool
    :return: last experiment identifier used, 'empty' if there is none
    :rtype: str
    """
    fn = _last_name_used
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _last_name_used_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(fn, queue, test, operational, evaluation))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception as e:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Get last named used failed to complete.", 7000, str(e))
    finally:
        proc.terminate()
    return result


def delete_experiment(experiment_id):
    """
    Removes experiment from database. Anti-lock version.  

    :param experiment_id: experiment identifier
    :type experiment_id: str
    :return: True if delete is successful
    :rtype: bool
    """
    fn = _delete_experiment
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _delete_experiment_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(target=fn_wrapper, args=(fn, queue, experiment_id))
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Delete experiment {experiment_id} failed to complete.")
    finally:
        proc.terminate()
    return result


def get_experiment_id(name: str) -> int:
    """
    Gets the experiment numerical id from the database. Anti-lock version.

    :param name: experiment name
    :return: experiment numerical id
    """
    fn = _get_experiment_id
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        fn = _get_experiment_id_sqlalchemy

    queue = multiprocessing.Queue(1)
    proc = multiprocessing.Process(
        target=fn_wrapper, args=(fn, queue, name)
    )
    proc.start()

    try:
        result = queue.get(True, TIMEOUT)
    except Exception:
        raise AutosubmitCritical(f"The database process exceeded the timeout limit {TIMEOUT}s. "
                                 f"Get experiment {name} ID failed to complete.")
    finally:
        proc.terminate()
    return result


def _save_experiment(name, description, version):
    """
    Stores experiment in database

    :param version:
    :type version: str
    :param name: experiment's name
    :type name: str
    :param description: experiment's description
    :type description: str
    """
    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to database", 7001, str(e))
    try:
        cursor.execute('INSERT INTO experiment (name, description, autosubmit_version) '
                       'VALUES (:name, :description, :version)',
                       {'name': name, 'description': description, 'version': version})
    except sqlite3.IntegrityError as e:
        close_conn(conn, cursor)
        raise AutosubmitCritical('Could not register experiment', 7005, str(e))

    conn.commit()
    close_conn(conn, cursor)
    return True


def _check_experiment_exists(name, error_on_inexistence=True):
    """
    Checks if exist an experiment with the given name.

    :param error_on_inexistence: if True, adds an error log if experiment does not exist
    :type error_on_inexistence: bool
    :param name: Experiment name
    :type name: str
    :return: If experiment exists returns true, if not returns false
    :rtype: bool
    """
    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to database", 7001, str(e))
    conn.isolation_level = None

    # SQLite always return a unicode object, but we can change this
    # behaviour with the next sentence
    conn.text_factory = str
    cursor.execute('select name from experiment where name=:name', {'name': name})
    row = cursor.fetchone()
    close_conn(conn, cursor)
    if row is None:
        if error_on_inexistence:
            raise AutosubmitCritical(f'The experiment "{name}" does not exist yet!!!', 7005)
        if os.path.exists(os.path.join(BasicConfig.LOCAL_ROOT_DIR, name)):
            with suppress(Exception):
                _save_experiment(name, 'No description', "3.14.0")
            return True
        return False
    return True


def get_experiment_description(expid: str):
    if BasicConfig.DATABASE_BACKEND == 'postgres':
        return _get_experiment_description_sqlalchemy(expid)

    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to the database.", 7001, str(e))
    conn.isolation_level = None

    # Changing default unicode
    conn.text_factory = str
    # get values
    cursor.execute(f"select description from experiment where name='{expid}'")
    return [row for row in cursor]


def _update_experiment_description_version(name, description=None, version=None):
    """
    Updates the experiment's description and/or version

    :param name: experiment name (expid)  
    :rtype name: str  
    :param description: experiment new description  
    :rtype description: str  
    :param version: experiment autosubmit version  
    :rtype version: str  
    :return: If description has been update, True; otherwise, False.  
    :rtype: bool
    """
    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to the database.", 7001, str(e))
    conn.isolation_level = None

    # Changing default unicode
    conn.text_factory = str
    # Conditional update
    if description is not None and version is not None:
        cursor.execute('update experiment set description=:description, autosubmit_version=:version where name=:name',
                       {'description': description, 'version': version, 'name': name})
    elif description is not None and version is None:
        cursor.execute('update experiment set description=:description where name=:name',
                       {'description': description, 'name': name})
    elif version is not None and description is None:
        cursor.execute('update experiment set autosubmit_version=:version where name=:name',
                       {'version': version, 'name': name})
    else:
        raise AutosubmitCritical(f"Not enough data to update {name}.", 7005)
    row = cursor.rowcount
    close_conn(conn, cursor)
    if row == 0:
        raise AutosubmitCritical(f"Update on experiment {name} failed.", 7005)
    return True


def _get_autosubmit_version(expid):
    """
    Get the minimum autosubmit version needed for the experiment

    :param expid: Experiment name
    :type expid: str
    :return: If experiment exists returns the autosubmit version for it, if not returns None
    :rtype: str
    """
    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to database", 7001, str(e))
    conn.isolation_level = None

    # SQLite always return a unicode object, but we can change this
    # behaviour with the next sentence
    conn.text_factory = str
    cursor.execute('SELECT autosubmit_version FROM experiment WHERE name=:expid', {
                   'expid': expid})
    row = cursor.fetchone()
    close_conn(conn, cursor)
    if row is None:
        raise AutosubmitCritical(f'The experiment "{expid}" does not exist', 7005)
    return row[0]


def _last_name_used(test=False, operational=False, evaluation=False):
    """
    Gets last experiment identifier used

    :param test: flag for test experiments
    :type test: bool
    :param operational: flag for operational experiments
    :type test: bool
    :param evaluation: flag for evaluation experiments
    :type test: bool
    :return: last experiment identifier used, 'empty' if there is none
    :rtype: str
    """    
    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical(
            "Could not establish a connection to database", 7001, str(e))
    conn.text_factory = str
    if test:
        cursor.execute('SELECT name '
                       'FROM experiment '
                       'WHERE rowid=(SELECT max(rowid) FROM experiment WHERE name LIKE "t%" AND '
                       'autosubmit_version IS NOT NULL AND '
                       'NOT (autosubmit_version LIKE "%3.0.0b%"))')
    elif operational:
        cursor.execute('SELECT name '
                       'FROM experiment '
                       'WHERE rowid=(SELECT max(rowid) FROM experiment WHERE name LIKE "o%" AND '
                       'autosubmit_version IS NOT NULL AND '
                       'NOT (autosubmit_version LIKE "%3.0.0b%"))')
    elif evaluation:
        cursor.execute('SELECT name '
                       'FROM experiment '
                       'WHERE rowid=(SELECT max(rowid) FROM experiment WHERE name LIKE "e%" AND '
                       'autosubmit_version IS NOT NULL AND '
                       'NOT (autosubmit_version LIKE "%3.0.0b%"))')
    else:
        cursor.execute('SELECT name '
                       'FROM experiment '
                       'WHERE rowid=(SELECT max(rowid) FROM experiment WHERE name NOT LIKE "t%" AND '
                       'name NOT LIKE "o%" AND name NOT LIKE "e%" AND autosubmit_version IS NOT NULL AND '
                       'NOT (autosubmit_version LIKE "%3.0.0b%"))')
    row = cursor.fetchone()
    close_conn(conn, cursor)
    if row is None:
        return 'empty'

    # If starts by number (during 3.0 beta some jobs starting with numbers where created), returns empty.
    if row[0][0].isnumeric():
        return 'empty'
    return row[0]


def _delete_experiment(experiment_id):
    """
    Removes experiment from database

    :param experiment_id: experiment identifier
    :type experiment_id: str
    :return: True if delete is successful
    :rtype: bool
    """
    check_db()

    if not _check_experiment_exists(experiment_id, False):  # Reference the no anti-lock version.
        return True
    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to database", 7001, str(e))
    cursor.execute('DELETE FROM experiment '
                   'WHERE name=:name', {'name': experiment_id})
    row = cursor.fetchone()
    if row is None:
        Log.debug(f'The experiment {experiment_id} has been deleted!!!')
    close_conn(conn, cursor)
    return True


def _update_database(version, cursor):
    Log.info("Autosubmit's database version is {0}. Current version is {1}. Updating...",
             version, CURRENT_DATABASE_VERSION)
    try:
        # For databases from Autosubmit 2
        if version <= -1:
            cursor.executescript('CREATE TABLE experiment_backup(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, '
                                 'name VARCHAR NOT NULL, type VARCHAR, autosubmit_version VARCHAR, '
                                 'description VARCHAR NOT NULL, model_branch VARCHAR, template_name VARCHAR, '
                                 'template_branch VARCHAR, ocean_diagnostics_branch VARCHAR);'
                                 'INSERT INTO experiment_backup (name,type,description,model_branch,template_name,'
                                 'template_branch,ocean_diagnostics_branch) SELECT name,type,description,model_branch,'
                                 'template_name,template_branch,ocean_diagnostics_branch FROM experiment;'
                                 'UPDATE experiment_backup SET autosubmit_version = "2";'
                                 'DROP TABLE experiment;'
                                 'ALTER TABLE experiment_backup RENAME TO experiment;')
        if version <= 0:
            # Autosubmit beta version. Create db_version table
            cursor.executescript(
                'CREATE TABLE db_version(version INTEGER NOT NULL);'
                'INSERT INTO db_version (version) VALUES (1);'
                'ALTER TABLE experiment ADD COLUMN autosubmit_version VARCHAR;'
                'UPDATE experiment SET autosubmit_version = "3.0.0b" WHERE autosubmit_version NOT NULL;'
            )
        cursor.execute(f'UPDATE db_version SET version={CURRENT_DATABASE_VERSION};')
    except sqlite3.Error as e:
        raise AutosubmitCritical('unable to update database version', 7001, str(e))
    Log.info("Update completed")
    return True


def _get_experiment_id(name: str) -> int:
    """
    Gets the experiment id from the database

    :param name: experiment name
    :return: experiment numerical id
    """
    check_db()

    try:
        (conn, cursor) = open_conn()
    except DbException as e:
        raise AutosubmitCritical("Could not establish a connection to database", 7001, str(e))
    conn.isolation_level = None

    conn.text_factory = str
    cursor.execute("SELECT id FROM experiment WHERE name=:name", {"name": name})
    row = cursor.fetchone()
    close_conn(conn, cursor)
    if row is None:
        raise AutosubmitCritical(f'The experiment "{name}" does not exist', 7005)
    return row[0]


class DbException(Exception):
    """
    Exception class for database errors
    """

    def __init__(self, message):
        self.message = message


# Code added for SQLAlchemy support


def _get_sqlalchemy_conn() -> Connection:
    """Return the database connection.
    It captures any exception, returning an ``AutosubmitCritical``
    as in the previous SQLite-only code. With this function we
    can use a context-manager and keep the previous behaviour
    intact.
    """
    return session.create_engine(BasicConfig.DATABASE_CONN_URL).connect()


def _create_db_pg() -> bool:
    """Create the Postgres tables (not really the database).

    This function is the equivalent to the old ``create_db`` function
    for SQLite, with the difference that that function has a parameter
    with the query.

    However, at the moment of writing, the only use of that function is
    to execute the contents of ``autosubmit.sql``, which creates the
    ``experiment`` and the ``db_version`` tables.

    This whole module was not well-designed and is up for a refactoring
    at some point in the future, where both codes might be superseded by
    a better version.

    :raise AutosubmitCritical: If there are any issues with the database.
    """
    tables_to_create = [
        tables.ExperimentTable,
        tables.DBVersionTable,
        tables.DetailsTable
    ]

    try:
        with _get_sqlalchemy_conn() as conn:
            for table in tables_to_create:
                conn.execute(CreateTable(table, if_not_exists=True))
            conn.execute(delete(tables.DBVersionTable))
            conn.execute(insert(tables.DBVersionTable).values({"version": 1}))
            conn.commit()
    except Exception as exc:
        raise AutosubmitCritical(f"Database can not be created: {str(exc)}", 7004, str(exc))

    return True


def _save_experiment_sqlalchemy(name: str, description: str, version: str) -> bool:
    with _get_sqlalchemy_conn() as conn:
        try:
            conn.execute(
                insert(tables.ExperimentTable).values(
                    name=name, description=description, autosubmit_version=version
                )
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise AutosubmitCritical("Could not register experiment", 7005, str(exc))
    return True


def _check_experiment_exists_sqlalchemy(name: str, error_on_inexistence=True) -> bool:
    row = None
    with _get_sqlalchemy_conn() as conn:
        query = select(tables.ExperimentTable).where(
            tables.ExperimentTable.c.name == name  # type: ignore
        )
        row = conn.execute(query).one_or_none()

    if row is None:
        if error_on_inexistence:
            raise AutosubmitCritical(
                'The experiment name "{0}" does not exist yet!!!'.format(name), 7005
            )
        # FIXME: what if this is issued from another server/VM?
        if Path(BasicConfig.LOCAL_ROOT_DIR, name).exists():
            with suppress(Exception):
                save_experiment(name, "No description", "3.14.0")
            return True
        return False

    return True


def _get_experiment_description_sqlalchemy(expid) -> List[List[str]]:
    with _get_sqlalchemy_conn() as conn:
        query = select(tables.ExperimentTable).where(
            tables.ExperimentTable.c.name == expid  # type: ignore
        )
        row = conn.execute(query).one_or_none()

    if row:
        return [[row.description]]
    return []


def _update_experiment_description_version_sqlalchemy(
    name: str, description: Optional[str] = None, version: Optional[str] = None
) -> bool:
    # Conditional update statement
    if description is None and version is None:
        raise AutosubmitCritical(f"Not enough data to update {name}.", 7005)

    query = update(tables.ExperimentTable).where(tables.ExperimentTable.c.name == name)  # type: ignore
    vals = {}
    if isinstance(description, str):
        vals["description"] = description
    if isinstance(version, str):
        vals["autosubmit_version"] = version
    query = query.values(vals)

    with _get_sqlalchemy_conn() as conn:
        result = conn.execute(query)
        conn.commit()

    if result.rowcount == 0:
        raise AutosubmitCritical(f"Update on experiment {name} failed.", 7005)
    return True


def _get_autosubmit_version_sqlalchemy(expid) -> str:
    with _get_sqlalchemy_conn() as conn:
        query = select(tables.ExperimentTable).where(
            tables.ExperimentTable.c.name == expid  # type: ignore
        )
        row = conn.execute(query).one_or_none()

    if row is None:
        raise AutosubmitCritical(f'The experiment "{expid}" does not exist', 7005)
    return row.autosubmit_version


def _last_name_used_sqlalchemy(test=False, operational=False, evaluation=False) -> str:
    if test:
        condition = tables.ExperimentTable.c.name.like("t%")
    elif operational:
        condition = tables.ExperimentTable.c.name.like("o%")
    elif evaluation:
        condition = tables.ExperimentTable.c.name.like("e%")
    else:
        condition = tables.ExperimentTable.c.name.not_like(
            "t%"
        ) & tables.ExperimentTable.c.name.not_like("o%")

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
        tables.ExperimentTable.c.id == sub_query  # type: ignore
    )

    with _get_sqlalchemy_conn() as conn:
        row = conn.execute(query).one_or_none()

    if row is None:
        return "empty"

    # If starts by number (during 3.0 beta some jobs starting with numbers where created), returns empty.
    if row.name.isnumeric():
        return "empty"
    return row.name


def _delete_experiment_sqlalchemy(experiment_id: str) -> bool:
    if not _check_experiment_exists_sqlalchemy(
        experiment_id, False
    ):  # Reference the no anti-lock version.
        return True

    with _get_sqlalchemy_conn() as conn:
        # Delete from experiment table
        query = delete(tables.ExperimentTable).where(
            tables.ExperimentTable.c.name == experiment_id  # type: ignore
        )
        result = conn.execute(query)
        conn.commit()

        # Drop schema
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{experiment_id}" CASCADE'))
        conn.commit()

        # Delete from experiment_status table
        try:
            query = delete(tables.ExperimentStatusTable).where(
                tables.ExperimentStatusTable.c.name == experiment_id  # type: ignore
            )
            conn.execute(query)
            conn.commit()
        except Exception as e:
            Log.debug(f"The experiment {experiment_id} has no status: {str(e)}")

        if cast(int, result.rowcount) > 0:
            Log.debug(f"The experiment {experiment_id} has been deleted!!!")
        return True


def _get_experiment_id_sqlalchemy(name: str) -> int:
    query = select(tables.ExperimentTable.c.id).where(
        tables.ExperimentTable.c.name == name  # type: ignore
    )

    with _get_sqlalchemy_conn() as conn:
        row = conn.execute(query).one_or_none()

    if not row:
        raise AutosubmitCritical(f'The experiment "{name}" does not exist', 7005)

    return int(row[0])


def get_connection_url(db_path: Optional['Path'] = None) -> str:
    """Return a SQLAlchemy connection URL."""
    if BasicConfig.DATABASE_BACKEND == "postgres":
        return BasicConfig.DATABASE_CONN_URL

    if not db_path:
        raise ValueError('For SQLite databases you MUST provide a database file.')

    return f'sqlite:///{str(Path(db_path).resolve())}'
