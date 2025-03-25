#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import sqlite3
import os
from typing import Any, Dict, Iterable, List, Protocol, Union, cast, Optional
from sqlalchemy import Connection, Engine, delete, insert, select, text
from autosubmit.database import session
from autosubmit.database.tables import get_table_from_name
from sqlalchemy.schema import CreateTable, CreateSchema,  DropTable

class DbManager(object):
    """
    Class to manage an SQLite database.
    """

    def __init__(self, root_path: str, db_name: str, db_version: int):
        self.root_path = root_path
        self.db_name = db_name
        self.db_version = db_version
        is_new = not os.path.exists(self._get_db_filepath())
        self.connection = sqlite3.connect(self._get_db_filepath())
        if is_new:
            self._initialize_database()
    def backup(self):
        pass
    def restore(self):
        pass

    def disconnect(self):
        """
        Closes the manager connection

        """
        self.connection.close()

    def create_table(self, table_name, fields):
        """
        Creates a new table with the given fields
        :param table_name: str
        :param fields: [str]

        """
        cursor = self.connection.cursor()
        create_command = self.generate_create_table_command(table_name, fields[:])
        cursor.execute(create_command)
        self.connection.commit()

    def drop_table(self, table_name):
        """
        Drops the given table
        :param table_name: str

        """
        cursor = self.connection.cursor()
        drop_command = self.generate_drop_table_command(table_name)
        cursor.execute(drop_command)
        self.connection.commit()

    def insert(self, table_name, columns, values):
        """
        Inserts a new row on the given table
        :param table_name: str
        :param columns: [str]
        :param values: [str]

        """
        cursor = self.connection.cursor()
        insert_command = self.generate_insert_command(table_name, columns[:], values[:])
        cursor.execute(insert_command)
        self.connection.commit()

    def insertMany(self, table_name, data):
        """
        Inserts multiple new rows on the given table
        :param table_name: str
        :param data: [()]

        """
        cursor = self.connection.cursor()
        insert_many_command = self.generate_insert_many_command(table_name, len(data[0]))
        cursor.executemany(insert_many_command, data)
        self.connection.commit()

    def delete_where(self, table_name: str, where: list[str]):
        """
        Deletes the rows of the given table that matches the given where conditions
        :param table_name: str
        :param where: [str]
        """
        cursor = self.connection.cursor()
        delete_command = self.generate_delete_command(table_name, where[:])
        cursor.execute(delete_command)
        self.connection.commit()

    def select_first(self, table_name):
        """
        Returns the first row of the given table
        :param table_name: str
        :return row: []
        """
        cursor = self._select_with_all_fields(table_name)
        return cursor.fetchone()

    def select_first_where(self, table_name, where):
        """
        Returns the first row of the given table that matches the given where conditions
        :param table_name: str
        :param where: [str]
        :return row: []
        """
        cursor = self._select_with_all_fields(table_name, where)
        return cursor.fetchone()

    def select_all(self, table_name):
        """
        Returns all the rows of the given table
        :param table_name: str
        :return rows: [[]]
        """
        cursor = self._select_with_all_fields(table_name)
        return cursor.fetchall()

    def count(self, table_name):
        """
        Returns the number of rows of the given table
        :param table_name: str
        :return int
        """
        cursor = self.connection.cursor()
        count_command = self.generate_count_command(table_name)
        cursor.execute(count_command)
        return cursor.fetchone()[0]

    def drop(self):
        """
        Drops the database (deletes the .db file)

        """
        self.connection.close()
        if os.path.exists(self._get_db_filepath()):
            os.remove(self._get_db_filepath())

    def _get_db_filepath(self) -> str:
        """
        Returns the path of the .db file
        :return path: int

        """
        return os.path.join(self.root_path, self.db_name) + '.db'

    def _initialize_database(self):
        """
        Initialize the database with an option's table
        with the name and the version of the DB

        """
        options_table_name = 'db_options'
        columns = ['option_name', 'option_value']
        self.create_table(options_table_name, columns)
        self.insert(options_table_name, columns, ['name', self.db_name])
        self.insert(options_table_name, columns, ['version', self.db_version])

    def _select_with_all_fields(self, table_name: str, where: List[str] = []) -> sqlite3.Cursor:
        """
        Returns the cursor of the select command with the given parameters
        :param table_name: str
        :param where: [str]
        :return cursor: Cursor
        """
        cursor = self.connection.cursor()
        count_command = self.generate_select_command(table_name, where[:])
        cursor.execute(count_command)
        return cursor

    """
    Static methods that generates the SQLite commands to make the queries
    """

    @staticmethod
    def generate_create_table_command(table_name: str, fields: List[str]) -> str:
        create_command = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' (' + fields.pop(0)
        for field in fields:
            create_command += (', ' + field)
        create_command += ')'
        return create_command

    @staticmethod
    def generate_drop_table_command(table_name: str) -> str:
        drop_command = 'DROP TABLE IF EXISTS ' + table_name
        return drop_command

    @staticmethod
    def generate_insert_command(
        table_name: str, columns: List[str], values: List[str]
    ) -> str:
        insert_command = 'INSERT INTO ' + table_name + '(' + columns.pop(0)
        for column in columns:
            insert_command += (', ' + column)
        insert_command += (') VALUES ("' + str(values.pop(0)) + '"')
        for value in values:
            insert_command += (', "' + str(value) + '"')
        insert_command += ')'
        return insert_command

    @staticmethod
    def generate_insert_many_command(table_name: str, num_of_values: int) -> str:
        insert_command = 'INSERT INTO ' + table_name + ' VALUES (?'
        num_of_values -= 1
        while num_of_values > 0:
            insert_command += ',?'
            num_of_values -= 1
        insert_command += ')'
        return insert_command

    @staticmethod
    def generate_count_command(table_name: str) -> str:
        count_command = 'SELECT count(*) FROM ' + table_name
        return count_command

    @staticmethod
    def generate_select_command(table_name: str, where: List[str] = []) -> str:
        basic_select = 'SELECT * FROM ' + table_name
        select_command = basic_select if len(where) == 0 else basic_select + ' WHERE ' + where.pop(0)
        for condition in where:
            select_command += ' AND ' + condition
        return select_command
    
    @staticmethod
    def generate_delete_command(table_name: str, where: List[str] = []) -> str:
        delete_command = "DELETE FROM " + table_name + " WHERE " + where.pop(0)
        for condition in where:
            delete_command += " AND " + condition
        return delete_command

class DatabaseManager(Protocol):
    """Common interface for database managers.
    We used a protocol here to avoid having to modify the existing
    SQLite code (as we would if we used an abstract/ABC class).
    And the new database manager will "quack" like the other one does.
    """

    connection: Union[sqlite3.Connection]

    def backup(self): ...
    def restore(self): ...
    def disconnect(self): ...
    def create_table(self, table_name: str, fields: List[str]): ...
    def drop_table(self, table_name: str): ...
    def insert(self, table_name: str, columns: List[str], values: List[str]): ...
    def insertMany(self, table_name: str, data: List[Union[Iterable, Dict]]): ...
    def delete_where(self, table_name: str, where: List[str]): ...
    def select_first(self, table_name: str) -> List[Any]: ...
    def select_first_where(self, table_name: str, where: List[str]) -> List[Any]: ...
    def select_all(self, table_name: str) -> List[List[Any]]: ...
    def select_all_where(self, table_name: str, where: List[str]) -> List[List[Any]]: ...
    def count(self, table_name: str) -> int: ...
    def drop(self): ...

class SqlAlchemyDbManager:
    """A database manager using SQLAlchemy.
    It contains the same public functions as ``DbManager``
    (SQLite only), but uses SQLAlchmey instead of calling database
    driver functions directly.
    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    Static functions from ``DbManager`` were skipped, as those are
    similar to what SQLAlchemy already provides for creating and
    queries.
    Some operations here may raise ``NotImplemented``, as they existed in
    the ``DbManager`` but were never used in Autosubmit (i.e. we can
    delete that code -- for later).
    TODO: At some point in the future, we can drop ``DbManager`` and
          replace it with this one (or rename this to DbManager), as
          SQLAlchemy should be able to handle SQLite too.
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        self.engine: Engine = session.create_engine()
        self.schema = schema
        # Each time we self.engine.connect(),
        # the engine will create or reuse a connection from the pool
        # Using the connection as a context (with) will safely release it
        self.connection: Connection = None 

    def backup(self):
        pass

    def restore(self):
        pass

    def disconnect(self):
        if self.connection and not self.connection.closed:
            self.connection.close()

    def create_table(self, table_name: str, fields: List[str]):
        # NOTE: ``fields`` is ignored as they are defined in the SQLAlchemy
        #       tables definitions (see ``.database.tables``). Kept for
        #       backward compatibility with the old API for SQLite.
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            if self.schema:
                conn.execute(CreateSchema(self.schema, if_not_exists=True))
            conn.execute(CreateTable(table, if_not_exists=True))
            conn.commit()

    def drop_table(self, table_name: str):
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            conn.execute(DropTable(table, if_exists=True))
            conn.commit()

    def insert(self, table_name: str, columns: List[str], values: List[str]):
        """Not implemented.
        In the original ``DbManager`` (SQLite), this function is used
        only to initialize the SQLite database (i.e. no external users).
        """
        raise NotImplementedError()

    def insertMany(self, table_name: str, data: List[Union[Iterable, Dict]]):
        """
        N.B.: One difference between SQLite and SQLAlchemy here;
              whereas with SQLite you insert a list of values
              matching the columns, for SQLAlchemy you provide
              a dictionary. For backward-compatibility, we risk
              accepting a list or dictionary here. If a list
              is provided, then we will use the Table metadata
              to fetch the columns. Not the safest approach, so
              prefer to send a dictionary!
        :type table_name: str
        :type data: List[Union[Iterable, Dict]]
        """
        if len(data) == 0:
            return 0
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        if type(data[0]) is list or type(data[0]) is tuple:
            # Convert into a dictionary; we know the keys from the
            # table metadata columns.
            columns = [column.name for column in cast(list, table.columns)]
            data = map(lambda values: {
                key: value
                for key, value in
                zip(columns, values)
            }, cast(list, data))
            data = list(data)

        with self.engine.connect() as conn:
            result = conn.execute(insert(table), data)
            conn.commit()

        return cast(int, result.rowcount)

    def delete_where(self, table_name: str, where: List[str]):
        command = DbManager.generate_delete_command(table_name, where)
        with self.engine.connect() as conn:
            result = conn.execute(text(command))
            conn.commit()
        return cast(int, result.rowcount)

    def select_first(self, table_name: str):
        """Not used in the original ``DbManager``!"""
        raise NotImplementedError()

    def select_first_where(self, table_name: str, where: List[str]):
        command = DbManager.generate_select_command(table_name, where)
        with self.engine.connect() as conn:
            row = conn.execute(text(command)).first()
        return row.tuple()

    def select_all(self, table_name: str):
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            rows = conn.execute(select(table)).all()
        return [row.tuple() for row in rows]

    def select_all_where(self, table_name: str, where: List[str]):
        """Not used in the original ``DbManager``!"""
        raise NotImplementedError()

    def count(self, table_name: str):
        """Not used in the original ``DbManager``!"""
        raise NotImplementedError()

    def drop(self):
        """Not used in the original ``DbManager``!"""
        raise NotImplementedError()

    # New! Used for testing...

    def delete_all(self, table_name: str) -> int:
        """Deletes all the rows of the table.
        :params table_name: The name of the table.
        :return: number of tables modified.
        """
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            result = conn.execute(delete(table))
            conn.commit()
        return cast(int, result.rowcount)



def create_db_manager(db_engine: str, **options) -> DatabaseManager:
    """
    Creates a Postgres or SQLite database manager based on the Autosubmit configuration.
    Note that you must provide the options even if they are optional, in which case
    you must provide ``options=None``, or you will get a ``KeyError``.
    Later we might be able to drop the SQLite database manager. So, for the moment,
    please call the function providing the database engine type, and the arguments
    for both SQLite and for SQLAlchemy.
    This means you do not have to do an ``if/else`` in your code, just give
    this function the engine type, and all the valid options, and it should
    handle choosing and building the database manager for you. e.g.
    ```python
    from autosubmit.database.db_manager import create_db_manager
    options = {
        # these are for sqlite
        'root_path': '/tmp/',
        'db_name': 'name.db',
        'db_version': 1,
        # and these for sqlalchemy -- not very elegant, but this is
        # to work-effectively-with-legacy-code (as in that famous book).
        'schema': 'a001'
    }
    db_manager = create_db_manager(db_engine='postgres', **options)
    ```
    :param db_engine: The database engine type.
    :return: A ``DatabaseManager``.
    :raises ValueError: If the database engine type is not valid.
    :raises KeyError: If the ``options`` dictionary is missing a required parameter for an engine.
    """
    if db_engine == "postgres":
        return cast(DatabaseManager, SqlAlchemyDbManager(options.get('schema', None)))
    elif db_engine == "sqlite":
        return cast(DatabaseManager, DbManager(options['root_path'], options['db_name'], options['db_version']))
    else:
        raise ValueError(f"Invalid database engine: {db_engine}")