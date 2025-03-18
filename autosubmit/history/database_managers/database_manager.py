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
import traceback

import autosubmit.history.utils as HUtils
import autosubmit.history.database_managers.database_models as Models
from abc import ABCMeta

from log.log import Log

DEFAULT_JOBDATA_DIR = os.path.join('/esarchive', 'autosubmit', 'as_metadata', 'data')
DEFAULT_HISTORICAL_LOGS_DIR = os.path.join('/esarchive', 'autosubmit', 'as_metadata', 'logs')
DEFAULT_LOCAL_ROOT_DIR = os.path.join('/esarchive', 'autosubmit')


class DatabaseManager(metaclass=ABCMeta):
    """ Simple database manager. Needs expid. """
    AS_TIMES_DB_NAME = "as_times.db"  # default AS_TIMES location
    ECEARTH_DB_NAME = "ecearth.db"  # default EC_EARTH_DB_NAME location

    def __init__(self, expid, jobdata_dir_path=DEFAULT_JOBDATA_DIR, local_root_dir_path=DEFAULT_LOCAL_ROOT_DIR):
        self.expid = expid
        self.JOBDATA_DIR = jobdata_dir_path
        self.LOCAL_ROOT_DIR = local_root_dir_path

    def get_connection(self, path):
        # type : (str) -> Sqlite3Connection
        """
        Create a database connection to the SQLite database specified by path.
        :param path: database file name
        :return: Connection object or None
        """
        if not os.path.exists(path):
            self._create_database_file(path)
        return sqlite3.connect(path)

    def _create_database_file(self, path):
        # type : (str) -> None
        """ creates a database files with full permissions """
        os.umask(0)
        os.open(path, os.O_WRONLY | os.O_CREAT, 0o776)


    def execute_statement_on_dbfile(self, path, statement):
        # type : (str, str) -> None
        """ Executes a statement on a database file specified by path. """
        conn = self.get_connection(path)
        cursor = conn.cursor()
        cursor.execute(statement)
        conn.commit()
        conn.close()

    def execute_statement_with_arguments_on_dbfile(self, path, statement, arguments):
        # type : (str, str, Tuple) -> None
        """ Executes a statement with arguments on a database file specified by path. """
        conn = self.get_connection(path)
        cursor = conn.cursor()
        cursor.execute(statement, arguments)
        conn.commit()
        conn.close()

    def execute_many_statement_with_arguments_on_dbfile(self, path, statement, arguments_list):
        # type : (str, str, List[Tuple]) -> None
        """ Executes many statements from a list of arguments specified by a path. """
        conn = self.get_connection(path)
        cursor = conn.cursor()
        cursor.executemany(statement, arguments_list)
        conn.commit()
        conn.close()

    def execute_many_statements_on_dbfile(self, path, statements):
        # type : (str, List[str]) -> None
        """
        Updates the table schema using a **small** list of statements. No Exception raised.
        Should be used to execute a list of schema updates that might have been already applied.
        """
        for statement in statements:
            try:
                self.execute_statement_on_dbfile(path, statement)
            except Exception as exp:
                if "duplicate" not in str(exp).lower():
                    Log.debug(
                        f"Error executing statement {statement} on {path}: {str(exp)}, Traceback: {traceback.format_exc()}")

    def get_from_statement(self, path, statement):
        # type : (str, str) -> List[Tuple]
        """ Get the rows from a statement with no arguments """
        conn = self.get_connection(path)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute(statement)
        statement_rows = cursor.fetchall()
        conn.close()
        return statement_rows

    def get_from_statement_with_arguments(self, path, statement, arguments):
        # type : (str, str, Tuple) -> List[Tuple]
        """ Get the rows from a statement with arguments """
        conn = self.get_connection(path)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute(statement, arguments)
        statement_rows = cursor.fetchall()
        conn.close()
        return statement_rows

    def insert_statement(self, path, statement):
        # type : (str, str) -> int
        """ Insert statement into path """
        conn = self.get_connection(path)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute(statement)
        lastrow_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return lastrow_id

    def insert_statement_with_arguments(self, path, statement, arguments):
        # type : (str, str, Tuple) -> int
        """ Insert statement with arguments into path """
        conn = self.get_connection(path)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute(statement, arguments)
        lastrow_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return lastrow_id

    def get_built_select_statement(self, table_name, conditions=None):
        # type : (str, namedtuple, str) -> str
        """ Build and return a SELECT statement with the same fields as the model. Requires that the table is associated with a model (namedtuple). """
        model = Models.table_name_to_model[table_name]
        if conditions:
            return "SELECT {0} FROM {1} WHERE {2}".format(HUtils.get_fields_as_comma_str(model), table_name, conditions)
        else:
            return "SELECT {0} FROM {1}".format(HUtils.get_fields_as_comma_str(model), table_name)
