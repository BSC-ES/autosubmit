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

import os
import textwrap
import time
from typing import Protocol, cast
from autosubmitconfigparser.config.basicconfig import BasicConfig
import autosubmit.history.utils as HUtils
from .database_manager import DatabaseManager, DEFAULT_LOCAL_ROOT_DIR
from . import database_models as Models

BasicConfig.read()


class ExperimentStatusDbManager(DatabaseManager):
    """ Manages the actions on the status database """

    def __init__(self, expid, db_dir_path, main_db_name,
                 local_root_dir_path=DEFAULT_LOCAL_ROOT_DIR):
        super(ExperimentStatusDbManager, self).__init__(expid,
                                                        local_root_dir_path=local_root_dir_path)
        self._as_times_file_path = os.path.join(db_dir_path, BasicConfig.AS_TIMES_DB)
        self._ecearth_file_path = os.path.join(db_dir_path, main_db_name)
        self._pkl_file_path = os.path.join(local_root_dir_path, self.expid, "pkl",
                                           "job_list_{0}.pkl".format(self.expid))
        self._validate_status_database()

    def _validate_status_database(self):
        """ Creates experiment_status table if it does not exist """
        create_table_query = textwrap.dedent(
            '''CREATE TABLE
                IF NOT EXISTS experiment_status (
                exp_id integer PRIMARY KEY,
                name text NOT NULL,
                status text NOT NULL,
                seconds_diff integer NOT NULL,
                modified text NOT NULL
            );'''
        )
        self.execute_statement_on_dbfile(self._as_times_file_path, create_table_query)

    def print_current_table(self):
        for experiment in self._get_experiment_status_content():
            print(experiment)
        if self.current_experiment_status_row:
            print(("Current Row:\n\t" + self.current_experiment_status_row.name + "\n\t" +
                   str(self.current_experiment_status_row.exp_id) +
                   "\n\t" + self.current_experiment_status_row.status))

    def is_running(self, time_condition=600):
        # type : (int) -> bool
        """ True if experiment is running, False otherwise. """
        if os.path.exists(self._pkl_file_path):
            current_stat = os.stat(self._pkl_file_path)
            timest = int(current_stat.st_mtime)
            timesys = int(time.time())
            time_diff = int(timesys - timest)
            if time_diff < time_condition:
                return True
            else:
                return False
        return False

    def set_existing_experiment_status_as_running(self, expid):
        """ Set the experiment_status row as running. """
        self.update_exp_status(expid, Models.RunningStatus.RUNNING)

    def create_experiment_status_as_running(self, experiment):
        """ Create a new experiment_status row for the Models.Experiment item."""
        self.create_exp_status(experiment.id, experiment.name, Models.RunningStatus.RUNNING)

    def get_experiment_status_row_by_expid(self, expid):
        # type : (str) -> Models.ExperimentRow
        """
        Get Models.ExperimentRow by expid.
        """
        experiment_row = self.get_experiment_row_by_expid(expid)
        return self.get_experiment_status_row_by_exp_id(experiment_row.id)

    def get_experiment_row_by_expid(self, expid):
        # type : (str) -> Models.ExperimentRow
        """
        Get the experiment from ecearth.db by expid as Models.ExperimentRow.
        """
        statement = self.get_built_select_statement("experiment", "name=?")
        current_rows = self.get_from_statement_with_arguments(self._ecearth_file_path,
                                                              statement, (expid,))
        if len(current_rows) <= 0:
            raise ValueError("Experiment {0} not found in {1}".format(
                expid, self._ecearth_file_path))
        return Models.ExperimentRow(*current_rows[0])

    def _get_experiment_status_content(self):
        # type : () -> List[Models.ExperimentStatusRow]
        """
        Get all registers from experiment_status as List of Models.ExperimentStatusRow.\n
        """
        statement = self.get_built_select_statement("experiment_status")
        current_rows = self.get_from_statement(self._as_times_file_path, statement)
        return [Models.ExperimentStatusRow(*row) for row in current_rows]

    def get_experiment_status_row_by_exp_id(self, exp_id):
        # type : (int) -> Models.ExperimentStatusRow
        """ Get Models.ExperimentStatusRow from as_times.db by exp_id (int)"""
        statement = self.get_built_select_statement("experiment_status", "exp_id=?")
        arguments = (exp_id,)
        current_rows = self.get_from_statement_with_arguments(self._as_times_file_path,
                                                              statement, arguments)
        if len(current_rows) <= 0:
            return None
        return Models.ExperimentStatusRow(*current_rows[0])

    def create_exp_status(self, exp_id, expid, status):
        # type : (int, str) -> None
        """
        Create experiment status
        """
        statement = ''' INSERT INTO experiment_status(exp_id, name,
        status, seconds_diff, modified) VALUES(?,?,?,?,?) '''
        arguments = (exp_id, expid, status, 0, HUtils.get_current_datetime())
        return self.insert_statement_with_arguments(self._as_times_file_path, statement, arguments)

    def update_exp_status(self, expid, status="RUNNING"):
        # type : (str, str) -> None
        """
        Update status, seconds_diff, modified in experiment_status.
        """
        statement = ''' UPDATE experiment_status SET status = ?, 
        seconds_diff = ?, modified = ? WHERE name = ? '''
        arguments = (status, 0, HUtils.get_current_datetime(), expid)
        self.execute_statement_with_arguments_on_dbfile(
            self._as_times_file_path, statement, arguments)

    def delete_exp_status(self, expid):
        # type : (str) -> None
        """ Deletes experiment_status row by expid. Useful for testing purposes. """
        statement = ''' DELETE FROM experiment_status where name = ? '''
        arguments = (expid,)
        self.execute_statement_with_arguments_on_dbfile(
            self._as_times_file_path, statement, arguments)


class ExperimentStatusDatabaseManager(Protocol):
    def print_current_table(self): ...

    def is_running(self, time_condition=600): ...

    def set_existing_experiment_status_as_running(self, expid): ...

    def create_experiment_status_as_running(self, experiment): ...

    def get_experiment_status_row_by_expid(self, expid): ...

    def get_experiment_row_by_expid(self, expid): ...

    def get_experiment_status_row_by_exp_id(self, exp_id): ...

    def create_exp_status(self, exp_id, expid, status): ...

    def update_exp_status(self, expid, status="RUNNING"): ...

    def delete_exp_status(self, expid): ...


def create_experiment_status_db_manager(
        db_engine: str, **options
) -> ExperimentStatusDatabaseManager:
    """
    Creates a Postgres or SQLite database manager based on the Autosubmit configuration.
    Note that you must provide the options even if they are optional, in which case
    you must provide ``options=None``, or you will get a ``KeyError``.
    TODO: better example and/or link to DbManager.
    :param db_engine: The database engine type.
    :return: An ``ExperimentStatusDatabaseManager``.
    :raises ValueError: If the database engine type is not valid.
    :raises KeyError: If the ``options`` dictionary is missing a required parameter for an engine.
    """
    return cast(
        ExperimentStatusDatabaseManager,
        ExperimentStatusDbManager(
            expid=options["expid"],
            db_dir_path=options["db_dir_path"],
            main_db_name=options["main_db_name"],
            local_root_dir_path=options["local_root_dir_path"],
        ),
    )
