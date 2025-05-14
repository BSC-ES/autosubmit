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

import gc
import os
import pickle
import shutil
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from sys import setrecursionlimit, getrecursionlimit
from typing import TYPE_CHECKING

from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import JobPklTable
from log.log import Log

if TYPE_CHECKING:
    from networkx import DiGraph

from autosubmit.database.tables import JobsTable, GraphTable, WrapperJobsTable, WrapperInfoTable, metadata_obj, PreviewWrapperJobsTable, PreviewWrapperInfoTable


class JobListPersistence(object):
    """
    Class to manage the persistence of the job lists

    """

    def save(self, persistence_path, persistence_file, job_list, graph):
        """
        Persists a job list
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str
        :param graph: DiGraph
        """
        raise NotImplementedError

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from persistence
        :param persistence_file: str
        :param persistence_path: str

        """
        raise NotImplementedError

    def sqlite_exists(self, persistence_path, persistence_file):
        """
        Check if a sqlite db file exists
        :param persistence_file: str
        :param persistence_path: str
        """
        raise NotImplementedError


class JobListPersistenceDb(JobListPersistence):
    """
    Class to manage the database persistence of the job lists

    """
    VERSION = 1

    def __init__(self, expid):
        options = {
            "root_path": str(Path(BasicConfig.LOCAL_ROOT_DIR, expid, "db")),  # folder renamed
            "db_name": f"graph_{expid}",
            "db_version": self.VERSION
        }
        self.expid = expid

        self.db_manager = create_db_manager(BasicConfig.DATABASE_BACKEND, **options)
        metadata_obj.create_all(self.db_manager, tables=[JobsTable, GraphTable, WrapperJobsTable, WrapperInfoTable, PreviewWrapperJobsTable, PreviewWrapperInfoTable])
        pass
    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from a database
        :param persistence_file: str
        :param persistence_path: str

        """
        row = self.db_manager.select_first_where(
            self.JOB_LIST_TABLE,
            [f"expid = '{self.expid}'"]
        )
        if row:
            pickled_data = row[1]
            return pickle.loads(pickled_data)
        return None

    def save(self, persistence_path, persistence_file, job_list, graph: 'DiGraph') -> None:
        """
        Persists a job list in a database
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str
        :param graph: networkx graph object
        :type graph: DiGraph
        """
        # Serialize the job list
        data = {job.name: job.__getstate__() for job in job_list}
        pickled_data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        gc.collect()

        # Delete previous row
        try:
            self.db_manager.delete_where(
                self.JOB_LIST_TABLE,
                [f"expid = '{self.expid}'"]
            )
        except Exception:
            Log.debug("No previous row to delete")

        # Insert the new row
        Log.debug("Saving JobList on DB")
        # Use insertMany as it is a generalization of insert
        self.db_manager.insertMany(
            self.JOB_LIST_TABLE,
            [
                {
                    "expid": self.expid,
                    "sqlite": pickled_data,
                    "modified": str(datetime.now()),
                }
            ]
        )
        Log.debug("JobList saved in DB")
