#!/usr/bin/env python3
import gc
# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

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
import pickle
from datetime import datetime
from sys import setrecursionlimit, getrecursionlimit
import shutil
from autosubmit.database.db_manager import create_db_manager
from log.log import Log
from contextlib import suppress
from autosubmitconfigparser.config.basicconfig import BasicConfig
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from networkx import DiGraph


class JobListPersistence(object):
    """
    Class to manage the persistence of the job lists

    """

    def save(self, persistence_path, persistence_file, job_list , graph):
        """
        Persists a job list
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str

        """
        raise NotImplementedError

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from persistence
        :param persistence_file: str
        :param persistence_path: str

        """
        raise NotImplementedError

    def pkl_exists(self, persistence_path, persistence_file):
        """
        Check if a pkl file exists
        :param persistence_file: str
        :param persistence_path: str
        """
        raise NotImplementedError


class JobListPersistencePkl(JobListPersistence):
    """
    Class to manage the pickle persistence of the job lists

    """

    EXT = '.pkl'

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from a pkl file
        :param persistence_file: str
        :param persistence_path: str

        """
        path = os.path.join(persistence_path, persistence_file + '.pkl')
        path_tmp = os.path.join(persistence_path[:-3]+"tmp", persistence_file + f'.pkl.tmp_{os.urandom(8).hex()}')

        try:
            open(path).close()
        except PermissionError:
            Log.warning(f'Permission denied to read {path}')
            raise
        except FileNotFoundError:
            Log.warning(f'File {path} does not exist. ')
            raise
        else:
            # copy the path to a tmp file randomseed to avoid corruption
            try:
                shutil.copy(path, path_tmp)
                with open(path_tmp, 'rb') as fd:
                    current_limit = getrecursionlimit()
                    setrecursionlimit(100000)
                    job_list = pickle.load(fd)
                    setrecursionlimit(current_limit)
            finally:
                os.remove(path_tmp)

            return job_list

    def save(self, persistence_path, persistence_file, job_list, graph: 'DiGraph'):
        """
        Persists a job list in a pkl file
        :param job_list: JobList
        :param persistence_file: str
        :param persistence_path: str
        :param graph: networkx graph object
        :type graph: DiGraph
        """

        path = os.path.join(persistence_path, persistence_file + '.pkl' + '.tmp')
        with suppress(FileNotFoundError, PermissionError):
            os.remove(path)
        Log.debug("Saving JobList: " + path)
        with open(path, 'wb') as fd:
            current_limit = getrecursionlimit()
            setrecursionlimit(100000)
            pickle.dump({job.name: job.__getstate__() for job in job_list}, fd, pickle.HIGHEST_PROTOCOL)
            setrecursionlimit(current_limit)
            gc.collect()  # Tracemalloc show leaks without this
        os.replace(path, path[:-4])
        Log.debug(f'JobList saved in {path[:-4]}')

    def pkl_exists(self, persistence_path, persistence_file):
        """
        Check if a pkl file exists
        :param persistence_file: str
        :param persistence_path: str
        """
        path = os.path.join(persistence_path, persistence_file + '.pkl')
        return os.path.exists(path)


class JobListPersistenceDb(JobListPersistence):
    """
    Class to manage the database persistence of the job lists

    """

    VERSION = 4
    JOB_LIST_TABLE = "job_pkl"
    TABLE_FIELDS = ["expid", "pkl", "modified"]

    def __init__(self, expid):
        options = {
            "root_path": str(Path(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl")),
            "db_name": f"job_list_{expid}",
            "db_version": self.VERSION
        }
        self.expid = expid
        self.db_manager = create_db_manager(BasicConfig.DATABASE_BACKEND, **options)
        self.db_manager.create_table(self.JOB_LIST_TABLE, self.TABLE_FIELDS)

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
        self.db_manager.delete_where(
            self.JOB_LIST_TABLE,
            [f"expid = '{self.expid}'"]
        )

        # Insert the new row
        Log.debug("Saving JobList on DB")
        # Use insertMany as it is a generalization of insert
        self.db_manager.insertMany(
            self.JOB_LIST_TABLE,
            [
                {
                    "expid": self.expid,
                    "pkl": pickled_data,
                    "modified": str(datetime.now()),
                }
            ]
        )
        Log.debug("JobList saved in DB")

    def pkl_exists(self, persistence_path, persistence_file):
        """Check if a pkl file exists

        :param persistence_file: str
        :param persistence_path: str
        """
        return self.db_manager.select_first_where(
            self.JOB_LIST_TABLE, [f"expid = '{self.expid}'"]
        ) is not None
