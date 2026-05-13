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

from autosubmit.config.basicconfig import BasicConfig

from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import JobPklTable
from autosubmit.log.log import Log

if TYPE_CHECKING:
    from networkx import DiGraph
    from autosubmit.job.job import Job


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
        raise NotImplementedError  # pragma: no cover

    def load(self, persistence_path, persistence_file):
        """
        Loads a job list from persistence
        :param persistence_file: str
        :param persistence_path: str

        """
        raise NotImplementedError  # pragma: no cover

    def pkl_exists(self, persistence_path, persistence_file):
        """
        Check if a pkl file exists
        :param persistence_file: str
        :param persistence_path: str
        """
        raise NotImplementedError

    def save_job_log(self, persistence_path: str, persistence_file: str, job: 'Job') -> None:
        """Save only the log-related fields of a single job to persistence.

        :param persistence_path: Directory where the persistence file lives.
        :param persistence_file: Base name of the persistence file (without extension).
        :param job: Job object whose log information is to be saved.
        :raises NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError  # pragma: no cover


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
            # copy the path to a tmp file random seed to avoid corruption
            try:
                shutil.copy(str(path), str(path_tmp))
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
        Log.debug("Saving JobList: " + str(path))
        with open(path, 'wb') as fd:
            current_limit = getrecursionlimit()
            setrecursionlimit(100000)
            pickle.dump({job.name: job.__getstate__() for job in job_list}, fd, pickle.HIGHEST_PROTOCOL)  # type: ignore
            setrecursionlimit(current_limit)
            # profiler shows memory leak if we remove this.
            gc.collect()
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

    def save_job_log(self, persistence_path: str, persistence_file: str, job: 'Job') -> None:
        """Save only the log-related fields of a single job to the pkl file.

        Load the existing pkl, patch the log keys for the given job, and
        write it back atomically via a temporary file.

        :param persistence_path: Directory where the pkl file lives.
        :param persistence_file: Base name of the pkl file (without extension).
        :param job: Job object whose log information is to be saved.
        """
        _LOG_KEYS = frozenset({
            'name', 'log', 'updated_log',
            'local_logs_out', 'local_logs_err',
            'remote_logs_out', 'remote_logs_err',
        })

        path = os.path.join(persistence_path, persistence_file + '.pkl')
        try:
            job_list: dict = self.load(persistence_path, persistence_file) or {}
        except FileNotFoundError:
            Log.warning(f'pkl not found at {path}; skipping log save for {job.name}.')
            return

        job_state: dict = job.__getstate__()
        log_data = {k: v for k, v in job_state.items() if k in _LOG_KEYS}
        job_list.setdefault(job.name, {}).update(log_data)

        tmp_path = path + '.tmp'
        with suppress(FileNotFoundError, PermissionError):
            os.remove(tmp_path)
        with open(tmp_path, 'wb') as fd:
            current_limit = getrecursionlimit()
            setrecursionlimit(100000)
            pickle.dump(job_list, fd, pickle.HIGHEST_PROTOCOL)
            setrecursionlimit(current_limit)
            gc.collect()
        os.replace(tmp_path, path)
        Log.debug(f'Log fields for job {job.name} saved in {path}')


class JobListPersistenceDb(JobListPersistence):
    """Class to manage the database persistence of the job lists."""

    # TODO: Was this actually used anywhere? Couldn't locate where...
    VERSION = 4

    def __init__(self, expid):
        self.expid = expid
        database_file = Path(BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl', f'job_list_{expid}.db')
        connection_url = get_connection_url(db_path=database_file)
        self.db_manager = DbManager(connection_url=connection_url)
        self.db_manager.create_table(JobPklTable.name)

    def load(self, persistence_path, persistence_file):
        """Loads a job list from a database.

        :param persistence_file: str
        :param persistence_path: str
        """
        row = self.db_manager.select_first_where(
            JobPklTable.name,
            {'expid': self.expid}
        )
        if row:
            pickled_data = row[1]
            return pickle.loads(pickled_data)
        return None

    def save(self, persistence_path, persistence_file, job_list, graph: 'DiGraph') -> None:
        """Persists a job list in a database.

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
            JobPklTable.name,
            {'expid': self.expid}
        )

        # Insert the new row
        Log.debug("Saving JobList on DB")
        # Use insertMany as it is a generalization of insert
        self.db_manager.insert_many(
            JobPklTable.name,
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
        """Check if a pickle file exists.

        :param persistence_file: str
        :param persistence_path: str
        """
        return self.db_manager.select_first_where(
            JobPklTable.name, {'expid': self.expid}
        ) is not None

    def save_job_log(self, persistence_path: str, persistence_file: str, job: 'Job') -> None:
        """Save only the log-related fields of a single job to the database pkl blob.

        Load the existing pickled job list, patch the log keys for the given
        job, and write the updated blob back, replacing the previous row.

        :param persistence_path: Unused; kept for interface compatibility.
        :param persistence_file: Unused; kept for interface compatibility.
        :param job: Job object whose log information is to be saved.
        """
        _LOG_KEYS = frozenset({
            'name', 'log', 'updated_log',
            'local_logs_out', 'local_logs_err',
            'remote_logs_out', 'remote_logs_err',
        })

        job_list: dict = self.load(persistence_path, persistence_file) or {}

        job_state: dict = job.__getstate__()
        log_data = {k: v for k, v in job_state.items() if k in _LOG_KEYS}
        job_list.setdefault(job.name, {}).update(log_data)

        pickled_data = pickle.dumps(job_list, protocol=pickle.HIGHEST_PROTOCOL)
        gc.collect()

        self.db_manager.delete_where(JobPklTable.name, {'expid': self.expid})
        self.db_manager.insert_many(
            JobPklTable.name,
            [{"expid": self.expid, "pkl": pickled_data, "modified": str(datetime.now())}],
        )
        Log.debug(f'Log fields for job {job.name} saved in DB pkl for {self.expid}')
