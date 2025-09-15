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
from pathlib import Path
from sys import setrecursionlimit, getrecursionlimit
from typing import TYPE_CHECKING

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_manager import create_db_manager
from autosubmit.log.log import Log, AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job import Job
    from autosubmit.job.job_list import JobList


class JobListPersistence(object):
    """Class to manage the persistence of the job lists."""

    def save(self, persistence_path: str, persistence_file: str, job_list: list['Job']) -> None:
        """Persists a job list.

        :param job_list: JobList
        :param persistence_file: The name of the persistence database file.
        :param persistence_path: The path to the persistence database.
        """
        raise NotImplementedError

    def load(self, persistence_path, persistence_file) -> 'JobList':
        """Loads a job list from persistence

        :param persistence_file: The name of the persistence database file.
        :param persistence_path: The path to the persistence database.
        """
        raise NotImplementedError


class JobListPersistencePkl(JobListPersistence):
    """Class to manage the pickle persistence of the job lists."""

    def load(self, persistence_path: str, persistence_file: str):
        """Loads a job list from a pkl file."""
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

    def save(self, persistence_path: str, persistence_file: str, job_list: list['Job']):
        """Persists a job list in a pickle pkl file."""
        path = Path(persistence_path, f'{persistence_file}.pkl.tmp')
        with suppress(FileNotFoundError, PermissionError):
            path.unlink(missing_ok=True)
        Log.debug(f"Saving JobList: {str(path)}")
        with open(path, 'wb') as fd:
            current_limit = getrecursionlimit()
            setrecursionlimit(100000)
            pickle.dump({job.name: job.__getstate__() for job in job_list}, fd, pickle.HIGHEST_PROTOCOL)
            setrecursionlimit(current_limit)
            gc.collect()  # Tracemalloc show leaks without this

        path_tmp_name = str(path)
        path_name = path_tmp_name[:-4]

        os.replace(path_tmp_name, path_name)
        Log.debug(f'JobList saved in {path_name}')


class JobListPersistenceDb(JobListPersistence):
    """Class to manage the database persistence of the job lists."""

    VERSION = 3
    JOB_LIST_TABLE = 'job_list'
    TABLE_FIELDS = [
        "name",
        "id",
        "status",
        "priority",
        "section",
        "date",
        "member",
        "chunk",
        "split",
        "local_out",
        "local_err",
        "remote_out",
        "remote_err",
        "wrapper_type",
    ]

    def __init__(self, expid: str):
        options = {
            "root_path": str(Path(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl")),
            "db_name": f"job_list_{expid}",
            "db_version": self.VERSION,
            "schema": expid
        }
        self.expid = expid
        self.db_manager = create_db_manager(BasicConfig.DATABASE_BACKEND, **options)

    def load(self, persistence_path, persistence_file):
        """Loads a job list from a database."""
        return self.db_manager.select_all(self.JOB_LIST_TABLE)

    def save(self, persistence_path, persistence_file, job_list):
        """Persists a job list to a database."""
        self._reset_table()
        jobs_data = [(job.name, job.id, job.status,
                      job.priority, job.section, job.date,
                      job.member, job.chunk, job.split,
                      job.local_logs[0], job.local_logs[1],
                      job.remote_logs[0], job.remote_logs[1], job.wrapper_type) for job in job_list]
        self.db_manager.insertMany(self.JOB_LIST_TABLE, jobs_data)

    def _reset_table(self) -> None:
        """Drops and recreates the database."""
        self.db_manager.drop_table(self.JOB_LIST_TABLE)
        self.db_manager.create_table(self.JOB_LIST_TABLE, self.TABLE_FIELDS)


def get_job_list_persistence(expid: str, as_conf: 'AutosubmitConfig') -> JobListPersistence:
    """Return the persistence object for a ``JobList`` based on what is configured in Autosubmit."""
    storage_type = as_conf.get_storage_type()

    if storage_type not in ('pkl', 'db'):
        raise AutosubmitCritical('Storage type not known', 7014)

    if storage_type == 'pkl':
        return JobListPersistencePkl()
    elif storage_type == 'db':
        return JobListPersistenceDb(expid)
