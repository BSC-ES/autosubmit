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
from autosubmit.log.log import Log, AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job import Job
    from autosubmit.job.job_list import JobList
    from networkx import DiGraph


class JobListPersistence(object):
    """Class to manage the persistence of the job lists."""

    def save(self, persistence_path: str, persistence_file: str, job_list: list['Job'], graph: 'DiGraph') -> None:
        """Persists a job list.

        :param job_list: List of Autosubmit Jobs.
        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
        :param graph: NetworkX graph object.
        """
        raise NotImplementedError  # pragma: no cover

    def load(self, persistence_path, persistence_file) -> 'JobList':
        """Loads a job list from persistence.

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
        """
        raise NotImplementedError  # pragma: no cover

    def pkl_exists(self, persistence_path, persistence_file):
        """Check if a pkl file exists.

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
        """
        raise NotImplementedError  # pragma: no cover


class JobListPersistencePkl(JobListPersistence):
    """Class to manage the pickle persistence of the job lists."""

    def load(self, persistence_path: str, persistence_file: str):
        """Loads a job list from a pkl file.

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
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

    def save(self, persistence_path: str, persistence_file: str, job_list: list['Job'], graph: 'DiGraph'):
        """Persists a job list in a pickle pkl file.

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
        :param job_list: List of Autosubmit Jobs.
        :param graph: NetworkX graph object.
        """
        path = Path(persistence_path, f'{persistence_file}.pkl.tmp')
        with suppress(FileNotFoundError, PermissionError):
            path.unlink(missing_ok=True)
        Log.debug(f"Saving JobList: {str(path)}")
        with open(path, 'wb') as fd:
            current_limit = getrecursionlimit()
            setrecursionlimit(100000)
            pickle.dump({job.name: job.__getstate__() for job in job_list}, fd, pickle.HIGHEST_PROTOCOL)  # type: ignore
            setrecursionlimit(current_limit)
            # profiler shows memory leak if we remove this.
            gc.collect()

        path_tmp_name = str(path)
        path_name = path_tmp_name[:-4]

        os.replace(path_tmp_name, path_name)
        Log.debug(f'JobList saved in {path_name}')

    def pkl_exists(self, persistence_path, persistence_file):
        """Check if a pkl file exists.

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
        """
        path = Path(persistence_path, persistence_file + '.pkl')
        return path.exists()


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

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
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
        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
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

        :param persistence_path: The path to the persistence database.
        :param persistence_file: The name of the persistence database file.
        """
        return self.db_manager.select_first_where(
            JobPklTable.name, {'expid': self.expid}
        ) is not None


def get_job_list_persistence(expid: str, as_conf: 'AutosubmitConfig') -> JobListPersistence:
    """Return the persistence object for a ``JobList`` based on what is configured in Autosubmit."""
    storage_type = as_conf.get_storage_type()

    if storage_type not in ('pkl', 'db'):
        raise AutosubmitCritical('Storage type not known', 7014)

    if storage_type == 'pkl':
        return JobListPersistencePkl()
    elif storage_type == 'db':
        return JobListPersistenceDb(expid)
