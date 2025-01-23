

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

from autosubmit.database.db_manager import DbManager
from log.log import AutosubmitCritical
from typing import Any, List


class JobPackagePersistence(object):
    """
    Class that handles packages workflow.

    Create Packages Table, Wrappers Table.

    :param persistence_path: Path to the persistence folder pkl. \n
    :type persistence_path: String \n
    :param persistence_file: Name of the persistence pkl file. \n
    :type persistence_file: String
    """

    VERSION = 1
    JOB_PACKAGES_TABLE = 'job_package'
    WRAPPER_JOB_PACKAGES_TABLE = 'wrapper_job_package'
    TABLE_FIELDS = ['exp_id', 'package_name', 'job_name', 'wallclock' ]  # new field, needs a new autosubmit create

    def __init__(self, persistence_path, persistence_file):
        self.db_manager = DbManager(persistence_path, persistence_file, self.VERSION)
        self.db_manager.create_table(self.JOB_PACKAGES_TABLE, self.TABLE_FIELDS)
        self.db_manager.create_table(self.WRAPPER_JOB_PACKAGES_TABLE, self.TABLE_FIELDS)

    def load(self, wrapper=False) -> List[Any]:
        """
        Loads package of jobs from a database
        :param: wrapper: boolean
        :return: list of jobs per package
        """
        if not wrapper:
            results = self.db_manager.select_all(self.JOB_PACKAGES_TABLE)
        else:
            results = self.db_manager.select_all(self.WRAPPER_JOB_PACKAGES_TABLE)
        if len(results) > 0:
            # ['exp_id', 'package_name', 'job_name', 'wallclock']  wallclock is the new addition
            for wrapper in results:
                if len(wrapper) != 4:
                    # New field in the db, so not compatible if the wrapper package is not reset
                    # (done in the create function)
                    raise AutosubmitCritical("Error while loading the wrappers. The current wrappers have a different "
                                             "amount of fields than the expected. Possibly due to using different "
                                             "versions of Autosubmit in the same experiment. Please, run "
                                             "'autosubmit create -f <EXPID>' to fix this issue.")
        return results

    def reset(self):
        """
        Loads package of jobs from a database

        """
        self.db_manager.drop_table(self.WRAPPER_JOB_PACKAGES_TABLE)
        self.db_manager.create_table(self.WRAPPER_JOB_PACKAGES_TABLE, self.TABLE_FIELDS)

    def save(self, package, preview_wrappers=False):
        """
        Persists a job list in a database
        :param package: all wrapper attributes
        :param preview_wrappers: boolean
        """
        #self._reset_table()
        job_packages_data = []
        for job in package.jobs:
            job_packages_data += [(package._expid, package.name, job.name, package._wallclock)]

        if preview_wrappers:
            self.db_manager.insertMany(self.WRAPPER_JOB_PACKAGES_TABLE, job_packages_data)
        else:
            self.db_manager.insertMany(self.JOB_PACKAGES_TABLE, job_packages_data)
            self.db_manager.insertMany(self.WRAPPER_JOB_PACKAGES_TABLE, job_packages_data)

    def reset_table(self,wrappers=False):
        """
        Drops and recreates the database
        """
        if wrappers:
            self.db_manager.drop_table(self.WRAPPER_JOB_PACKAGES_TABLE)
            self.db_manager.create_table(self.WRAPPER_JOB_PACKAGES_TABLE, self.TABLE_FIELDS)
        else:
            self.db_manager.drop_table(self.JOB_PACKAGES_TABLE)
            self.db_manager.create_table(self.JOB_PACKAGES_TABLE, self.TABLE_FIELDS)
            self.db_manager.drop_table(self.WRAPPER_JOB_PACKAGES_TABLE)
            self.db_manager.create_table(self.WRAPPER_JOB_PACKAGES_TABLE, self.TABLE_FIELDS)
