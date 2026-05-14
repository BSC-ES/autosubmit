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

"""Database layer for the Job packages."""

from pathlib import Path
from typing import Any, List

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import WrapperInfoTable, PreviewWrapperInfoTable, WrapperJobsTable, \
    PreviewWrapperJobsTable
from autosubmit.log.log import AutosubmitCritical


class JobPackagePersistence:
    """Class that handles packages workflow.

    Create Packages Table, Wrappers Table.
    """

    VERSION = 1

    def __init__(self, expid: str):
        database_file = Path(BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl', f'job_packages_{expid}.db')
        connection_url = get_connection_url(db_path=database_file)

        if BasicConfig.DATABASE_BACKEND == "postgres":
            _schema = expid
        else:
            _schema = None

        self.db_manager = DbManager(connection_url=connection_url, schema=_schema)
        self.db_manager.create_table(WrapperInfoTable.name)
        self.db_manager.create_table(PreviewWrapperInfoTable.name)
        self.db_manager.create_table(WrapperJobsTable.name)
        self.db_manager.create_table(PreviewWrapperJobsTable.name)

    def load(self, preview=False) -> tuple[Any, Any]:
        """Loads the job packages from the database.

        :param preview: Use an auxiliar table for previewing the wrappers.
        :rtype: bool
        :return: list of job packages
        """
        if not preview:
            wrapper_info = self.db_manager.select_all(WrapperInfoTable.name)
            wrapped_jobs = self.db_manager.select_all(WrapperJobsTable.name)
        else:
            wrapper_info = self.db_manager.select_all(PreviewWrapperInfoTable.name)
            wrapped_jobs = self.db_manager.select_all(PreviewWrapperJobsTable.name)

        return wrapper_info, wrapped_jobs

    def save(self, package, preview_wrappers=False):
        """Persists a job list in a database.

        :param package: all wrapper attributes
        :param preview_wrappers: boolean
        """
        job_packages_data = []
        for job in package.jobs:
            # noinspection PyProtectedMember
            # job_packages_data += [{
            #     'exp_id': package._expid,
            #     'package_name': package.name,
            #     'job_name': job.name,
            #     'wallclock': package._wallclock
            # }]
            job_packages_data += [{
                'exp_id': package._expid,
                'package_name': package.name,
                'job_name': job.name,
                'wallclock': package._wallclock

            }]

        if preview_wrappers:
            self.db_manager.insert_many(WrapperJobPackageTable.name, job_packages_data)
        else:
            self.db_manager.insert_many(JobPackageTable.name, job_packages_data)
            self.db_manager.insert_many(WrapperJobPackageTable.name, job_packages_data)

    def reset_table(self, preview=False):
        """Drops and recreates the database."""

        self.db_manager.drop_table(PreviewWrapperJobsTable.name)
        self.db_manager.create_table(PreviewWrapperJobsTable.name)
        if not preview:
            self.db_manager.drop_table(WrapperInfoTable.name)
            self.db_manager.drop_table(WrapperJobsTable.name)
