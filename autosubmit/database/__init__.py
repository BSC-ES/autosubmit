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

"""Autosubmit Database."""

import os
import subprocess

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import Log

__all__ = ["database_backup"]


def database_backup(expid):
    if BasicConfig.DATABASE_BACKEND == "sqlite":
        try:
            database_path = os.path.join(
                BasicConfig.JOBDATA_DIR, f"job_data_{expid}.db"
            )
            backup_path = os.path.join(BasicConfig.JOBDATA_DIR, f"job_data_{expid}.sql")
            command = f"sqlite3 {database_path} .dump > {backup_path} "
            Log.debug("Backing up jobs_data...")
            subprocess.call(command, shell=True)
            Log.debug("Jobs_data database backup completed.")
        except BaseException:
            Log.debug("Jobs_data database backup failed.")
    elif BasicConfig.DATABASE_BACKEND == "postgres":
        # TODO: Implement Postgres backup
        Log.debug("Postgres database backup not implemented yet.")
