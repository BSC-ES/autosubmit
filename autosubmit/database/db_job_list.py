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

"""Code to manage the structure of tables.

It uses the db_manager code to manage the database.
"""

import traceback
from pathlib import Path
from typing import Optional
from autosubmit.database.db_common import get_connection_url, check_db_path
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import JobsTable


ACTIVE_STATUSES = ['READY', 'SUBMITTED', 'QUEUING', 'HELD', 'RUNNING']

def _get_db_manager(sqlite_db_file: Optional[Path]) -> DbManager:
    """Create a ``db_manager`` with the given parameters."""
    connection_url = get_connection_url(db_path=sqlite_db_file)
    return DbManager(connection_url=connection_url)



def save_jobs(job_list_path, job_list):
    """Save the job list to the database. Normally this will save the current active jobs"""
    check_db_path(job_list_path)
    db_manager = _get_db_manager(job_list_path / f"job_list.db")
    db_manager.create_table(JobsTable.name)
    job_data = {job.name: job.__getstate__(structure=True, log_process=False) for job in job_list}
    db_manager.insert_many(JobsTable.name, job_data)
    #update_structure(job_list_path, job_data)
    pass

def load_active_and_children_jobs():
    """Load all active jobs ( READY, SUBMITTED, QUEUING, RUNNING ) and their children from the database."""

def load_all_jobs(job_list_path, job_list):
    """Load all jobs from the database. Used in generate"""
    check_db_path(job_list_path)
    db_manager = _get_db_manager(job_list_path / f"job_list.db")
    db_manager.create_table(JobsTable.name)
    job_data = db_manager.load(JobsTable.name)
    return job_data


def load_active_jobs(job_list_path, job_list):
    check_db_path(job_list_path)
    db_manager = _get_db_manager(job_list_path / f"job_list.db")
    db_manager.create_table(JobsTable.name)
    job_data = db_manager.load(JobsTable.name)
    return job_data


def select_active_jobs(job_list_path: Path) -> dict:
    """
    Returns a dict of active jobs: {job_name: job_data} for jobs with status in ACTIVE_STATUSES.
    """
    check_db_path(job_list_path)
    db_manager = _get_db_manager(job_list_path / "job_list.db")
    db_manager.create_table(JobsTable.name)

    active_jobs = {}
    for status in ACTIVE_STATUSES:
        rows = db_manager.select_where(JobsTable.name, {"status": status})
        for row in rows:
            # Assuming the first column is job_name
            job_name = row[0]
            active_jobs[job_name] = row
    return active_jobs

def select_children_of_active_jobs(job_list_path: Path) -> dict:
    """
    Returns a dict of active jobs: {job_name: job_data} for jobs with status in ACTIVE_STATUSES.
    """
    check_db_path(job_list_path)
    db_manager = _get_db_manager(job_list_path / "job_list.db")
    db_manager.create_table(JobsTable.name)

    active_jobs = {}
    for status in ACTIVE_STATUSES:
        rows = db_manager.select_where(JobsTable.name, {"status": status})
        for row in rows:
            # Assuming the first column is job_name
            job_name = row[0]
            active_jobs[job_name] = row
    return active_jobs
