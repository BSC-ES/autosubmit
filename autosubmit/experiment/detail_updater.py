#!/usr/bin/env python3

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

import datetime
import pwd
from pathlib import Path
import sqlite3
from autosubmit.database.db_common import get_experiment_id
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


LOCAL_TZ = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo


class ExperimentDetailsRepository:
    """
    Class to manage the experiment details in a SQLite database.
    This class is responsible for creating the database, creating the
    table, and providing methods to insert, update, delete, and retrieve
    experiment details.
    """

    def __init__(self):
        self.db_path = Path(BasicConfig.DB_PATH)

        with sqlite3.connect(self.db_path) as conn:
            # Create the details table if it does not exist
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS details (
                    exp_id INTEGER NOT NULL, 
                    user TEXT NOT NULL, 
                    created TEXT NOT NULL, 
                    model TEXT NOT NULL, 
                    branch TEXT NOT NULL, 
                    hpc TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def get_details(self, exp_id: int):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT exp_id, user, created, model, branch, hpc
                FROM details
                WHERE exp_id = ?;
                """,
                (exp_id,),
            )

            result = cursor.fetchone()
            if result:
                return {
                    "exp_id": result[0],
                    "user": result[1],
                    "created": result[2],
                    "model": result[3],
                    "branch": result[4],
                    "hpc": result[5],
                }
            else:
                return None

    def upsert_details(
        self, exp_id: int, user: str, created: str, model: str, branch: str, hpc: str
    ):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                DELETE FROM details
                WHERE exp_id = ?;
                """,
                (exp_id,),
            )
            conn.execute(
                """
                INSERT INTO details (exp_id, user, created, model, branch, hpc)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    exp_id,
                    user,
                    created,
                    model,
                    branch,
                    hpc,
                ),
            )
            conn.commit()

    def delete_details(self, exp_id: int):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                DELETE FROM details
                WHERE exp_id = (?);
                """,
                (exp_id,),
            )
            conn.commit()


class ExperimentDetails:
    """
    Class to manage the experiment details.
    """

    def __init__(self, expid: str, init_reload: bool = True):
        self.expid = expid
        self._details_repo = ExperimentDetailsRepository()
        if init_reload:
            self.reload()

    def reload(self):
        """
        Reload the necessary components to get the experiment details.
        """
        # Build path stat
        self.exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(self.expid)
        self.exp_dir_stat = self.exp_path.stat()

        # Get experiment id
        self.exp_id: int = get_experiment_id(self.expid)

        # Get experiment config
        self.as_conf = AutosubmitConfig(self.expid, BasicConfig, YAMLParserFactory())
        self.as_conf.reload()

    def save_update_details(self):
        """
        Save the details of the experiment to the database.
        This method will upsert the details into the database.
        """
        # Upsert the details into the database
        self._details_repo.upsert_details(
            self.exp_id, self.user, self.created, self.model, self.branch, self.hpc
        )

    def delete_details(self):
        """
        Delete the details of the experiment from the database.
        """
        self._details_repo.delete_details(self.exp_id)

    @property
    def user(self) -> str:
        """
        Get the user that created the experiment. This is obtained from the
        experiment directory stat information.
        """
        return pwd.getpwuid(self.exp_dir_stat.st_uid).pw_name

    @property
    def created(self) -> str:
        """
        Get the creation date of the experiment. This is obtained from the
        experiment directory stat information.
        """
        return datetime.datetime.fromtimestamp(
            int(self.exp_dir_stat.st_ctime), tz=LOCAL_TZ
        ).isoformat()

    @property
    def model(self) -> str:
        """
        Get the model of the experiment. This is obtained from the
        Autosubmit configuration.
        """
        project_type = self.as_conf.get_project_type()
        if project_type == "git":
            return self.as_conf.get_git_project_origin()
        else:
            return "NA"

    @property
    def branch(self) -> str:
        """
        Get the branch of the experiment. This is obtained from the
        Autosubmit configuration.
        """
        project_type = self.as_conf.get_project_type()
        if project_type == "git":
            return self.as_conf.get_git_project_branch()
        else:
            return "NA"

    @property
    def hpc(self) -> str:
        """
        Get the HPC of the experiment. This is obtained from the
        Autosubmit configuration.
        """
        try:
            return self.as_conf.get_platform()
        except Exception:
            return "NA"
