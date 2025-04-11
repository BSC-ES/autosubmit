import datetime
import os
from pathlib import Path
import sqlite3
from autosubmit.database.db_common import get_experiment_id
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


LOCAL_TZ = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo


class ExperimentDetailsRepository:
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
    def __init__(self, expid: str, init_reload: bool = True):
        self.expid = expid
        self._details_repo = ExperimentDetailsRepository()
        if init_reload:
            self.reload()

    def reload(self):
        # Build path stat
        self.exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(self.expid)
        self.exp_dir_stat = self.exp_path.stat()

        # Get experiment id
        self.exp_id: int = get_experiment_id(self.expid)

        # Get experiment config
        self.as_conf = AutosubmitConfig(self.expid, BasicConfig, YAMLParserFactory())
        self.as_conf.reload()

    def save_update_details(self):
        # Upsert the details into the database
        self._details_repo.upsert_details(
            self.exp_id, self.user, self.created, self.model, self.branch, self.hpc
        )

    def delete_details(self):
        self._details_repo.delete_details(self.exp_id)

    @property
    def user(self) -> str:
        stdout = os.popen("id -nu {0}".format(str(int(self.exp_dir_stat.st_uid))))
        owner_name = stdout.read().strip()
        return str(owner_name)

    @property
    def created(self) -> str:
        return datetime.datetime.fromtimestamp(
            int(self.exp_dir_stat.st_ctime), tz=LOCAL_TZ
        ).isoformat()

    @property
    def model(self) -> str:
        project_type = self.as_conf.get_project_type()
        if project_type == "git":
            return self.as_conf.get_git_project_origin()
        elif project_type == "svn":
            return self.as_conf.get_svn_project_url()
        else:
            return "NA"

    @property
    def branch(self) -> str:
        project_type = self.as_conf.get_project_type()
        if project_type == "git":
            return self.as_conf.get_git_project_branch()
        elif project_type == "svn":
            return self.as_conf.get_svn_project_url()
        else:
            return "NA"

    @property
    def hpc(self) -> str:
        try:
            return self.as_conf.get_platform()
        except Exception:
            return "NA"
