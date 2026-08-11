# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Code to install and set up Autosubmit on a new environment."""

import os
from configparser import ConfigParser
from importlib.resources import files
from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import create_db
from autosubmit.helpers.utils import get_rc_path
from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log

__all__ = ["create_required_directories", "install"]


_DEFAULT_DIRECTORY_MODE = 0o775


def _get_required_directories() -> dict[Path, int]:
    """Return the directories required by Autosubmit installation.

    :return: Mapping of directory paths to their required permissions.
    """
    return {
        Path(BasicConfig.LOCAL_ROOT_DIR): _DEFAULT_DIRECTORY_MODE,
        Path(BasicConfig.GLOBAL_LOG_DIR): _DEFAULT_DIRECTORY_MODE,
        Path(BasicConfig.STRUCTURES_DIR): _DEFAULT_DIRECTORY_MODE,
        Path(BasicConfig.JOBDATA_DIR): _DEFAULT_DIRECTORY_MODE,
        Path(BasicConfig.HISTORICAL_LOG_DIR): _DEFAULT_DIRECTORY_MODE,
        Path(BasicConfig.DB_DIR): _DEFAULT_DIRECTORY_MODE,
    }


def _create_directories(directories: dict[Path, int]) -> None:
    """Create directories and set their permissions.

    :param directories: Mapping of directory paths to their required permissions.
    :raises OSError: If a directory cannot be created or its permissions cannot
        be set.
    """
    for path, mode in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(mode)


def create_required_directories() -> None:
    """Create the directories required by Autosubmit.

    :raises OSError: If a directory cannot be created or its permissions cannot
        be set.
    """
    directories = _get_required_directories()
    _create_directories(directories)

    Log.result(
        "\n".join(
            [
                "Directories have been created and configured successfully:",
                *map(str, directories),
            ]
        )
    )


def _create_sqlite_database() -> bool:
    """Create the Autosubmit SQLite database.

    :return: ``True`` if the database was created successfully, otherwise
        ``False`` if the database already exists or could not be created.
    """
    database_path = Path(BasicConfig.DB_PATH)

    if database_path.exists():
        Log.error("Database already exists.")
        return False

    Log.info("Creating autosubmit database...")

    query_file = files("autosubmit.database") / "data/autosubmit.sql"
    query = query_file.read_text()

    if not create_db(query):
        Log.error("Can not write database file")
        return False

    Log.result("Autosubmit database created successfully")
    return True


def _create_postgres_database() -> bool:
    """Create the Autosubmit PostgreSQL database.

    :return: ``True`` if the database was created successfully, otherwise
        ``False`` if the database could not be created.
    """
    Log.info("Creating autosubmit Postgres database...")

    if not create_db(""):
        Log.error("Failed to create Postgres database")
        return False

    return True


def _create_database() -> bool:
    """Create the configured Autosubmit database.

    :return: ``True`` if the database was created successfully, otherwise
        ``False`` if the database could not be created.
    """
    if BasicConfig.DATABASE_BACKEND == "sqlite":
        return _create_sqlite_database()

    return _create_postgres_database()


def install() -> bool:
    """Create the directories and database required by Autosubmit.

    :return: ``True`` if the installation completed successfully, otherwise
        ``False`` if the database could not be created.
    :raises OSError: If a required directory cannot be created or its
        permissions cannot be set.
    """
    create_required_directories()
    return _create_database()


def configure(
    advanced,
    database_path,
    database_filename,
    local_root_path,
    platforms_conf_path,
    jobs_conf_path,
    smtp_hostname,
    mail_from,
    machine: bool,
    local: bool,
    database_backend: str = "sqlite",
    database_conn_url: str | None = None,
):
    """Configure several paths for autosubmit: database, local root and others. Can be configured at system,
    user or local levels. Local level configuration precedes user level and user level precedes system
    configuration.

    :param advanced:
    :param database_path: path to autosubmit database
    :param database_filename: database filename
    :param local_root_path: path to autosubmit's experiments' directory
    :param platforms_conf_path: path to platforms conf file to be used as model for new experiments
    :param jobs_conf_path: path to jobs conf file to be used as model for new experiments
    :param machine: True if this configuration has to be stored for all the machine users
    :param local: True if this configuration has to be stored in the local path
    :param mail_from:
    :param smtp_hostname:
    :param database_backend: The system database backend. Defaults to sqlite.
    :param database_conn_url: The database connection URL.
    """
    try:
        home_path = Path.home()
        autosubmitapi_url = "http://192.168.11.91:8081" + " # Replace me?"
        # Setting default values
        if not advanced and database_path is None and local_root_path is None:
            database_path = home_path / "autosubmit"
            local_root_path = home_path / "autosubmit"
            global_logs_path = home_path / "autosubmit/logs"
            structures_path = home_path / "autosubmit/metadata/structures"
            historicdb_path = home_path / "autosubmit/metadata/data"
            historiclog_path = home_path / "autosubmit/metadata/logs"
            database_filename = "autosubmit.db"

        if database_backend == "sqlite":
            while database_path is None:
                database_path = input("Introduce Database path: ")
                if database_path.find("~/") < 0:
                    database_path = None
                    Log.error(
                        "Not a valid path. You must include '~/' at the beginning."
                    )
            database_path = Path(database_path).expanduser().resolve()
            # if not os.path.exists(database_path):
            # Log.error("Database path does not exist.")
            # return False
            while database_filename is None:
                database_filename = input("Introduce Database name: ")

        while local_root_path is None:
            local_root_path = input("Introduce path to experiments: ")
            if local_root_path.find("~/") < 0:
                local_root_path = None
                Log.error("Not a valid path. You must include '~/' at the beginning.")
        local_root_path = Path(local_root_path).expanduser().resolve()

        global_logs_path = local_root_path / "logs"
        structures_path = local_root_path / "metadata/structures"
        historicdb_path = local_root_path / "metadata/data"
        historiclog_path = local_root_path / "metadata/logs"

        if platforms_conf_path is not None and len(str(platforms_conf_path)) > 0:
            platforms_conf_path = Path(platforms_conf_path).expanduser().resolve()
            if not platforms_conf_path.exists():
                Log.error("platforms.yml path does not exist.")
                return False
        if jobs_conf_path is not None and len(str(jobs_conf_path)) > 0:
            jobs_conf_path = Path(jobs_conf_path).expanduser().resolve()
            if not os.path.exists(jobs_conf_path):
                Log.error("jobs.yml path does not exist.")
                return False

        rc_path: Path = get_rc_path(machine, local)

        with open(rc_path, "w") as config_file:
            Log.info("Writing configuration file...")
            try:
                parser = ConfigParser()
                parser.add_section("database")
                parser.set("database", "backend", database_backend)
                if database_backend == "postgres":
                    if database_conn_url is None:
                        raise AutosubmitCritical(
                            "You must provide a connection URL for the PostgreSQL database. "
                            "Try adding --database-conn-url=[YOUR_URL] to your configure command.",
                            7014,
                        )
                    parser.set("database", "connection_url", str(database_conn_url))
                else:
                    parser.set("database", "path", str(database_path))
                    if (
                        database_filename is not None
                        and len(str(database_filename)) > 0
                    ):
                        parser.set("database", "filename", str(database_filename))
                parser.add_section("local")
                parser.set("local", "path", str(local_root_path))
                if (jobs_conf_path is not None and len(str(jobs_conf_path)) > 0) or (
                    platforms_conf_path is not None
                    and len(str(platforms_conf_path)) > 0
                ):
                    parser.add_section("conf")
                    if jobs_conf_path is not None:
                        parser.set("conf", "jobs", str(jobs_conf_path))
                    if platforms_conf_path is not None:
                        parser.set("conf", "platforms", str(platforms_conf_path))
                if smtp_hostname is not None or mail_from is not None:
                    parser.add_section("mail")
                    parser.set("mail", "smtp_server", smtp_hostname)
                    parser.set("mail", "mail_from", mail_from)
                parser.add_section("globallogs")
                parser.set("globallogs", "path", str(global_logs_path))
                parser.add_section("structures")
                parser.set("structures", "path", str(structures_path))
                parser.add_section("historicdb")
                parser.set("historicdb", "path", str(historicdb_path))
                parser.add_section("historiclog")
                parser.set("historiclog", "path", str(historiclog_path))
                parser.add_section("autosubmitapi")
                parser.set("autosubmitapi", "url", autosubmitapi_url)
                parser.write(config_file)
                Log.result(f"Configuration file written successfully: \n\t{rc_path}")

                paths_info = [
                    f"Local root path: {local_root_path}",
                    f"Database path: {database_path}",
                    f"Global logs path: {global_logs_path}",
                    f"Structures path: {structures_path}",
                    f"Historic DB path: {historicdb_path}",
                    f"Historic logs path: {historiclog_path}",
                ]
                sep = "\n\t- "
                Log.result(
                    sep.join(
                        ["Directories added to the configuration file:"] + paths_info
                    )
                )

            except OSError as e:
                raise AutosubmitCritical(
                    f"Can not write config file: {e.message}", 7012
                )
            except OSError as e:
                raise AutosubmitCritical(f"Can not write config file: {e}", 7012)
    except (AutosubmitCritical, AutosubmitError):
        raise
    except Exception as e:
        raise AutosubmitCritical(str(e), 7014)
    return True
