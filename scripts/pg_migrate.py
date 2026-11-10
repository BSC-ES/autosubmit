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

import argparse
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from migrate import migrate_utils
from sqlalchemy import create_engine, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import tables


def get_paths_from_rc_files(
    old_rc_file: str, new_rc_file: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    paths = []
    print(f"Reading old rc file: {old_rc_file}")

    for rc_file in [old_rc_file, new_rc_file]:
        os.environ["AUTOSUBMIT_CONFIGURATION"] = rc_file
        BasicConfig.read()
        paths.append(
            {
                "LOCAL_ROOT_DIR": BasicConfig.LOCAL_ROOT_DIR,
                "JOBDATA_DIR": BasicConfig.JOBDATA_DIR,
                "STRUCTURES_DIR": BasicConfig.STRUCTURES_DIR,
                "LOCAL_TMP_DIR": BasicConfig.LOCAL_TMP_DIR,
                "DATABASE_CONN_URL": BasicConfig.DATABASE_CONN_URL,
                "DB_DIR": BasicConfig.DB_DIR,
                "DB_FILE": BasicConfig.DB_FILE,
                "DB_PATH": BasicConfig.DB_PATH,
            }
        )

    return paths[0], paths[1]


def copy_files(old_paths: dict, new_paths: dict, expid: str):
    old_exp_dir = Path(old_paths["LOCAL_ROOT_DIR"]) / expid
    new_exp_dir = Path(new_paths["LOCAL_ROOT_DIR"]) / expid

    if not old_exp_dir.exists():
        raise FileNotFoundError(f"Source directory {old_exp_dir} does not exist.")

    new_exp_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            "rsync",
            "-r",
            "--exclude=*.db",
            "--exclude=*.pkl",
            str(old_exp_dir) + "/",
            str(new_exp_dir) + "/",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"rsync command failed: {result.stderr}")
    else:
        print("rsync completed successfully.")


def copy_database(old_paths: dict, new_paths: dict, expid: str):
    conn_url = new_paths["DATABASE_CONN_URL"]
    engine = create_engine(conn_url)

    with engine.connect() as pg_conn:
        # Base dirs
        old_exp_dir = Path(old_paths["LOCAL_ROOT_DIR"]) / expid

        # Migrate Job data
        job_data_db = Path(old_paths["JOBDATA_DIR"]) / f"job_data_{expid}.db"
        if job_data_db.exists():
            print(f"Migrating job data from {job_data_db} to PostgreSQL.")
            source_db = create_engine(f"sqlite:///{job_data_db}")

            with source_db.connect() as sqlite_conn:
                job_data_table = migrate_utils.check_table_schema(
                    sqlite_conn, [tables.JobDataTable, migrate_utils.OldJobDataTable]
                )
                migrate_utils._copy_table_data(
                    sqlite_conn,
                    pg_conn,
                    expid,
                    job_data_table,
                    dest_table=tables.JobDataTable,
                    schema_required=True,
                )
                migrate_utils._copy_table_data(
                    sqlite_conn,
                    pg_conn,
                    expid,
                    tables.ExperimentRunTable,
                    schema_required=True,
                )
                pg_conn.commit()
        else:
            print(
                f"Job data database {job_data_db} does not exist. Skipping job data migration."
            )

        # Migrate structures
        struct_db = Path(old_paths["STRUCTURES_DIR"]) / f"structure_{expid}.db"
        if struct_db.exists():
            print(f"Migrating structures from {struct_db} to PostgreSQL.")
            source_db = create_engine(f"sqlite:///{struct_db}")

            with source_db.connect() as sqlite_conn:
                migrate_utils._copy_table_data(
                    sqlite_conn,
                    pg_conn,
                    expid,
                    tables.ExperimentStructureTable,
                    schema_required=True,
                )
                pg_conn.commit()
        else:
            print(
                f"Structure database {struct_db} does not exist. Skipping structure migration."
            )

        # Migrate packages
        job_pkg_db = old_exp_dir / "pkl" / f"job_packages_{expid}.db"
        if job_pkg_db.exists():
            print(f"Migrating job packages from {job_pkg_db} to PostgreSQL.")
            source_db = create_engine(f"sqlite:///{job_pkg_db}")

            with source_db.connect() as sqlite_conn:
                migrate_utils._copy_table_data(
                    sqlite_conn,
                    pg_conn,
                    expid,
                    tables.JobPackageTable,
                    schema_required=True,
                )
                migrate_utils._copy_table_data(
                    sqlite_conn,
                    pg_conn,
                    expid,
                    tables.WrapperJobPackageTable,
                    schema_required=True,
                )
                pg_conn.commit()
        else:
            print(
                f"Job package database {job_pkg_db} does not exist. Skipping job package migration."
            )

        # Migrate user metrics
        user_metrics_db = (
            old_exp_dir / old_paths["LOCAL_TMP_DIR"] / f"metrics_{expid}.db"
        )
        if user_metrics_db.exists():
            print(f"Migrating user metrics from {user_metrics_db} to PostgreSQL.")
            source_db = create_engine(f"sqlite:///{user_metrics_db}")

            with source_db.connect() as sqlite_conn:
                migrate_utils._copy_table_data(
                    sqlite_conn,
                    pg_conn,
                    expid,
                    tables.UserMetricsTable,
                    schema_required=True,
                )
                pg_conn.commit()
        else:
            print(
                f"User metrics database {user_metrics_db} does not exist. Skipping user metrics migration."
            )

        # Migrate public info
        main_db = Path(old_paths["DB_PATH"])
        if main_db.exists():
            print(f"Migrating main database from {main_db} to PostgreSQL.")
            source_db = create_engine(f"sqlite:///{main_db}")

            with source_db.connect() as sqlite_conn:
                exp_row = sqlite_conn.execute(
                    select(tables.ExperimentTable).where(
                        tables.ExperimentTable.c.name == expid
                    )
                ).first()
                if exp_row:
                    pg_conn.execute(
                        tables.ExperimentTable.delete().where(
                            tables.ExperimentTable.c.id == exp_row.id
                            and tables.ExperimentTable.c.name == expid
                        )
                    )
                    pg_conn.execute(
                        tables.ExperimentTable.insert().values(
                            id=exp_row.id,
                            name=exp_row.name,
                            description=exp_row.description,
                            autosubmit_version=exp_row.autosubmit_version,
                        )
                    )
                    pg_conn.commit()

                    details_row = sqlite_conn.execute(
                        select(tables.DetailsTable).where(
                            tables.DetailsTable.c.exp_id == exp_row.id
                        )
                    ).first()
                    if details_row:
                        pg_conn.execute(
                            tables.DetailsTable.delete().where(
                                tables.DetailsTable.c.exp_id == details_row.exp_id
                            )
                        )
                        pg_conn.execute(
                            tables.DetailsTable.insert().values(
                                exp_id=details_row.exp_id,
                                user=details_row.user,
                                created=details_row.created,
                                model=details_row.model,
                                branch=details_row.branch,
                                hpc=details_row.hpc,
                            )
                        )
                        pg_conn.commit()
                    else:
                        print(f"Details for experiment {expid} not found. Skipping.")

                else:
                    print(f"Experiment {expid} not found in main database. Skipping.")

        else:
            print(
                f"Main database {main_db} does not exist. Skipping main database migration."
            )

        # Migrate pkl
        job_pkl_path = old_exp_dir / "pkl" / f"job_list_{expid}.pkl"
        if job_pkl_path.exists():
            print(f"Migrating job pickle from {job_pkl_path} to PostgreSQL.")
            try:
                with open(job_pkl_path, "rb") as f:
                    pkl_data = f.read()

                pg_conn.execute(
                    tables.JobPklTable.delete().where(
                        tables.JobPklTable.c.expid == expid
                    )
                )
                pg_conn.execute(
                    tables.JobPklTable.insert().values(
                        expid=expid,
                        pkl=pkl_data,
                        modified=datetime.fromtimestamp(
                            os.path.getmtime(job_pkl_path), tz=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S%z"),
                    )
                )
                pg_conn.commit()
            except Exception as e:
                print(f"Error migrating job pickle: {e}")
        else:
            print(
                f"Job pickle file {job_pkl_path} does not exist. Skipping job pickle migration."
            )


def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Migrate PostgreSQL database.")
    parser.add_argument(
        "expid", type=str, help="Experiment identifier (schema name in PostgreSQL)."
    )
    parser.add_argument(
        "--old-rc-file",
        type=str,
        required=True,
        help="Path to the SQLite autosubmitrc file.",
    )
    parser.add_argument(
        "--new-rc-file",
        type=str,
        required=True,
        help="Path to the new PostgreSQL autosubmitrc file.",
    )

    args = parser.parse_args()

    # Get paths from rc files
    old_paths, new_paths = get_paths_from_rc_files(args.old_rc_file, args.new_rc_file)
    print("Old paths:", old_paths)
    print("New paths:", new_paths)

    # Copy files excluding .db files
    copy_files(old_paths, new_paths, args.expid)

    # Copy database contents
    copy_database(old_paths, new_paths, args.expid)


if __name__ == "__main__":
    main()
