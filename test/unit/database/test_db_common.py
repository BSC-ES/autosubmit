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

"""Unit tests for ``autosubmit.database.db_common``.

We cover mainly error and validation scenarios here.
"""

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy.exc import IntegrityError

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import db_common
from autosubmit.log.log import AutosubmitCritical


def test_install_creates_schema_migrations(autosubmit_config) -> None:
    """install() creates the general database with a recorded schema version."""
    autosubmit_config("a000", {})

    conn = sqlite3.connect(BasicConfig.DB_PATH)
    try:
        rows = conn.execute("SELECT version FROM general_schema_migrations").fetchall()
    finally:
        conn.close()

    assert rows == [(db_common.CURRENT_DATABASE_VERSION,)]


@pytest.mark.parametrize(
    "fn,args",
    [
        ("check_db", ()),
        ("check_experiment_exists", ("a000",)),
        ("delete_experiment", ("a000",)),
        ("get_autosubmit_version", ("a000",)),
        ("get_experiment_description", ("a000",)),
        ("get_experiment_expids", ()),
        ("get_experiment_id", ("a000",)),
        ("last_name_used", ()),
        ("save_experiment", ("a000", "desc", "4.0.0")),
        ("update_experiment_description_version", ("a000",)),
    ],
)
def test_db_common_raises_without_database(monkeypatch, tmp_path, fn: str, args: tuple) -> None:
    """Every database operation raises when the general database is missing."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "missing.db"))

    with pytest.raises(AutosubmitCritical):
        getattr(db_common, fn)(*args)


@pytest.mark.parametrize(
    "exception",
    [
        IntegrityError("stmt", {}, Exception("boom")),
        Exception("boom"),
    ],
    ids=["integrity-error", "generic-exception"],
)
def test_save_experiment_wraps_insert_errors(mocker, monkeypatch, tmp_path, exception) -> None:
    """``save_experiment`` wraps insert errors in an ``AutosubmitCritical``."""
    db_file = tmp_path / "autosubmit.db"
    db_file.touch()
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))
    mocker.patch("autosubmit.database.db_common._get_engine", side_effect=exception)

    with pytest.raises(AutosubmitCritical):
        db_common.save_experiment("a000", "desc", "4.0.0")


def test_create_db_wraps_errors(mocker, monkeypatch, tmp_path) -> None:
    """``create_db`` wraps creation errors in an ``AutosubmitCritical``."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "autosubmit.db"))
    mocker.patch("autosubmit.database.db_common._get_engine", side_effect=Exception("boom"))

    with pytest.raises(AutosubmitCritical):
        db_common.create_db()


@pytest.mark.parametrize("error_on_inexistence, create_folder, expect_raises", [
    (True, False, True),
    (False, True, False),
])
def test_check_experiment_exists_missing(autosubmit_config, error_on_inexistence, create_folder, expect_raises) -> None:
    """A missing experiment either raises or is registered from the local filesystem."""
    autosubmit_config("a000", {})
    name = "a999"
    if create_folder:
        Path(BasicConfig.LOCAL_ROOT_DIR, name).mkdir(parents=True, exist_ok=True)

    if expect_raises:
        with pytest.raises(AutosubmitCritical):
            db_common.check_experiment_exists(name, error_on_inexistence=error_on_inexistence)
    else:
        assert db_common.check_experiment_exists(name, error_on_inexistence=error_on_inexistence) is True