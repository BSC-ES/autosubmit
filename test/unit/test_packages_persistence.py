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

"""Tests for autosubmit/job/job_package_persistence.py."""

import pytest
from sqlalchemy.exc import IntegrityError

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_package_persistence import JobPackagePersistence


@pytest.fixture
def mock_db_manager(mocker):
    """Mock DbManager and DB infrastructure for JobPackagePersistence."""
    mock_manager = mocker.MagicMock()
    mocker.patch(
        "autosubmit.job.job_package_persistence.DbManager",
        return_value=mock_manager,
    )
    mocker.patch(
        "autosubmit.job.job_package_persistence.get_connection_url",
        return_value="sqlite:///:memory:",
    )
    return mock_manager


@pytest.fixture
def mock_engine(mocker, mock_db_manager):
    """Mock the SQLAlchemy engine connection."""
    mock_conn = mocker.MagicMock()
    mock_conn.__enter__ = mocker.MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = mocker.MagicMock(return_value=False)
    mock_db_manager.engine.connect.return_value = mock_conn
    return mock_conn


@pytest.fixture
def persistence(mock_db_manager, tmp_path, mocker):
    """Create a JobPackagePersistence instance with mocked DB."""
    from autosubmit.config.basicconfig import BasicConfig

    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))
    return JobPackagePersistence("a000")


def test_init_creates_db_manager(mock_db_manager, tmp_path, mocker):
    """Test __init__ creates a DbManager instance."""
    from autosubmit.config.basicconfig import BasicConfig

    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))
    JobPackagePersistence("a000")

    mock_db_manager.create_table.assert_called()


def test_init_creates_all_tables(mock_db_manager, tmp_path, mocker):
    """Test __init__ creates all required tables."""
    from autosubmit.config.basicconfig import BasicConfig

    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))
    JobPackagePersistence("a000")

    table_names = [c[0][0] for c in mock_db_manager.create_table.call_args_list]
    assert "wrappers_info" in table_names
    assert "preview_wrappers_info" in table_names
    assert "wrappers_jobs" in table_names
    assert "preview_wrappers_jobs" in table_names


def test_init_creates_parent_directory(mocker, tmp_path):
    """Test __init__ creates the pkl directory if it doesn't exist."""
    from autosubmit.config.basicconfig import BasicConfig

    pkl_dir = tmp_path / "a000" / "pkl"
    assert not pkl_dir.exists()

    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))
    mocker.patch("autosubmit.job.job_package_persistence.DbManager")
    mocker.patch(
        "autosubmit.job.job_package_persistence.get_connection_url",
        return_value="sqlite:///:memory:",
    )

    JobPackagePersistence("a000")

    assert pkl_dir.exists()


@pytest.mark.parametrize("preview", [False, True])
def test_save_calls_bulk_insert_inner_jobs(persistence, mock_db_manager, mock_engine, mocker, preview):
    """Test save persists inner jobs via _bulk_insert_inner_jobs for both preview modes."""
    mock_bulk = mocker.patch.object(persistence, '_bulk_insert_inner_jobs')
    status = str(Status.SUBMITTED if not preview else Status.READY)
    wrapper_name = "wrapper1" if not preview else "preview_wrapper"
    wrapper_info = {"name": wrapper_name, "status": status, "job_list": "job1"}
    inner_jobs = [{"job_name": "job1", "package_name": wrapper_name, "package_id": 1}]

    persistence.save(wrappers=[(wrapper_info, inner_jobs)], preview=preview)

    mock_bulk.assert_called_once()


def test_save_with_multiple_wrapper_infos(persistence, mock_db_manager, mock_engine, mocker):
    """Test save handles wrapper_info as a list of dicts."""
    mock_bulk = mocker.patch.object(persistence, '_bulk_insert_inner_jobs')
    wrapper_info = [
        {"name": "w1", "status": str(Status.SUBMITTED), "job_list": "job1"},
        {"name": "w2", "status": str(Status.READY), "job_list": "job2"},
    ]
    inner_jobs = [
        {"job_name": "job1", "package_name": "w1", "package_id": 1},
        {"job_name": "job2", "package_name": "w2", "package_id": 2},
    ]

    persistence.save(wrappers=[(wrapper_info, inner_jobs)], preview=False)

    mock_bulk.assert_called_once()


def test_save_logs_warning_on_integrity_error(persistence, mock_db_manager, mock_engine, mocker):
    """Test save logs a warning when IntegrityError is raised."""
    mock_warning = mocker.patch("autosubmit.job.job_package_persistence.Log.warning")
    mocker.patch.object(
        persistence, '_bulk_insert_inner_jobs',
        side_effect=IntegrityError("unique constraint", {}, None),
    )

    wrapper_info = {"name": "wrapper1", "status": str(Status.SUBMITTED), "job_list": "job1"}
    inner_jobs = [{"job_name": "job1", "package_name": "wrapper1", "package_id": 1}]

    persistence.save(wrappers=[(wrapper_info, inner_jobs)], preview=False)

    mock_warning.assert_called_once()
    assert "Unique constraint failed" in mock_warning.call_args[0][0]


@pytest.mark.parametrize("preview,job_list", [
    (True, None),
    (False, None),
])
def test_load_returns_lists(persistence, mock_db_manager, mock_engine, preview, job_list):
    """Test load returns lists for both preview and non-preview modes."""
    mock_engine.execute.return_value.fetchall.return_value = []

    wrappers_info, inner_jobs = persistence.load(preview=preview, job_list=job_list)

    assert isinstance(wrappers_info, list)
    assert isinstance(inner_jobs, list)


def test_load_filters_by_job_list(persistence, mock_db_manager, mock_engine):
    """Test load filters results by job_list names when preview=False."""
    mock_engine.execute.return_value.fetchall.return_value = []
    job1 = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job2 = Job(name="job2", job_id=2, status=Status.READY, priority=0)

    wrappers_info, inner_jobs = persistence.load(preview=False, job_list=[job1, job2])

    assert isinstance(wrappers_info, list)
    assert isinstance(inner_jobs, list)


@pytest.mark.parametrize("preview,expected_drops", [
    (True, ["preview_wrappers_jobs"]),
    (False, ["wrappers_info", "wrappers_jobs"]),
])
def test_reset_table(persistence, mock_db_manager, preview, expected_drops):
    """Test reset_table drops the correct tables for both preview modes."""
    persistence.reset_table(preview=preview)

    drop_calls = [c[0][0] for c in mock_db_manager.drop_table.call_args_list]
    for table in expected_drops:
        assert table in drop_calls


@pytest.mark.parametrize("preview", [False, True])
def test_upsert_wrapper_info_does_not_insert_inner_jobs(
        persistence, mock_db_manager, mock_engine, mocker, preview
):
    """Test upsert_wrapper_info does not call insert_many on inner jobs table."""
    mock_warning = mocker.patch("autosubmit.job.job_package_persistence.Log.warning")
    mock_bulk = mocker.patch.object(persistence, '_bulk_insert_inner_jobs')

    wrapper_info_list = [
        {"name": "w1", "status": str(Status.SUBMITTED)},
        {"name": "w2", "status": str(Status.READY)},
    ]

    persistence.upsert_wrapper_info(wrapper_info_list, preview=preview)

    mock_db_manager.insert_many.assert_not_called()
    mock_bulk.assert_not_called()
    mock_warning.assert_not_called()


def test_upsert_wrapper_info_empty_list_returns_early(
        persistence, mock_db_manager, mock_engine
):
    """Test upsert_wrapper_info with empty list does not touch the database."""
    create_count_before = mock_db_manager.create_table.call_count

    persistence.upsert_wrapper_info([], preview=False)

    assert mock_db_manager.create_table.call_count == create_count_before
