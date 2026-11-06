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

"""Unit tests for the set_status command with the updated_list flag."""

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.manage import set_status


@pytest.fixture
def mock_job_setup(mocker, tmp_path):
    """Set up a mock experiment structure with jobs."""
    expid = "a000"
    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))

    exp_dir = tmp_path / expid
    status_dir = exp_dir / "status"
    status_dir.mkdir(parents=True, exist_ok=True)

    job1 = mocker.MagicMock(spec=Job)
    job1.name = "a000_19600101_fc0_1_SIM"
    job1.status = Status.WAITING
    job1.section = "SIM"

    job2 = mocker.MagicMock(spec=Job)
    job2.name = "a000_19600101_fc0_2_SIM"
    job2.status = Status.WAITING
    job2.section = "SIM"

    job_list = mocker.MagicMock()
    job_list.get_job_list.return_value = [job1, job2]
    job_list.get_job_by_name.side_effect = lambda name: {
        job1.name: job1,
        job2.name: job2,
    }.get(name)

    mocker.patch(
        "autosubmit.job.manage.load_job_list", return_value=job_list
    )

    mock_as_conf = mocker.MagicMock()
    mock_as_conf.get_output_type.return_value = "pdf"
    mock_as_conf.get_platform.return_value = "marenostrum"
    mock_as_conf.jobs_data = {}
    mocker.patch("autosubmit.config.registry.load_config", return_value=mock_as_conf)
    mocker.patch("autosubmit.job.manage.validate_job_filters")
    mocker.patch("autosubmit.job.manage.restore_platforms")
    mocker.patch("autosubmit.job.manage.ExperimentHistory")

    return expid, [job1, job2], job_list, status_dir


def test_set_status_updated_list_appends_to_file_when_saved(mock_job_setup, mocker):
    """Verify that set_status with updated_list and save writes to the updated_list file."""
    expid, jobs, job_list, status_dir = mock_job_setup
    mock_restore = mocker.patch("autosubmit.job.manage.restore_platforms")

    result = set_status(
        expid=expid,
        noplot=True,
        save=True,
        final="SUSPENDED",
        filter_list=f"{jobs[0].name} {jobs[1].name}",
        filter_chunks="",
        filter_status="",
        filter_section="",
        filter_type_chunk="",
        filter_type_chunk_split="",
        hide=True,
        updated_list=True,
    )

    assert result is True
    update_file = status_dir / f"updated_list_{expid}.txt"
    assert update_file.exists()
    content = update_file.read_text(encoding="utf-8")
    expected = f"{jobs[0].name} SUSPENDED\n{jobs[1].name} SUSPENDED\n"
    assert content == expected

    # Platform restoration and job list saving must not be called
    mock_restore.assert_not_called()
    job_list.save_jobs.assert_not_called()


def test_set_status_updated_list_appends_to_existing_file(mock_job_setup):
    """Verify that existing entries in updated_list are preserved."""
    expid, jobs, _, status_dir = mock_job_setup
    update_file = status_dir / f"updated_list_{expid}.txt"
    update_file.write_text("a000_19600101_fc0_0_INI COMPLETED\n", encoding="utf-8")

    result = set_status(
        expid=expid,
        noplot=True,
        save=True,
        final="SUSPENDED",
        filter_list=jobs[0].name,
        filter_chunks="",
        filter_status="",
        filter_section="",
        filter_type_chunk="",
        filter_type_chunk_split="",
        hide=True,
        updated_list=True,
    )

    assert result is True
    content = update_file.read_text(encoding="utf-8")
    assert content == (
        "a000_19600101_fc0_0_INI COMPLETED\n"
        f"{jobs[0].name} SUSPENDED\n"
    )


def test_set_status_updated_list_dry_run_does_not_write(mock_job_setup):
    """Verify that when save=False, the updated_list file is not created."""
    expid, jobs, _, status_dir = mock_job_setup
    update_file = status_dir / f"updated_list_{expid}.txt"

    result = set_status(
        expid=expid,
        noplot=True,
        save=False,
        final="SUSPENDED",
        filter_list=jobs[0].name,
        filter_chunks="",
        filter_status="",
        filter_section="",
        filter_type_chunk="",
        filter_type_chunk_split="",
        hide=True,
        updated_list=True,
    )

    assert result is True
    assert not update_file.exists()
