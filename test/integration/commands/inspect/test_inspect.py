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

from pathlib import Path

import pytest

from autosubmit.config.basicconfig import BasicConfig
from bscearth.utils.date import date2str

"""Integration tests for argument behavior."""


@pytest.fixture(scope="function")
def templates_dir(as_exp) -> Path:
    """Return the path to the templates directory for the given experiment."""
    as_conf = as_exp.as_conf
    return (
        Path(as_conf.basic_config.LOCAL_ROOT_DIR)
        / as_exp.expid
        / BasicConfig.LOCAL_TMP_DIR
    )


def cleanup_cmds(templates_dir: Path) -> None:
    """Remove any .cmd files from the templates directory."""
    if templates_dir.exists():
        for f in templates_dir.glob("*.cmd"):
            try:
                f.unlink()
            except Exception:
                pass


# TODO: this will not be necessary once the lock file is deleted correctly
def cleanup_lock(as_exp) -> None:
    """Remove the inspect lock file if the fixture created one."""
    lock_file = (
        Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR)
        / as_exp.expid
        / BasicConfig.LOCAL_TMP_DIR
        / "autosubmit.lock"
    )
    if lock_file.exists():
        try:
            lock_file.unlink()
        except Exception:
            pass


def do_inspect(
    as_exp, fl=None, fc=None, fs=None, ft=None, quick=False, check_wrapper=False
):
    """Call the inspect command with the given filters and return the list of generated .cmd files."""
    as_exp.as_conf.set_last_as_command("inspect")
    templates = (
        Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR)
        / as_exp.expid
        / BasicConfig.LOCAL_TMP_DIR
    )
    cleanup_cmds(templates)
    cleanup_lock(as_exp)

    as_exp.autosubmit.inspect(
        expid=as_exp.expid,
        lst=fl,
        filter_chunks=fc,
        filter_status=fs,
        filter_section=ft,
        force=False,
        check_wrapper=check_wrapper,
        quick=quick,
    )

    return [p.name for p in templates.glob("*.cmd")]


def test_inspect_combined_filters(as_exp, mocker, templates_dir):
    """Test that when inspect and multiple filters are used, selected jobs must match the intersecion of the filters."""
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)

    target = next(
        job
        for job in job_list.get_job_list()
        if job.section == "LOCALJOB"
        and date2str(job.date) == "20200101"
        and job.member == "fc0"
        and str(job.chunk) == "1"
        and str(job.split) == "2"
    )
    target_job = target.name
    no_matching_job = next(
        job.name for job in job_list.get_job_list() if job.name != target_job
    )
    filter_list = f"{target_job} {no_matching_job}"

    captured_jobs = {}

    def capture_generate_scripts(
        as_conf, job_list_obj, jobs_filtered, packages_persistence, only_wrappers=False
    ):
        captured_jobs["names"] = [job.name for job in jobs_filtered]

    mocker.patch(
        "autosubmit.autosubmit.Autosubmit.generate_scripts_andor_wrappers",
        side_effect=capture_generate_scripts,
    )

    do_inspect(
        as_exp,
        fl=filter_list,
        fc="[20200101 [fc0 [1] ] ]",
        ft="LOCALJOB",
        fs="WAITING",
    )

    assert captured_jobs["names"] == [target_job]


def test_inpect_no_filters_selects_all_jobs(as_exp, mocker):
    """Test that when inspect is called without filters, all jobs are selected."""
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)
    all_job_names = [job.name for job in job_list.get_job_list()]

    captured_jobs = {}

    def capture_generate_scripts(
        as_conf, job_list_obj, jobs_filtered, packages_persistence, only_wrappers=False
    ):
        captured_jobs["names"] = [job.name for job in jobs_filtered]

    mocker.patch(
        "autosubmit.autosubmit.Autosubmit.generate_scripts_andor_wrappers",
        side_effect=capture_generate_scripts,
    )

    do_inspect(as_exp)

    assert set(captured_jobs["names"]) == set(all_job_names)


def test_check_wrappers_selects_uncompleted_jobs(as_exp, mocker):
    """Test that when inspect is called with check_wrapper=True, the uncompleted jobs are selected."""
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)
    expected_jobs = {job.name for job in job_list.get_uncompleted()}

    captured_jobs = {}

    def capture_generate_scripts(
        as_conf, job_list_obj, jobs_filtered, packages_persistence, only_wrappers=False
    ):
        captured_jobs["names"] = [job.name for job in jobs_filtered]

    mocker.patch(
        "autosubmit.autosubmit.Autosubmit.generate_scripts_andor_wrappers",
        side_effect=capture_generate_scripts,
    )

    do_inspect(as_exp, ft="LOCALJOB", check_wrapper=True)

    assert set(captured_jobs["names"]) == expected_jobs
