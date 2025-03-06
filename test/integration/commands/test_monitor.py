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


import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.database_managers.experiment_history_db_manager import (
    SqlAlchemyExperimentHistoryDbManager,
)

from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
import pytest
from .conftest import wrapped_jobs
import sqlite3

def reset(as_exp_, target="WAITING"):
    job_list_ = as_exp_.autosubmit.load_job_list(
        as_exp_.expid, as_exp_.as_conf, new=False
    )

    job_names = " ".join([job.name for job in job_list_.get_job_list()])
    as_exp_.autosubmit.set_status(
        as_exp_.expid,
        noplot=True,
        save=True,
        final=target,
        filter_list=job_names,
        filter_chunks=None,
        filter_status=None,
        filter_section=None,
        filter_type_chunk=None,
        filter_type_chunk_split=None,
        hide=False,
        group_by=None,
        expand=[],
        expand_status=[],
        check_wrapper=False,
        detail=False,
    )

    return as_exp_.autosubmit.load_job_list(as_exp_.expid, as_exp_.as_conf, new=False)


def do_monitor(as_exp, fl=None, fc=None, fs=None, ft=None):
    return as_exp.autosubmit.monitor(
        expid=as_exp.expid,
        file_format="pdf",
        lst=fl,
        filter_chunks=fc,
        filter_status=fs,
        filter_section=ft,
        hide=True,
        txt_only=False,
    )


@pytest.mark.docker
@pytest.mark.ssh
@pytest.mark.slurm
def test_monitor_combined_filters(as_exp, mocker):
    """Test that when monitor and multiple filters are used,
    only jobs matching all filters are monitored."""
    db_manager = SqlAlchemyExperimentHistoryDbManager(
        as_exp.expid, BasicConfig.JOBDATA_DIR, f"job_data_{as_exp.expid}.db"
    )
    db_manager.initialize()

    reset(as_exp, target="WAITING")

    target_job = f"{as_exp.expid}_20200101_fc0_1_1_LOCALJOB"
    no_matching_job = f"{as_exp.expid}_20200101_fc0_1_1_PSJOB"
    filter_list = f"{target_job} {no_matching_job}"

    mocked_generate_output = mocker.patch(
        "autosubmit.monitor.monitor.Monitor.generate_output"
    )

    do_monitor(
        as_exp,
        fl=filter_list,
        fc="[20200101 [fc0 [1] ] ]",
        fs="WAITING",
        ft="LOCALJOB",
    )

    assert mocked_generate_output.called
    monitored_jobs = mocked_generate_output.call_args.args[1]
    monitored_job_names = [job.name for job in monitored_jobs]

    assert monitored_job_names == [target_job]


@pytest.mark.parametrize("wrapper_type,min_trigger_status,from_step", [
    ('simple', None, None),
    ("simple", "READY", 0),
    ("simple", "QUEUING", 0),
    ("simple", "RUNNING", 0),
    ("simple", "COMPLETED", 0),
    ("simple", "FAILED", 0),
    ("simple", "READY", 3),
    ("simple", "QUEUING", 3),
    ("simple", "RUNNING", 3),
    ("simple", "COMPLETED", 3),
    ("simple", "FAILED", 3),

    ('vertical', None, None),
    ("vertical", "READY", 0),
    ("vertical", "QUEUING", 0),
    ("vertical", "RUNNING", 0),
    ("vertical", "COMPLETED", 0),
    ("vertical", "FAILED", 0),
    ("vertical", "READY", 3),
    ("vertical", "QUEUING", 3),
    ("vertical", "RUNNING", 3),
    ("vertical", "COMPLETED", 3),
    ("vertical", "FAILED", 3),

    ("horizontal", None, None),
    ("horizontal", "READY", 0),
    ("horizontal", "QUEUING", 0),
    ("horizontal", "RUNNING", 0),
    ("horizontal", "COMPLETED", 0),
    ("horizontal", "FAILED", 0),
    ("horizontal", "READY", 3),
    ("horizontal", "QUEUING", 3),
    ("horizontal", "RUNNING", 3),
    ("horizontal", "COMPLETED", 3),
    ("horizontal", "FAILED", 3),

    ("vertical-horizontal", None, None),
    ("vertical-horizontal", "READY", 0),
    ("vertical-horizontal", "QUEUING", 0),
    ("vertical-horizontal", "RUNNING", 0),
    ("vertical-horizontal", "COMPLETED", 0),
    ("vertical-horizontal", "FAILED", 0),
    ("vertical-horizontal", "READY", 3),
    ("vertical-horizontal", "QUEUING", 3),
    ("vertical-horizontal", "RUNNING", 3),
    ("vertical-horizontal", "COMPLETED", 3),
    ("vertical-horizontal", "FAILED", 3),

    ("horizontal-vertical", None, None),
    ("horizontal-vertical", "READY", 0),
    ("horizontal-vertical", "QUEUING", 0),
    ("horizontal-vertical", "RUNNING", 0),
    ("horizontal-vertical", "COMPLETED", 0),
    ("horizontal-vertical", "FAILED", 0),
    ("horizontal-vertical", "READY", 3),
    ("horizontal-vertical", "QUEUING", 3),
    ("horizontal-vertical", "RUNNING", 3),
    ("horizontal-vertical", "COMPLETED", 3),
    ("horizontal-vertical", "FAILED", 3),
])
def test_monitor(autosubmit_exp, wrapper_type, min_trigger_status, from_step, general_data, experiment_data):
    """Test the monitor of an experiment."""

    as_exp = new_as_exp(
        autosubmit_exp=autosubmit_exp,
        general_data=general_data,
        experiment_data=experiment_data,
        wrapper_type=wrapper_type,
        structure={'min_trigger_status': min_trigger_status, 'from_step': from_step} if min_trigger_status and from_step is not None else {},
        sizes={"MIN_V": 2, "MAX_V": 2, "MIN_H": 2, "MAX_H": 2}
    )

    # TODO: For now we will just check that the pdf is generated without any autosubmit error.
    # TODO: Show in the -d option, the wrapped jobs
    as_exp.autosubmit.monitor(
        as_exp.expid,
        hide=True,  # disables the open() of the pdf after generation
        group_by=None,
        expand=[],
        expand_status=[],
        file_format="pdf",
        lst=None,
        filter_chunks=None,
        filter_status=None,
        filter_section=None,
        check_wrapper=False if wrapper_type == "simple" else True,
    )

    # check that plot folder is generated
    plot_folder = Path(BasicConfig.LOCAL_ROOT_DIR) / as_exp.expid / "plot"
    assert plot_folder.exists()
    for plot_file in plot_folder.glob("*"):
        assert plot_file.stat().st_size > 0

    if wrapper_type != "simple":
        # Check that jobs_data.db contains the preview tables
        db_path = Path(BasicConfig.LOCAL_ROOT_DIR) / as_exp.expid / "db" / "job_list.db"
        assert db_path.exists()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        assert 'preview_wrappers_jobs' in tables
        assert 'preview_wrappers_info' in tables

        # check that preview_wrapper_jobs table correctness
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT package_id, job_name FROM 'preview_wrappers_jobs';")
        rows = cursor.fetchall()
        conn.close()
        assert len(rows) > 0

        # Check that there is only entry per job_name
        job_names = [row[1] for row in rows]
        assert len(job_names) == len(set(job_names))
