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
