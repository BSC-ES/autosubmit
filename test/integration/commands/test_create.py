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
from autosubmit.log.log import AutosubmitCritical


@pytest.mark.parametrize("noplot", [True, False])
def test_create_noplot_calls_generate_output(as_exp, mocker, noplot):
    """Test that create calls generate_output when noplot is False and does not call it when noplot is True."""
    db_manager = SqlAlchemyExperimentHistoryDbManager(
        as_exp.expid, BasicConfig.JOBDATA_DIR, f"job_data_{as_exp.expid}.db"
    )

    db_manager.initialize()

    mock_generate_output = mocker.patch(
        "autosubmit.monitor.monitor.Monitor.generate_output"
    )

    as_exp.autosubmit.create(
        as_exp.expid,
        noplot=noplot,
        hide=True,
    )

    if noplot:
        mock_generate_output.assert_not_called()
    else:
        mock_generate_output.assert_called_once()


def test_create_cw_calls_generate_scripts_andor_wrappers_without_plt(
    autosubmit_exp, general_data, mocker
):
    """create -cw must call generate_scripts_andor_wrappers even without -plt.

    Before the fix the wrapper generation was nested inside ``if not noplot:``
    so ``create -cw`` without ``-plt`` silently skipped preview-table
    population.
    """
    mock_gen = mocker.patch(
        "autosubmit.autosubmit.Autosubmit.generate_scripts_andor_wrappers"
    )

    exp_data = {
        "EXPERIMENT": {
            "DATELIST": "20200101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": 1,
            "NUMCHUNKS": 1,
            "CALENDAR": "standard",
        },
        "JOBS": {
            "SLURMJOB": {
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "WALLCLOCK": "02:00",
                "PLATFORM": "TEST_SLURM",
            },
        },
        "WRAPPERS": {
            "SIMPLE_WRAPPER": {
                "TYPE": "vertical",
                "JOBS_IN_WRAPPER": "SLURMJOB",
                "MAX_WRAPPED_V": 2,
                "MIN_WRAPPED_V": 2,
            },
        },
    }
    config_data = general_data | exp_data

    exp = autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)

    exp.autosubmit.create(
        exp.expid,
        noplot=True,
        hide=True,
        check_wrappers=True,
    )

    mock_gen.assert_called_once()


@pytest.mark.parametrize(
    "autosubmit_totaljobs, platforms_totaljobs, raise_error",
    [
        (None, None, False),
        (None, 10, False),
        (None, -10, False),
        (None, 0, True),
        (10, None, False),
        (10, 10, False),
        (10, -10, False),
        (10, 0, True),
        (-10, None, False),
        (-10, 10, False),
        (-10, -10, False),
        (-10, 0, True),
        (0, None, True),
        (0, 10, False),
        (0, -10, False),
        (0, 0, True),
    ],
    ids=[
        "CONFIG.TOTALJOBS=None, PLATFORMS.TOTALJOBS=None logs warning",
        "CONFIG.TOTALJOBS=None, PLATFORMS.TOTALJOBS=10 logs warning",
        "CONFIG.TOTALJOBS=None, PLATFORMS.TOTALJOBS=-10 logs warning",
        "CONFIG.TOTALJOBS=None, PLATFORMS.TOTALJOBS=0 raises error",
        "CONFIG.TOTALJOBS=10, PLATFORMS.TOTALJOBS=None correct",
        "CONFIG.TOTALJOBS=10, PLATFORMS.TOTALJOBS=10 correct",
        "CONFIG.TOTALJOBS=10, PLATFORMS.TOTALJOBS=-10 correct",
        "CONFIG.TOTALJOBS=10, PLATFORMS.TOTALJOBS=0 raises error",
        "CONFIG.TOTALJOBS=-10, PLATFORMS.TOTALJOBS=None logs warning",
        "CONFIG.TOTALJOBS=-10, PLATFORMS.TOTALJOBS=10 logs warning",
        "CONFIG.TOTALJOBS=-10, PLATFORMS.TOTALJOBS=-10 logs warning",
        "CONFIG.TOTALJOBS=-10, PLATFORMS.TOTALJOBS=0 raises error",
        "CONFIG.TOTALJOBS=0, PLATFORMS.TOTALJOBS=None raises error",
        "CONFIG.TOTALJOBS=0, PLATFORMS.TOTALJOBS=10 logs warning",
        "CONFIG.TOTALJOBS=0, PLATFORMS.TOTALJOBS=-10 logs warning",
        "CONFIG.TOTALJOBS=0, PLATFORMS.TOTALJOBS=0 raises error",
    ],
)
def test_create_cw_totaljobs_cases(
    autosubmit_exp,
    general_data,
    mocker,
    autosubmit_totaljobs,
    platforms_totaljobs,
    raise_error,
):
    """Test create -cw command with different combinations of CONFIG.TOTALJOBS and PLATFORMS.TOTALJOBS values."""
    exp_data = {
        "EXPERIMENT": {
            "DATELIST": "20200101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": 1,
            "NUMCHUNKS": 1,
            "CALENDAR": "standard",
        },
        "CONFIG": {
            "SAFETYSLEEPTIME": 0,
            "MAXWAITINGJOBS": 20,
            "TOTALJOBS": autosubmit_totaljobs,
        },
        "JOBS": {
            "SLURMJOB": {
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "WALLCLOCK": "02:00",
                "PLATFORM": "TEST_SLURM",
            },
            "LOCALJOB": {
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "WALLCLOCK": "02:00",
            }
        },
        "WRAPPERS": {
            "SIMPLE_WRAPPER": {
                "TYPE": "vertical",
                "JOBS_IN_WRAPPER": "SLURMJOB",
                "MAX_WRAPPED_V": 2,
                "MIN_WRAPPED_V": 2,
            },
        },
    }
    config_data = general_data | exp_data

    if platforms_totaljobs is not None:
        config_data["PLATFORMS"] = dict(config_data.get("PLATFORMS", {}))
        config_data["PLATFORMS"]["TEST_SLURM"] = dict(
            config_data["PLATFORMS"].get("TEST_SLURM", {})
        )
        config_data["PLATFORMS"]["TEST_SLURM"]["TOTALJOBS"] = platforms_totaljobs

    exp = autosubmit_exp(experiment_data=config_data, include_jobs=False, create=False)

    if raise_error:
        with pytest.raises(AutosubmitCritical):
            exp.autosubmit.create(
                exp.expid,
                noplot=True,
                hide=True,
                check_wrappers=True,
            )
    else:
        exp.autosubmit.create(
            exp.expid,
            noplot=True,
            hide=True,
            check_wrappers=True,
        )
