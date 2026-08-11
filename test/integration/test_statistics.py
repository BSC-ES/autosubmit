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

from collections.abc import Callable
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from autosubmit.workflow.manage import statistics


def test_stats_get_params(autosubmit_exp: Callable, mocker: MockerFixture) -> None:
    """
    Test that the statistics generator receives the correct parameters from the jobs, including platform information.
    """
    FAKE_EXPID = "st01"
    autosubmit_exp(
        FAKE_EXPID,
        experiment_data={
            "JOBS": {
                "SIM": {
                    "SCRIPT": "echo hello",
                    "PLATFORM": "NODE_PLATFORM",
                    "RUNNING": "once",
                }
            },
            "PLATFORMS": {
                "NODE_PLATFORM": {
                    "TYPE": "SLURM",
                    "NODES": 4,
                    "PROCESSORS_PER_NODE": 16,
                    "TASKS": 2,
                }
            },
        },
    )

    mock_monitor = MagicMock()
    mock_gen_stats = MagicMock()
    mock_monitor.generate_output_stats = mock_gen_stats
    mocker.patch("autosubmit.monitor.monitor.Monitor", return_value=mock_monitor)

    statistics(
        expid=FAKE_EXPID,
        filter_type="SIM",
        filter_period=None,
        file_format="png",
        section_summary=False,
        jobs_summary=False,
        hide=True,
    )

    # Assert that the jobs have the platform information correctly passed to the statistics generator
    called_job = mock_gen_stats.call_args[0][1][0]
    assert str(called_job.nodes) == "4"
    assert str(called_job.processors_per_node) == "16"
    assert str(called_job.tasks) == "2"
    assert str(called_job.processors) == "1"
    assert called_job.platform.name == "NODE_PLATFORM"
