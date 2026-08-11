# Copyright 2015-2026 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""File to create a test for the profiling."""

from pathlib import Path

import pytest

from autosubmit.profiler.profiler import Profiler
from autosubmit.workflow.manage import run


def check_profile(expid: str, run_tmpdir: str) -> bool:
    """Initialise the run, writing the jobs.yml file and creating the experiment."""
    # write jobs_data
    profile_path = Path(f"{run_tmpdir}/{expid}/tmp/profile/")
    return bool(profile_path.exists())


@pytest.mark.parametrize(
    "trace_enabled,max_checkpoints",
    [
        (False, 0),
        (True, 1),
        (False, 2),
        (True, 0),
    ],
)
def test_run_profile(trace_enabled, max_checkpoints, autosubmit_exp, tmp_path):
    as_exp = autosubmit_exp(
        experiment_data={
            "JOBS": {
                "job": {
                    "SCRIPT": 'echo "Hello World with id=Success"',
                    "PLATFORM": "local",
                    "RUNNING": "once",
                }
            },
            "PROJECT": {"TYPE": "local", "PROJECT_DESTINATION": "local_project"},
            "LOCAL": {"PROJECT_PATH": str(tmp_path)},
        }
    )
    # Run the experiment
    # TODO: In the future, we should be able to remove the MISC files, and
    #       instead either carry the state in the code via objects/decorators,
    #       etc., or use the DB to know what was the last command used -- if
    #       that is needed.
    as_exp.as_conf.set_last_as_command("run")
    profiler = Profiler(
        as_exp.expid, trace_enabled=trace_enabled, max_checkpoints=max_checkpoints
    )
    profiler.start()
    try:
        run(expid=as_exp.expid)
    finally:
        profiler.stop()
    assert check_profile(as_exp.expid, as_exp.as_conf.basic_config.LOCAL_ROOT_DIR)
