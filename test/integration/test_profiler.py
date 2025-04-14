# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

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

_EXPID = 't000'


def check_profile(run_tmpdir) -> bool:
    """
    Initialize the run, writing the jobs.yml file and creating the experiment.
    """
    # write jobs_data
    profile_path = Path(f"{run_tmpdir}/{_EXPID}/tmp/profile/")
    if profile_path.exists():
        return True
    return False


def test_run_profile(autosubmit_exp, tmp_path):
    as_exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'job': {
                'SCRIPT': 'echo "Hello World with id=Success"',
                'PLATFORM': 'local',
                'RUNNING': 'once'
            }
        },
        'PROJECT': {
            'TYPE': 'local',
            'PROJECT_DESTINATION': 'local_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(tmp_path)
        }
    })
    # Run the experiment
    # TODO: In the future, we should be able to remove the MISC files, and
    #       instead either carry the state in the code via objects/decorators,
    #       etc., or use the DB to know what was the last command used -- if
    #       that is needed.
    as_exp.autosubmit._check_ownership_and_set_last_command(
        as_exp.as_conf,
        as_exp.expid,
        'run')
    as_exp.autosubmit.run_experiment(expid=as_exp.expid, profile=True)
    assert check_profile(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR)
