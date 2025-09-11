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

"""Integration tests for ``autosubmit.job.job_list_persistence``."""

import pytest
from networkx import DiGraph
from pathlib import Path

from autosubmit.autosubmit import Autosubmit


_EXPID = 't000'


#TODO: change to use the new db
@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
@pytest.mark.skip() # TODO: change to use the new db
def test_job_list_persistence(db_engine: str, autosubmit_exp, request):
    # Dynamically load the fixture for that DB,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f"as_db_{db_engine}")
    experiment_data: dict = {
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            }
        }
    }
    if db_engine == "postgres":
        experiment_data['STORAGE'] = {
            'TYPE': 'db'
        }
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    exp_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID)

    job_list_pers = Autosubmit._get_job_list_persistence('job_list_persistence_postgres', exp.as_conf)

    graph = DiGraph(name="test_graph")

    job_list_pers.save(str(exp_dir / 'db'), __name__, [], graph)

    loaded_graph = job_list_pers.load(str(exp_dir / 'db'), __name__)

    assert isinstance(loaded_graph, dict)
    # TODO: improve this test with better assertion(s), e.g., what we had during development:
    #        assert loaded_graph.name == graph.name
