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

from pathlib import Path

import pytest
from networkx import DiGraph

from autosubmit.job.job_list_persistence import get_job_list_persistence

_EXPID = 't000'


@pytest.mark.docker
@pytest.mark.postgres
def test_job_list_persistence(as_db: str, autosubmit_exp):
    experiment_data: dict = {
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'echo "OK"'
            }
        }
    }
    if as_db == "postgres":
        experiment_data['STORAGE'] = {
            'TYPE': 'db'
        }
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    exp_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID)

    job_list_pers = get_job_list_persistence('job_list_persistence_postgres', exp.as_conf)

    graph = DiGraph(name="test_graph")

    job_list_pers.save(str(exp_dir / 'pkl'), __name__, [], graph)

    loaded_graph = job_list_pers.load(str(exp_dir / 'pkl'), __name__)

    assert isinstance(loaded_graph, dict)
    # TODO: improve this test with better assertion(s), e.g., what we had during development:
    #        assert loaded_graph.name == graph.name
