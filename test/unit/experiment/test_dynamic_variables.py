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

from autosubmit.log.log import AutosubmitCritical

@pytest.mark.parametrize('experiment_data', [
        {
            'TEST_SLURM': {
                'FDB_COPY_BIN': '%"CURRENT_FDB_COPY_BIN%/fdb-copy',
                'FDB_LIST_BIN': '%^CURRENT_FDB_LIST_BIN%/fdb-list',
            },
        }, {
            'TEST_SLURM': {
                'FDB_COPY_BIN': '%CURRENT_FDB_COPY_BIN%/fdb-copy',
                'FDB_LIST_BIN': '%CURRENT_FDB_LIST_BIN%/fdb-list',
            },
        }, {
            'TEST_SLURM': {
                'FDB_COPY_BIN': '%CURRENT_FDB_CPY_BIN%/fdb-copy',
                'FDB_LIST_BIN': '%CURRENT_FDB_LIST_BIN%/fdb-list',
            },
        }
], ids=["Special Dynamic Variable", "Dynamic Variable", "Multiple Dynamic Variable"])
def test_infinite_loop_dynamic_variable(autosubmit_config, general_data, experiment_data):
    as_conf = autosubmit_config('t000', {
            'PLATFORMS': experiment_data,
        })
    with pytest.raises(AutosubmitCritical) as ac:
        as_conf.experiment_data = as_conf.deep_add_missing_starter_conf(general_data, as_conf.experiment_data)
    assert "causing infinite recursion during evaluation" in ac.value.message
