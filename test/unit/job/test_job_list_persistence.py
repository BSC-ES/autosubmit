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

"""Unit tests for ``job_list_persistence.py``."""

from typing import Union, TYPE_CHECKING

import pytest

from autosubmit.job.job_list_persistence import get_job_list_persistence, JobListPersistenceDb, JobListPersistencePkl
from autosubmit.log.log import AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.job.job_list_persistence import JobListPersistence

_EXPID = 't000'


@pytest.mark.parametrize(
    'storage_type,expected',
    [
        ('ERROR', AutosubmitCritical),
        ('pkl', JobListPersistencePkl),
        ('db', JobListPersistenceDb)
    ]
)
def test_get_job_list_persistence(
        storage_type: str, expected: Union[AutosubmitCritical, 'JobListPersistence'],
        autosubmit_config
):
    """Test that we get the correct job list persistence for a storage type."""
    as_conf = autosubmit_config(_EXPID, {
        'STORAGE': {
            'TYPE': storage_type
        }
    })

    if expected is AutosubmitCritical:
        with pytest.raises(expected):  # type: ignore
            get_job_list_persistence(_EXPID, as_conf)
    else:
        job_list_persistence = get_job_list_persistence(_EXPID, as_conf)
        assert type(job_list_persistence) is expected
