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

import pytest

from autosubmit.job.job_package_persistence import JobPackagePersistence
from log.log import AutosubmitCritical


def test_load(mocker):
    """
    Loads package of jobs from a database
    :param: wrapper: boolean
    :return: list of jobs per package
    """
    mocker.patch('autosubmit.database.db_manager.DbManager.select_all').return_value = [
        ['random-id"', 'vertical-wrapper', 'dummy-job', '02:00']]
    mocker.patch('sqlite3.connect').return_value = mocker.MagicMock()
    job_package_persistence = JobPackagePersistence('dummy/expid')
    assert job_package_persistence.load(wrapper=True) == [['random-id"', 'vertical-wrapper', 'dummy-job', '02:00']]
    mocker.patch('autosubmit.database.db_manager.DbManager.select_all').return_value = [
        ['random-id"', 'vertical-wrapper', 'dummy-job']]
    with pytest.raises(AutosubmitCritical):
        job_package_persistence.load(wrapper=True)
