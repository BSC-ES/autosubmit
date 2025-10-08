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

"""Unit tests for ``autosubmit.database.db_manager``."""
from contextlib import nullcontext as does_not_raise

import pytest

from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentTable


def test_insert_rejects_empty_data():
    db_manager = DbManager('sqlite:///:memory:', schema='abc')
    with does_not_raise():
        db_manager.insert(ExperimentTable.name, {})


def test_insert_many_rejects_empty_data():
    db_manager = DbManager('sqlite:///:memory:', schema='abc')
    assert 0 == db_manager.insert_many(ExperimentTable.name, [])


def test_delete_where_raises_empty_data():
    db_manager = DbManager('sqlite:///:memory:', schema='abc')
    with pytest.raises(ValueError):
        db_manager.delete_where(ExperimentTable.name, {})
