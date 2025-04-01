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

from autosubmit.database.db_manager import DbManager


@pytest.mark.skip('It uses real data.')
class TestDbManager:
    def setup_method(self):
        self.db_manager = DbManager('', 'test-db', 1)

    def teardown_method(self):
        self.db_manager.drop()

    def test_db_manager_has_made_correct_initialization(self):
        name = self.db_manager.select_first_where('db_options', ['option_name="name"'])[1]
        version = self.db_manager.select_first_where('db_options', ['option_name="version"'])[1]
        assert self.db_manager.db_name == name
        assert self.db_manager.db_version == int(version)

    def test_after_create_table_command_then_it_returns_0_rows(self):
        table_name = 'test'
        self.db_manager.create_table(table_name, ['field1', 'field2'])
        count = self.db_manager.count(table_name)
        assert 0 == count

    def test_after_3_inserts_into_a_table_then_it_has_3_rows(self):
        table_name = 'test'
        columns = ['field1', 'field2']
        self.db_manager.create_table(table_name, columns)
        for i in range(3):
            self.db_manager.insert(table_name, columns, ['dummy', 'dummy'])
        count = self.db_manager.count(table_name)
        assert 3 == count
