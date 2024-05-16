from unittest import TestCase

import os
import sys
from mock import MagicMock
from mock import patch
from autosubmit.database.db_manager import DbManager


class TestDbManager(TestCase):
    def test_create_table_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        table_fields = ['dummy1', 'dummy2', 'dummy3']
        expected_command = 'CREATE TABLE IF NOT EXISTS tests (dummy1, dummy2, dummy3)'
        # act
        command = DbManager.generate_create_table_command(table_name, table_fields)
        # assert
        self.assertEqual(expected_command, command)

    def test_insert_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        columns = ['col1, col2, col3']
        values = ['dummy1', 'dummy2', 'dummy3']
        expected_command = 'INSERT INTO tests(col1, col2, col3) VALUES ("dummy1", "dummy2", "dummy3")'
        # act
        command = DbManager.generate_insert_command(table_name, columns, values)
        # assert
        self.assertEqual(expected_command, command)

    def test_insert_many_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        num_of_values = 3
        expected_command = 'INSERT INTO tests VALUES (?,?,?)'
        # act
        command = DbManager.generate_insert_many_command(table_name, num_of_values)
        # assert
        self.assertEqual(expected_command, command)

    def test_select_command_returns_a_valid_command(self):
        # arrange
        table_name = 'tests'
        where = ['test=True', 'debug=True']
        expected_command = 'SELECT * FROM tests WHERE test=True AND debug=True'
        # act
        command = DbManager.generate_select_command(table_name, where)
        # assert
        self.assertEqual(expected_command, command)

    # def test_when_database_already_exists_then_is_not_initialized_again(self):
    #     sys.modules['os'].path.exists = MagicMock(return_value=True)
    #     connection_mock = MagicMock()
    #     cursor_mock = MagicMock()
    #     cursor_mock.side_effect = Exception('This method should not be called')
    #     connection_mock.cursor = MagicMock(return_value=cursor_mock)
    #     original_connect = sys.modules['sqlite3'].connect
    #     sys.modules['sqlite3'].connect = MagicMock(return_value=connection_mock)
    #     DbManager('dummy-path', 'dummy-name', 999)
    #     connection_mock.cursor.assert_not_called()
    #     sys.modules['sqlite3'].connect = original_connect
