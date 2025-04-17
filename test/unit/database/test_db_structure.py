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

"""Unit tests for ``autosubmit.database.db_structure``.

We cover mainly error and validation scenarios here. See
the ``test/integration/test_db_structure.py`` for more tests.
"""
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.database.db_structure import (
    create_connection, create_table, get_structure, get_structure_sqlalchemy, save_structure,
    save_structure_sqlalchemy
)

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def test_get_structure_exception(mocker):
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')

    get_structure('a000', None)  # type: ignore

    assert mocked_log.printlog.called
    assert mocked_log.debug.called


def test_get_structure_invalid_path(mocker):
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')

    get_structure('a000', 'tree-hill')  # type: ignore

    assert mocked_log.printlog.called
    assert mocked_log.debug.called


def test_get_structure_exception_getting_table(mocker, tmpdir: 'LocalPath'):
    """When ``get_structure`` calls ``_get_exp_structure``, but this function
    finds an exception, instead of raising it, it returns a dict (for some reason).
    This test verifies that that dictionary is returned what later results in an
    empty dictionary being returned as the table structure.

    TODO: The function under-testing does not look very well-designed, can probably
          be reviewed and simplified (both for users, devs, and tests).
    """
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')
    mocked_create_connection = mocker.patch(
        'autosubmit.database.db_structure.create_connection',
        side_effect=(mocker.MagicMock(), Exception)
    )

    structure = get_structure('a000', str(tmpdir))  # type: ignore

    assert type(structure) is dict
    assert not structure
    assert mocked_create_connection.called
    assert mocked_create_connection.call_count == 2
    assert not mocked_log.printlog.called


def test_get_structure_sqlalchemy_exception(mocker):
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')
    mocked_create_db_manager = mocker.patch('autosubmit.database.db_structure.create_db_manager')

    mocked_create_db_manager.side_effect = Exception
    get_structure_sqlalchemy('', '')

    assert mocked_create_db_manager.called
    assert mocked_log.printlog.called
    assert mocked_log.debug.called


def test_create_connection_exception_returns_none(mocker):
    mocked_sqlite3 = mocker.patch('autosubmit.database.db_structure.sqlite3')
    mocked_sqlite3.connect.side_effect = sqlite3.DatabaseError

    conn = create_connection('')

    assert mocked_sqlite3.connect.called
    assert conn is None


def test_create_table_exception_logs(mocker):
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')

    mocked_conn = mocker.MagicMock()
    mocked_conn.cursor.side_effect = sqlite3.DatabaseError

    create_table(mocked_conn, '')

    assert mocked_conn.cursor.called
    assert mocked_log.printlog.called


def test_save_structure_exception_raises():
    with pytest.raises(Exception) as cm:
        save_structure(None, '', '')

    assert 'Structures folder not found' in str(cm.value)


def test_save_structure__create_edge_exception(tmpdir: 'LocalPath', mocker):
    # TODO: This amount of mocking never means something good about the design of the code.
    #       We can probably do better.
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')
    mocked_create_connection = mocker.patch('autosubmit.database.db_structure.create_connection')
    mocked_cursor = mocker.MagicMock()
    mocked_create_connection.return_value = mocked_cursor
    mocked_cursor.cursor.side_effect = sqlite3.DatabaseError

    save_structure(mocker.MagicMock(), 'a000', str(tmpdir))

    assert mocked_cursor.cursor.called
    assert mocked_log.debug.called
    assert mocked_log.warning.called


def test_save_structure__delete_table_content_exception(tmpdir: 'LocalPath', mocker):
    # TODO: This amount of mocking never means something good about the design of the code.
    #       We can probably do better.
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')
    mocked_create_connection = mocker.patch('autosubmit.database.db_structure.create_connection')
    mocked_cursor = mocker.MagicMock()
    mocked_create_connection.return_value = mocked_cursor
    mocked_cursor.cursor.side_effect = sqlite3.DatabaseError

    expid = 'a000'
    # Pre-create the DB, so that it's deleted first, triggering the call to
    # ``_delete_table_content``.
    Path(tmpdir / f'structure_{expid}.db').touch()
    save_structure(mocker.MagicMock(), expid, str(tmpdir))

    assert mocked_cursor.cursor.called
    assert mocked_log.debug.called
    assert mocked_log.warning.called


def test_save_structure_sqlalchemy_create_db_manager_exception(mocker):
    mocker.patch('autosubmit.database.db_structure.create_db_manager', side_effect=Exception)

    with pytest.raises(Exception):
        save_structure_sqlalchemy(None, 'a000', '')  # type: ignore

