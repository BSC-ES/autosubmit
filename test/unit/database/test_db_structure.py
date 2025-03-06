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
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.database.db_structure import (
    get_structure, save_structure
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
    """
    mocked_log = mocker.patch('autosubmit.database.db_structure.Log')
    
    assert get_structure('a000', None) is None  # type: ignore

    assert mocked_log.printlog.called
    assert mocked_log.debug.called


def test_save_structure_exception_raises(tmp_path):
    with pytest.raises(Exception) as cm:
        save_structure(None, '', Path(tmp_path) / 'does not exist yet')  # type: ignore

    assert 'Structures folder not found' in str(cm.value)
