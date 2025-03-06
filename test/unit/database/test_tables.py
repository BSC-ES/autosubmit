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

"""Unit tests for ``autosubmit.database.tables``."""

import pytest

from autosubmit.database.tables import get_table_with_schema, get_table_from_name, ExperimentTable


def test_get_table_with_schema_invalid_table():
    with pytest.raises(ValueError, match='Invalid source type on table schema change'):
        get_table_with_schema(schema=None, table=None)  # type: ignore


def test_get_table_with_schema():
    table = ExperimentTable
    assert table.schema is None
    table = get_table_with_schema(schema='testing', table=table)
    assert table.schema == 'testing'


def test_get_table_from_name_invalid_name():
    with pytest.raises(ValueError, match='Missing table name: None'):
        get_table_from_name(schema=None, table_name=None)  # type: ignore


def test_get_table_from_name_invalid_table_name():
    """An invalid table name will result in the same scenario as ``test_get_table_with_schema_invalid_table``."""
    with pytest.raises(ValueError, match='Invalid source type on table schema change'):
        get_table_from_name(schema=None, table_name='catch-me-if-you-can')


@pytest.mark.parametrize(
    'schema',
    [
        None,
        'paraguay'
    ]
)
def test_get_table_from_name(schema):
    table = get_table_from_name(schema=schema, table_name=ExperimentTable.name)
    assert table.name == ExperimentTable.name
    assert len(table.columns) == len(ExperimentTable.columns)  # type: ignore
    assert all([left.name == right.name for left, right in zip(table.columns, ExperimentTable.columns)])  # type: ignore
    assert table.schema == schema
