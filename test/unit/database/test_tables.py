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

from autosubmit.database.tables import ExperimentTable, TableRegistry


def test_get_table():
    table = ExperimentTable
    table_registry = TableRegistry("testing")
    assert table.schema is None
    table = table_registry.get(table.name)
    assert table.schema == 'testing'


def test_get_table_from_name_invalid_table_name() -> None:
    """Raise ``KeyError`` for an unknown table name."""
    table_registry = TableRegistry("testing")

    with pytest.raises(KeyError) as exc_info:
        table_registry.get(table_name="catch-me-if-you-can")

    assert exc_info.value.args[0] == "No table definition found for 'catch-me-if-you-can'."


@pytest.mark.parametrize(
    'schema',
    [
        None,
        'paraguay'
    ]
)
def test_get_table_from_name(schema):
    table_registry = TableRegistry(schema)
    table = table_registry.get(table_name=ExperimentTable.name)
    assert table.name == ExperimentTable.name
    assert len(table.columns) == len(ExperimentTable.columns)  # type: ignore
    assert all([left.name == right.name for left, right in zip(table.columns, ExperimentTable.columns)])  # type: ignore
    assert table.schema == schema
