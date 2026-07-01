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

from autosubmit.helpers.enums import ChunkUnit


@pytest.mark.parametrize(
    'member,value',
    [
        (ChunkUnit.YEAR, 'year'),
        (ChunkUnit.MONTH, 'month'),
        (ChunkUnit.DAY, 'day'),
        (ChunkUnit.HOUR, 'hour'),
    ]
)
def test_chunk_unit_values(member, value):
    """ChunkUnit members expose the expected string values."""
    assert member.value == value


@pytest.mark.parametrize(
    'value,member',
    [
        ('year', ChunkUnit.YEAR),
        ('month', ChunkUnit.MONTH),
        ('day', ChunkUnit.DAY),
        ('hour', ChunkUnit.HOUR),
    ]
)
def test_chunk_unit_parses_from_string(value, member):
    """ChunkUnit(value) resolves to the matching member."""
    assert ChunkUnit(value) is member


@pytest.mark.parametrize(
    'member,value',
    [
        (ChunkUnit.YEAR, 'year'),
        (ChunkUnit.MONTH, 'month'),
        (ChunkUnit.DAY, 'day'),
        (ChunkUnit.HOUR, 'hour'),
    ]
)
def test_chunk_unit_str_equality(member, value):
    """ChunkUnit members compare equal to their string values (str subclass)."""
    assert member == value


def test_chunk_unit_membership():
    """ChunkUnit contains exactly the four expected chunk units."""
    assert {u.value for u in ChunkUnit} == {'year', 'month', 'day', 'hour'}


def test_chunk_unit_invalid_value_raises():
    """Constructing ChunkUnit from an unknown value raises ValueError."""
    with pytest.raises(ValueError):
        ChunkUnit('minute')