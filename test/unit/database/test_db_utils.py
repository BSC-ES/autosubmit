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

"""Unit tests for ``autosubmit.database.db_utils``."""

import pytest

from autosubmit.database.db_utils import batch_size_for, chunked, max_params


@pytest.mark.parametrize(
    "dialect, expected",
    [
        ("sqlite", 999),
        ("postgresql", 65535),
    ],
)
def test_max_params(dialect: str, expected: int) -> None:
    """Return the conservative parameter limit for each backend."""
    assert max_params(dialect) == expected


@pytest.mark.parametrize(
    "num_columns, dialect, expected",
    [
        (1, "sqlite", 999),
        (4, "sqlite", 249),
        (36, "sqlite", 27),
        (1, "postgresql", 65535),
        (36, "postgresql", 1820),
    ],
)
def test_batch_size_for_accounts_for_columns(num_columns: int, dialect: str, expected: int) -> None:
    """The batch size shrinks as rows get wider."""
    assert batch_size_for(num_columns, dialect) == expected


@pytest.mark.parametrize("size", [1, 2, 3, 10])
def test_chunked_keeps_order_and_size(size: int) -> None:
    """Chunking preserves order and never exceeds the requested size."""
    items = list(range(7))

    chunks = list(chunked(items, size))

    assert all(len(chunk) <= size for chunk in chunks)
    assert [item for chunk in chunks for item in chunk] == items


def test_chunked_empty() -> None:
    """Chunking an empty iterable yields nothing."""
    assert list(chunked([], 3)) == []
