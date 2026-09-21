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

"""Database helpers shared across the SQLAlchemy managers.

The most important one is column-aware batching. A single statement can only bind
a limited number of parameters (SQLite builds vary between 999 and 32766, while
PostgreSQL allows 65535). The batch size must account for the number of columns,
not only the number of rows, otherwise a bulk operation on wide rows can exceed
the limit.
"""

from collections.abc import Iterable, Iterator
from typing import TypeVar

__all__ = ["batch_size_for", "chunked", "max_params"]

T = TypeVar("T")

# Conservative floor: SQLite builds vary (999 / 32766).
SQLITE_MAX_PARAMS = 999
# PostgreSQL protocol limit.
POSTGRES_MAX_PARAMS = 65535


def max_params(dialect_name: str) -> int:
    """Return the maximum number of bound parameters for a dialect.

    :param dialect_name: The SQLAlchemy dialect name (e.g. ``sqlite``).
    :return: The maximum number of bound parameters per statement.
    """
    if dialect_name.startswith("postgres"):
        return POSTGRES_MAX_PARAMS
    return SQLITE_MAX_PARAMS


def batch_size_for(num_columns: int, dialect_name: str) -> int:
    """Return how many rows fit in a single statement for the given width.

    :param num_columns: Number of bound parameters per row.
    :param dialect_name: The SQLAlchemy dialect name.
    :return: Number of rows per batch (at least 1).
    """
    return max(1, max_params(dialect_name) // max(1, num_columns))


def chunked(items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield ``items`` in lists of at most ``size`` elements.

    :param items: The items to split.
    :param size: The maximum size of each chunk.
    :return: An iterator over the chunks.
    """
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch
