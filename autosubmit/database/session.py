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

from sqlalchemy import Engine, NullPool, create_engine as sqlalchemy_create_engine


def create_engine(connection_url: str) -> Engine:
    """Create SQLAlchemy Core engine.

    :param connection_url: A SQLAlchemy connection URL.
    """
    if not connection_url:
        raise ValueError(f'Invalid SQLAlchemy connection URL: {connection_url}')

    is_sqlite = connection_url.startswith("sqlite")
    pool_class = NullPool if is_sqlite else None
    return sqlalchemy_create_engine(connection_url, poolclass=pool_class)


__all__ = ["create_engine"]
