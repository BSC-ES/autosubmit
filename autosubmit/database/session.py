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

from pathlib import Path
from typing import Optional

from sqlalchemy import Engine, NullPool, create_engine as sqlalchemy_create_engine

from autosubmitconfigparser.config.basicconfig import BasicConfig

_SQLITE_IN_MEMORY_URL = "sqlite://"


def _create_sqlite_engine(path: Optional[str] = None) -> Engine:
    # file-based, or in-memory database?
    sqlite_url = f"sqlite:///{Path(path).resolve()}" if path else _SQLITE_IN_MEMORY_URL
    return sqlalchemy_create_engine(sqlite_url, poolclass=NullPool)


def create_engine() -> Engine:
    """Create SQLAlchemy Core engine."""
    if BasicConfig.DATABASE_BACKEND == "postgres":
        return sqlalchemy_create_engine(BasicConfig.DATABASE_CONN_URL)
    else:
        # TODO: Note that this won't work with Autosubmit as Autosubmit requires
        #       a database available to different processes; where one process is
        #       going to first issue the configure and install commands, and then
        #       other process(es) will run experiments. We would need a daemon of
        #       sorts here to have the in-memory sqlite DB -- hence, we will have
        #       to change how we create the SQLite engine when we move everything
        #       to SQLAlchemy.
        return _create_sqlite_engine()


__all__ = ["create_engine"]
