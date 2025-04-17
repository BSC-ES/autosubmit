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

from typing import Optional

import pytest

from autosubmit.database.session import create_engine


@pytest.mark.parametrize(
    'backend,url,expected_name',
    [
        ('postgres', 'postgresql://user:pass@host:1984/db', 'postgresql'),
        ('sqlite', 'sqlite://', 'sqlite'),
        (None, 'sqlite://', 'sqlite')
    ]
)
def test_create_engine(backend: Optional[str], url: str, expected_name: str, mocker):
    mocked_basic_config = mocker.patch('autosubmit.database.session.BasicConfig')
    mocked_basic_config.DATABASE_BACKEND = backend
    mocked_basic_config.DATABASE_CONN_URL = url

    engine = create_engine()

    assert engine.name == expected_name
