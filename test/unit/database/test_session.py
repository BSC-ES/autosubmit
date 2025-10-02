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

from typing import Union

import pytest

from autosubmit.database.session import create_engine


@pytest.mark.parametrize(
    'url,expected',
    [
        ('postgresql://user:pass@host:1984/db', 'postgresql'),
        ('sqlite://', 'sqlite'),
        (None, ValueError)
    ]
)
def test_create_engine(url: str, expected: Union[str, Exception]):
    if type(expected) is not str:
        with pytest.raises(expected):  # type: ignore
            create_engine(connection_url=url)
    else:
        engine = create_engine(connection_url=url)
        assert engine.name == expected
