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

"""Utilities for Pytest integration."""
import os
from getpass import getuser
from pathlib import Path
from tempfile import gettempdir
from typing import Iterator, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from pytest import FixtureRequest

__all__ = [
    'get_next_pytest_base_temp',
    'markers_contain'
]


def markers_contain(request: "FixtureRequest", txt: str) -> bool:
    """Check if a marker is used in the test.

    Returns ``True`` if the caller test is decorated with a
    marker that matches the given text. Otherwise, ``False``.
    """
    markers = request.node.iter_markers()
    return any(marker.name == txt for marker in markers)


def get_next_pytest_base_temp() -> Path:
    """Get the next basetemp directory that pytest will use.

    Pytest has a class ``TempPathFactory`` that contains the function
    ``from_config`` that is a builder for the object. However, that
    function always creates the next basetemp.

    If you call that from a hook like ``pytest_configure``, that will
    create a new session base temp directory. However, further ahead
    in the lifecycle of Pytest, it will call that function again,
    discarding your previously created session base temp directory.

    This function emulates what pytest does. It is a bit risky, and
    in case this function fails (e.g. due to changes in Pytest code),
    you can fix this by simply creating temporary directories with
    ``tempfile``. The only downside of that is that you will have
    SSH, Git, etc., directories created per session probably under
    your ``/tmp`` folder, instead of under ``/tmp/pytest-of-$USER/pytest-42/``,
    and in case you need to access the SSH files used for the session
    ID 42, you may have a hard time tracking that file.

    This function is only for convenience of the developers, to locate
    those files more quickly.

    Refs: https://github.com/pytest-dev/pytest/blob/33d5a09a76c49e8fd6de440814cdc925c2fa1062/src/_pytest/pathlib.py#L225
    """
    user = getuser() or "unknown"
    pytest_debug_temproot = os.environ.get("PYTEST_DEBUG_TEMPROOT")
    temproot = Path(pytest_debug_temproot or gettempdir()).resolve()
    user_pytest_folder = temproot.joinpath(f"pytest-of-{user}")

    if not user_pytest_folder.exists():
        user_pytest_folder.mkdir()

    # The functions below were copied from Pytest (see commit ID above).

    def find_prefixed(root: Path, the_prefix: str) -> Iterator[os.DirEntry[str]]:
        """Find all elements in root that begin with the prefix, case-insensitive."""
        l_prefix = the_prefix.lower()
        for x in os.scandir(root):
            if x.name.lower().startswith(l_prefix):
                yield x

    def extract_suffixes(iter: Iterable[os.DirEntry[str]], the_prefix: str) -> Iterator[str]:
        """Return the parts of the paths following the prefix.

        :param iter: Iterator over path names.
        :param the_prefix: Expected prefix of the path names.
        """
        p_len = len(the_prefix)
        for entry in iter:
            yield entry.name[p_len:]

    def find_suffixes(root: Path, the_prefix: str) -> Iterator[str]:
        """Combine find_prefixes and extract_suffixes."""
        return extract_suffixes(find_prefixed(root, the_prefix), the_prefix)

    def parse_num(maybe_num: str) -> int:
        """Parse number path suffixes, returns -1 on error."""
        try:
            return int(maybe_num)
        except ValueError:
            return -1

    prefix = "pytest-"
    max_existing = max(map(parse_num, find_suffixes(user_pytest_folder, prefix)), default=-1)
    new_number = max_existing + 1

    return Path(user_pytest_folder, f'{prefix}{new_number}')
