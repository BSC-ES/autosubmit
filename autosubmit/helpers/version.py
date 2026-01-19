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

from importlib.metadata import version
from pathlib import Path

__all__ = [
    'get_version'
]


def get_version() -> str:
    """Get the Autosubmit version.

    Tries to find the latest version of Autosubmit.

    First, it will look for the VERSION file at the project root.

    If it fails to locate it, it will tru to find the latest
    version using ``importlib``. This may return a version from
    the installed libraries instead -- beware.
    """
    # Get the version number from the relevant file. If not, from autosubmit package
    this_path = Path(__file__).parent
    root_path = this_path.parents[1]
    version_file_path = root_path / 'VERSION'

    if version_file_path.is_file():
        with open(version_file_path) as f:
            autosubmit_version = f.read().strip()
    else:
        autosubmit_version = version("autosubmit")

    return autosubmit_version
