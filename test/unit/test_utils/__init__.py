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

"""Unit test utilities."""

import re
from pathlib import Path
from typing import Optional, Union

XZ_MAGIC = "FD 37 7A 58 5A 00"
GZIP_MAGIC = "1F 8B"


def is_xz_file(filepath: Union[Path, str]):
    with open(filepath, "rb") as f:
        magic = f.read(6)
    return magic == bytes.fromhex(XZ_MAGIC)


def is_gzip_file(filepath: Union[Path, str]):
    with open(filepath, "rb") as f:
        magic = f.read(2)
    return magic == bytes.fromhex(GZIP_MAGIC)


def find_uncompressed_files(
    file_path: Union[Path, str], pattern: Optional[str] = None
) -> list[str]:
    """
    Return all files that are not compressed with xz in a directory and
    match the filename with the given regex pattern.
    """

    if not Path(file_path).exists():
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")

    # Get all files in the directory sorted by modification time
    all_files = sorted(
        [f for f in Path(file_path).glob("*") if f.is_file()],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    result = []
    for filename in all_files:
        # Match the regex pattern if provided
        if pattern and not re.match(pattern, str(filename.name)):
            continue

        # Check if the file is not compressed
        if not is_xz_file(str(filename)) and not is_gzip_file(str(filename)):
            result.append(str(filename))

    return result
