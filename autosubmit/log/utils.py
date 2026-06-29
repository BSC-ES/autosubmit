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

import gzip
import lzma
from pathlib import Path
from typing import Optional, Union


def compress_xz(
    input_path: Union[Path, str],
    output_path: Optional[Union[Path, str]] = None,
    preset: int = 6,
    extreme: bool = False,
    keep_input: bool = True,
):
    """
    Compress a file using XZ compression.

    It uses the Python lzma module, not the system executable.

    :param input_path: Path to the input file.
    :param output_path: Path to the output compressed file. If None, defaults to <input_path>.xz.
    :param preset: Compression preset (1-9). Defaults to 6.
    :param extreme: Whether to use extreme compression settings. Defaults to False.
    :param keep_input: Whether to keep the original input file. Defaults to True.
    """
    if output_path is None:
        output_path = f"{input_path}.xz"

    final_preset = (preset | lzma.PRESET_EXTREME) if extreme else preset

    with open(input_path, "rb") as input_file:
        with lzma.open(output_path, "wb", preset=final_preset) as output_file:
            output_file.writelines(input_file)

    if not keep_input and input_path != output_path:
        Path(input_path).unlink(missing_ok=True)

    return output_path


def compress_gzip(
    input_path: str,
    output_path: Optional[str] = None,
    compression_level: int = 9,
    keep_input: bool = True,
):
    """
    Compress a file using Gzip compression.

    :param input_path: Path to the input file.
    :param output_path: Path to the output compressed file. If None, defaults to <input_path>.gz.
    :param compression_level: Compression level (0-9). Defaults to 9.
    :param keep_input: Whether to keep the original input file. Defaults to True.
    """

    if output_path is None:
        output_path = f"{input_path}.gz"

    with open(input_path, "rb") as input_file:
        with gzip.open(
            output_path, "wb", compresslevel=compression_level
        ) as output_file:
            output_file.writelines(input_file)

    if not keep_input and input_path != output_path:
        Path(input_path).unlink(missing_ok=True)

    return output_path
