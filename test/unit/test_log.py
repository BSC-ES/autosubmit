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

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.log.utils import compress_xz, find_uncompressed_files, is_xz_file

if TYPE_CHECKING:
    from pytest_mock import MockFixture


"""Tests for the log module."""


def test_autosubmit_error():
    ae = AutosubmitError()
    assert 'Unhandled Error' == ae.message
    assert 6000 == ae.code
    assert None is ae.trace
    assert 'Unhandled Error' == ae.error_message
    assert ' ' == str(ae)


def test_autosubmit_error_error_message():
    ae = AutosubmitError(trace='ERROR!')
    assert 'ERROR! Unhandled Error' == ae.error_message


def test_autosubmit_critical():
    ac = AutosubmitCritical()
    assert 'Unhandled Error' == ac.message
    assert 7000 == ac.code
    assert None is ac.trace
    assert ' ' == str(ac)

def test_log_not_format():
    """
    Smoke test if the log messages are sent correctly
    when having a formattable message that it is not
    intended to be formatted
    """

    def _send_messages(msg: str):
        Log.debug(msg)
        Log.info(msg)
        Log.result(msg)
        Log.warning(msg)
        Log.error(msg)
        Log.critical(msg)
        Log.status(msg)
        Log.status_failed(msg)

    # Standard messages
    msg = "Test"
    _send_messages(msg)

    # Format messages
    msg = "Test {foo, bar}"
    _send_messages(msg)


def test_set_file_retrial(mocker: "MockFixture"):
    max_retries = 3

    # Make os.path.split raise an exception
    mocker.patch("os.path.split", side_effect=Exception("Mocked exception"))

    sleep = mocker.patch("autosubmit.log.log.sleep")

    with pytest.raises(AutosubmitCritical):
        Log.set_file("imaginary.log", max_retries=max_retries, timeout=1)

    assert sleep.call_count == max_retries - 1


def test_compress_xz(tmp_path: Path):
    test_content = "Test content foo bar"

    test_path = tmp_path.joinpath("test-dir")
    test_path.mkdir()

    input_file = str(test_path.joinpath("test-input.txt"))
    with open(input_file, "w") as f:
        f.write(test_content)

    output_file = str(test_path.joinpath("test-compressed.xz"))
    compress_xz(input_file, output_file, preset=9, extreme=True)

    assert Path(output_file).exists()
    assert is_xz_file(output_file)
    assert len(find_uncompressed_files(str(test_path))) == 1

    # Verify same result with xz
    output_file2 = input_file + ".xz"
    subprocess.run(["xz", "-9", "-e", "-k", input_file], check=True)

    assert Path(output_file2).exists()
    assert is_xz_file(output_file2)

    # Verify both files output_file and output_file2 are equal
    assert subprocess.check_output(
        ["xz", "-d", "-c", output_file], text=True
    ) == subprocess.check_output(["xz", "-d", "-c", output_file2], text=True)
    assert subprocess.run(["cmp", output_file, output_file2], check=True)

    # Cover unexistent path
    with pytest.raises(FileNotFoundError):
        find_uncompressed_files(str(tmp_path.joinpath("unexistent_path")))
