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

import pytest
import os
import tempfile
import subprocess
from pathlib import Path
from log.log import AutosubmitError, AutosubmitCritical, Log

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

def is_xz_file(filepath: str):
    with open(filepath, 'rb') as f:
        magic = f.read(6)
    return magic == bytes.fromhex("FD 37 7A 58 5A 00")

def test_logfile_compression(tmpdir):
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(b"Some content to compress.\n")
        tmp_file_path = Path(tmp_file.name)

    try:
        Log.compress_logfile(str(tmp_file_path))

        compressed_path = tmp_file_path.with_suffix(tmp_file_path.suffix + ".xz")
        assert is_xz_file(compressed_path)

    finally:
        compressed_path = tmp_file_path.with_suffix(tmp_file_path.suffix + ".xz")
        if compressed_path.exists():
            os.remove(compressed_path)
