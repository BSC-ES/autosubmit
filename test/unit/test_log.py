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

from typing import TYPE_CHECKING

import pytest

from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log

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


def test_set_file_retrial(mocker: 'MockFixture'):
    max_retries = 3

    # Make os.path.split raise an exception
    mocker.patch("os.path.split", side_effect=Exception("Mocked exception"))

    sleep = mocker.patch("autosubmit.log.log.sleep")

    with pytest.raises(AutosubmitCritical):
        Log.set_file("imaginary.log", max_retries=max_retries, timeout=1)

    assert sleep.call_count == max_retries - 1
