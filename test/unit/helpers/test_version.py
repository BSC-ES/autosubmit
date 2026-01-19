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

"""Tests for the Autosubmit version retrieving function."""

from autosubmit.helpers.version import get_version
from pathlib import Path


def test_version():
    assert get_version() != ''


def test_version_missing_version_file(mocker, tmp_path):
    mocker.patch('autosubmit.helpers.version.Path', return_value=tmp_path)
    wrong_path = Path(tmp_path, 'VERSION')
    if wrong_path.exists():
        wrong_path.unlink()
    assert get_version() != ''
