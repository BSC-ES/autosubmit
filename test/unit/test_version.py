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

from autosubmit.autosubmit import Autosubmit


def test_autosubmit_version():
    exit_code, out = subprocess.getstatusoutput('autosubmit -v')
    assert exit_code == 0
    assert out.strip().endswith(Autosubmit.autosubmit_version)


def test_autosubmit_version_broken():
    exit_code, _ = subprocess.getstatusoutput('autosubmit -abcdefg')
    assert exit_code == 1
