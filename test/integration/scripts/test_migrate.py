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

"""Tests for migrating AS experiments."""

# noinspection PyProtectedMember
import pytest

from autosubmit.scripts.autosubmit import _autosubmit


def test_migrate_offer(autosubmit_exp):
    """Temporary test for AS migrate."""
    # TODO: Write the new test once we have the code working again (maybe not here, maybe not
    #       using ``autosubmit migrate`` directly).
    exp = autosubmit_exp(experiment_data={})

    with pytest.raises(SystemExit) as cm:
        _autosubmit(["migrate", exp.expid, "-o"])

    assert cm.value.code == 1
