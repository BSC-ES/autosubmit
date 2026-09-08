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

"""Unit tests for the ``autosubmit.scripts.expid`` module."""

from autosubmit.scripts.expid import args_parser


def test_expid_does_not_accept_filter_status():
    """The expid command must not expose the unrelated job-status filter."""
    option_strings = {
        option for action in args_parser()._actions for option in action.option_strings
    }

    assert "-fs" not in option_strings
    assert "--filter_status" not in option_strings
