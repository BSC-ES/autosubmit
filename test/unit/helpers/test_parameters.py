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

from autosubmit.helpers.parameters import get_parameters

"""Tests for the ``helpers.parameters`` module."""


def test_autosubmit_decorator():
    """Test the ``autosubmit_decorator``."""
    parameters = get_parameters()

    parameter_names = [
        "CURRENT_ARCH",
        "EXPID",
        "CHUNK_END_DATE",
        "AUTOSUBMIT_VERSION",
        "TASKTYPE",
        "CHUNKSIZEUNIT",
        "PROJECT_DESTINATION",
    ]
    parameter_group = [
        "PLATFORM",
        "DEFAULT",
        "EDGE",
        "CONFIG",
        "JOB",
        "EXPERIMENT",
        "PROJECT",
    ]
    parameter_len = [12, 3, 24, 4, 38, 6, 2]
    parameter_value = [
        "Folder to hold the project sources.",
        "Unit of the chunk size. Can be hour, day, month, or year.",
        "Type of the job, as given on job configuration file.",
        "Current version of Autosubmit.",
        "Chunk end date",
        "Returns the experiment identifier",
        "Platform name.",
    ]

    assert len(parameters) == 8
    for parameter in parameter_group:
        assert parameter in parameters
        parameter_name = parameter_names.pop(0)
        assert parameter_name in parameters[parameter]
        assert parameter_value.pop() == parameters[parameter][parameter_name]
        assert len(parameters[parameter]) == parameter_len.pop(0)
