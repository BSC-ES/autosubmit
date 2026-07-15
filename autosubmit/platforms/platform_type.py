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

from enum import Enum


class PlatformType(str, Enum):
    """The platform type.

    Note: use lowe-case only.
    """

    LOCAL = "local"
    ECACCESS = "ecaccess"
    PBS = "pbs"
    PJM = "pjm"
    PS = "ps"
    SLURM = "slurm"
    LOAD_LEVELER = "loadleveler"


__all__ = ["PlatformType"]
