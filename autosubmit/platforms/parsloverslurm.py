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

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
import textwrap

class ParslOverSlurmPlatform(ParamikoPlatform):
    """Class to manage jobs to host using SLURM scheduler."""

    def __init__(self) -> None:
        """
        The ParslOverSlurm platform provides an interface to facilitate building 
        Parsl scripts that will run on a remote platform.

        :rtype: None
        """
        self._header = ParslOverSlurmHeader()

class ParslOverSlurmHeader(object):
    """
    Class to handle the header of a job that runs with Parsl.
    """

    SERIAL = textwrap.dedent("""\
###############################################################################
# The following lines contain the script. [%TASKTYPE% %DEFAULT.EXPID% EXPERIMENT]
###############################################################################
           """)

    PARALLEL = SERIAL
