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

"""Autosubmit module.

These files are the core of the Autosubmit, where all the
workflows will be handled, processed, and generated
"""

from os import environ


def environ_init():
    """Initialise Autosubmit environment.

    Python output buffering is disabled, so the output of stderr
    and stdout is not buffered, being written immediately. This
    avoids issues where the output is missing or written much
    later.
    """
    environ["PYTHONUNBUFFERED"] = "true"


environ_init()
