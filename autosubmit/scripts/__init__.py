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

"""Autosubmit scripts and entry-points code.

This package contains code for handling command-line parsing.
There are modules that are marked as package/private with the
underscore in its name.

You probably should not import from this package in the rest
of the Autosubmit code. If you are doing so, there is probably
something wrong with the code design. The only exception is
testing.

* _args.py: Handles command-line arguments.
* _entry_points.py: The Python entry-point plug-ins (sub-commands).
* _terminal.py: Terminal printing, formatting.
* _validators.py: Handles validation of command-line arguments.

The rest of the files in this package are supposed to be only
sub-commands (entry-points). Avoid adding extra code, with other
features -- that code probably must reside in its own package
to keep consistency and organisation and avoid circular/complicated
dependencies.
"""
