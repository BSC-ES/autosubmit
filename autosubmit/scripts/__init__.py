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
* _cli_function.py: Code for the ``@cli_function`` decorator.
* _completion.py: Bash code completion.
* _entry_points.py: The Python entry-point plug-ins (sub-commands).
* _initialise.py: Runs required steps for each sub-command.
* _manpages.py: Linux manpage generation.
* _terminal.py: Terminal printing, formatting.
* _traceability.py: Display traceability information in each sub-command.
* _validation.py: Handles validation of command-line arguments.

The rest of the files in this package are supposed to be only
sub-commands (entry-points). Avoid adding extra code, with other
features -- that code probably must reside in its own package
to keep consistency and organisation and avoid circular/complicated
dependencies.

Also for the sake of consistency, try to follow:

- usage, options, and examples are lower case (usage and options forced
  by argparse);
- Help text starts with an upper case letter, first person if a verb, and ends
  with a period (e.g., "Set time.", "List of users.");
- Deprecated options raise a command-line warning (``warnings``);
- When adding options, if in doubt look at POSIX/GNU commands as
  reference, or other workflow managers like Cylc, ecFlow (user familiarity);
- Use long options like --all, and short options like -a. There is no need
  to always have both. Having only a long-form is perfectly fine. Avoid mixing
  --long and -short (to avoid issues like --all and -all).

These are not so arbitrary options. usage/options comes from argparse and
many POSIX/GNU commands. Upper-case first letter from the need to separate
sentences with periods. And first/third person simply follows Cylc example.

If a command like ``autosubmit --help`` becomes slow, it might be due
to imports that transitively import ``networkx`` or ``matplotlib``. These
large dependencies may take up to a second or more to load on some virtual
machine or environments with vCPUs. Use lazy imports to solve that.

Use a spell checker to verify options. Once we add a command or option, it
may be difficult to remove or modify it later.

If still in doubt, try to aim for consistency, and least surprise to users.
"""
