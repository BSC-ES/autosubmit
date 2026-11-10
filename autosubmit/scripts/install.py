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

"""install

Installs Autosubmit.

This command sets up the database, and creates the required directories.

It reads the global settings to choose the database and the location of the
directories.

If executed on an environment where Autosubmit was already installed, it will
print an error message and exit with a non-zero status.
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    AutosubmitOptions,
    CommandGroup,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function


class InstallOptions(AutosubmitOptions):
    """Options for the install command."""


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)
    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.CONFIGURATION,
    options_type=InstallOptions,
)
def main(opts: InstallOptions) -> int | bool | None:
    from autosubmit.install import install

    return install()
