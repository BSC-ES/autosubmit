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

"""upgrade

Upgrades an Autosubmit 3 experiment for use with Autosubmit 4.

It converts old ``INI/.conf`` files into YAML, and checks for deprecated
variables.
"""

from argparse import ArgumentParser

from autosubmit.config.upgrade_scripts import upgrade_scripts
from autosubmit.scripts._args import (
    AutosubmitOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class UpgradeOptions(AutosubmitOptions):
    """Options for the upgrade command."""

    expid: str
    """The experiment identifier."""
    profile: bool
    """Whether to profile the command execution."""
    # These are not passed via the command line.
    accepts_multiple_expids = False
    """Whether or not the sub-command accepts multiple expids. Default is no."""
    files: str


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument("-f", "--files", nargs="+", type=str, help="list of files")

    return parser


@cli_function(
    args_parser=args_parser, options_type=UpgradeOptions, validators=validate_expid
)
def main(opts: UpgradeOptions) -> int | bool | None:
    return upgrade_scripts(opts.expid, files=opts.files)
