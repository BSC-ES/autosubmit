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

"""sqlitefix

Tries to fix a corrupted SQLite database.

Removed in 4.2.0 (joblist pull request)!
"""

from argparse import ArgumentParser

from autosubmit.log.log import Log
from autosubmit.scripts._args import (
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class SqlitefixOptions(ExpidOptions):
    """Options for the sqlitefix command."""

    force: bool


def args_parser() -> ArgumentParser:
    """Create the argparse parser for the sqlitefix command."""
    parser = create_argparse_parser(__doc__)
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="force restore without confirmation",
    )
    return parser


@cli_function(
    args_parser=args_parser, options_type=SqlitefixOptions, validators=validate_expid
)
def main(_: SqlitefixOptions) -> int | bool | None:
    Log.error("Removed in 4.2.0 (joblist pull request)!")
    return 1
