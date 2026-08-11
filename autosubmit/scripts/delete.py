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

"""delete

Deletes an experiment.

Removes the experiment from the database and file system.
"""

from argparse import ArgumentParser

from autosubmit.experiment.manage import delete_experiment
from autosubmit.scripts._args import (
    AutosubmitOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class DeleteOptions(AutosubmitOptions):
    """Options for the dbfix command."""

    profile: bool
    """Whether to profile the command execution."""
    # These are not passed via the command line.
    accepts_multiple_expids = True
    """Whether or not the sub-command accepts multiple expids. Default is no."""

    expid: str


def args_parser() -> ArgumentParser:
    """Create the argparse parser for the dbfix command."""
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "expid", help="experiment identifiers separated by commas", nargs="?"
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="deletes experiment without confirmation",
    )

    return parser


@cli_function(
    args_parser=args_parser, options_type=DeleteOptions, validators=validate_expid
)
def main(opts: DeleteOptions) -> int | bool | None:
    return delete_experiment(opts.expid, opts.force)
