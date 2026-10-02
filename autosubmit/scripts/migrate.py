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

"""migrate

Migrates an experiment between users. (Currently unavailable.)

NOTE: This command was removed and will be added again in a future release.

examples:

    # offer experiment a000 for migration:
    $ autosubmit migrate a000 --offer

    # pick up experiment a000 released for migration:
    $ autosubmit migrate a000 --pickup

    # move only remote files:
    $ autosubmit migrate a000 --offer --onlyremote
"""

from argparse import ArgumentParser
from sys import exit

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid

INTERNAL = True


class MigrateOptions(ExpidOptions):
    """Options for the migrate command."""

    filter_type: str
    filter_period: int
    output: str
    section_summary: bool
    jobs_summary: bool
    hide: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-o", "--offer", action="store_true", default=False, help="Offer experiment."
    )
    group.add_argument(
        "-p",
        "--pickup",
        action="store_true",
        default=False,
        help="Pick up released experiment.",
    )
    parser.add_argument(
        "-r",
        "--onlyremote",
        action="store_true",
        default=False,
        help="Only move remote files.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=MigrateOptions,
    validators=validate_expid,
)
def main(_: MigrateOptions) -> int | bool | None:
    from autosubmit.log.log import Log

    Log.error(
        "The command migrate was removed and will be added again in a future release"
    )
    exit(1)
