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

"""describe

Lists and shows details about experiments.

Lists the experiments available to the current user. One or more experiment
identifiers can be provided to show details for specific experiments.
Use ``--user`` to list or inspect experiments belonging to another user.

examples:

    # list the experiments available to the current user.
    $ autosubmit describe

    # show details for a specific experiment.
    $ autosubmit describe a000

    # show details for multiple experiments.
    $ autosubmit describe a000,a001,a002

    # list experiments belonging to another user.
    $ autosubmit describe --user bob
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    DefaultOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function


class DescribeOptions(DefaultOptions):
    """Options for the describe command."""

    expid: str | None
    user: str

    accepts_multiple_expids = True
    accepts_other_users = True
    """IMPORTANT: We annotate it for the sake of correctness; do not use the
    ``validate_expid`` validator for ``describe`` since it should not stop
    the flow of the execution for invalid experiment identifiers.
    """


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "expid",
        help="Experiment identifiers separated by commas.",
        default="*",
        nargs="?",
    )
    parser.add_argument(
        "-u",
        "--user",
        help="Username; defaults to the current user or the user associated with the experiment identifier.",
        default="",
    )

    return parser


@cli_function(
    args_parser=args_parser, group=CommandGroup.EXPERIMENT, options_type=DescribeOptions
)
def main(opts: DescribeOptions) -> int | bool | None:
    from autosubmit.experiment.manage import describe

    return describe(opts.expid, opts.user) is not None
