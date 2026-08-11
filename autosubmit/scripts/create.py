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

"""create

Creates the workflow graph of an experiment.

When executed the first time, it will try to refresh the experiment,
copying the project files (see "autosubmit refresh --help").

examples:

    # create the workflow graph of an experiment
    $ autosubmit create a000

    # create the workflow graph generating a PDF plot and open it
    $ autosubmit create --plot a000

    # create the workflow graph of an experiment with wrappers
    $ autosubmit create -cw a000
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class CreateOptions(ExpidOptions):
    """Options for the create command."""

    noplot: bool
    hide: bool
    output: str
    group_by: str
    expand: list | None
    expand_status: str
    check_wrapper: bool
    detail: bool
    force: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    plot_group = parser.add_mutually_exclusive_group(required=False)
    plot_group.add_argument(
        "-np",
        "--noplot",
        action="store_true",
        dest="noplot",
        help="Omit plot (default).",
    )
    plot_group.add_argument(
        "-plt",
        "--plot",
        action="store_false",
        dest="noplot",
        help="Generate plot.",
    )
    parser.set_defaults(noplot=True)
    parser.add_argument(
        "--hide", action="store_true", default=False, help="Hide the plot window."
    )
    parser.add_argument(
        "-d",
        "--detail",
        action="store_true",
        default=False,
        help="Show Job List view in terminal.",
    )
    parser.add_argument(
        "-o",
        "--output",
        choices=("pdf", "png", "ps", "svg", "txt"),
        help="Choose the type of output for generated plot.",
    )  # Default -o value comes from .conf
    parser.add_argument(
        "-group_by",
        choices=("date", "member", "chunk", "split", "automatic"),
        default=None,
        help="Group the jobs automatically or by date, member, chunk or split.",
    )
    parser.add_argument(
        "-expand",
        type=str,
        help='Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". '
        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]".',
    )
    parser.add_argument(
        "-expand_status", type=str, help="Select the statuses to be expanded."
    )
    parser.add_argument(
        "-cw",
        "--check_wrapper",
        action="store_true",
        default=False,
        help="Generate possible wrapper in the current workflow.",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force to regenerate the job list.",
    )
    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=CreateOptions,
    validators=validate_expid,
)
def main(opts: CreateOptions) -> int | bool | None:
    from autosubmit.experiment.manage import create

    return create(
        opts.expid,
        opts.noplot,
        opts.hide,
        opts.output,
        opts.group_by,
        opts.expand,
        opts.expand_status,
        opts.check_wrapper,
        opts.detail,
        opts.force,
    )
