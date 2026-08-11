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

"""monitor

Plots the status of jobs in an experiment.

The monitoring plot provides an overview of an experiment's workflow and the
current status of its jobs. Jobs can be grouped by date, member, chunk, or split,
or grouped automatically.

Use the filtering options to focus the plot on specific jobs, job types, chunks,
or statuses. The plot can be displayed interactively or saved to a file with
``--output``.

Use ``--hide`` to generate the plot without opening a window, which is useful when
running Autosubmit in scripts or on systems without a graphical environment.

Examples:

    # Generate the default monitoring plot for an experiment.
    $ autosubmit monitor a000

    # Generate the monitoring plot as a PNG image.
    $ autosubmit monitor --output png a000

    # Group jobs by member instead of using automatic grouping.
    $ autosubmit monitor --group_by member a000

    # Show only jobs with a specific status.
    $ autosubmit monitor --filter_status FAILED a000
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid
from autosubmit.workflow.manage import monitor


class MonitorOptions(ExpidOptions):
    """Options for the monitor command."""

    project: bool
    plot: bool
    stats: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "-o",
        "--output",
        choices=("pdf", "png", "ps", "svg", "txt"),
        help="chooses type of output for generated plot",
    )  # Default -o value comes from .yml
    parser.add_argument(
        "-group_by",
        choices=("date", "member", "chunk", "split", "automatic"),
        default=None,
        help="Groups the jobs automatically or by date, member, chunk or split",
    )
    parser.add_argument(
        "-expand",
        type=str,
        help='Supply the list of dates/members/chunks to filter the list of jobs. Default = "Any". '
        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"',
    )
    parser.add_argument(
        "-expand_status", type=str, help="Select the stat uses to be expanded"
    )
    parser.add_argument(
        "--hide_groups",
        action="store_true",
        default=False,
        help="Hides the groups from the plot",
    )
    parser.add_argument(
        "-cw",
        "--check_wrapper",
        action="store_true",
        default=False,
        help="Generate possible wrapper in the current workflow",
    )
    group2 = parser.add_mutually_exclusive_group(required=False)
    parser.add_argument(
        "-fl",
        "--list",
        type=str,
        help='Supply the list of job names to be filtered. Default = "Any". '
        'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim"',
    )
    parser.add_argument(
        "-fc",
        "--filter_chunks",
        type=str,
        help='Supply the list of chunks to filter the list of jobs. Default = "Any". '
        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"',
    )
    parser.add_argument(
        "-fs",
        "--filter_status",
        type=str,
        choices=(
            "Any",
            "READY",
            "COMPLETED",
            "WAITING",
            "SUSPENDED",
            "FAILED",
            "UNKNOWN",
        ),
        help="Select the original status to filter the list of jobs",
    )
    parser.add_argument(
        "-ft",
        "--filter_type",
        type=str,
        help="Select the job type to filter the list of jobs",
    )
    parser.add_argument(
        "--hide", action="store_true", default=False, help="hides plot window"
    )
    group2.add_argument(
        "-txt",
        "--text",
        action="store_true",
        default=False,
        help="Generates only txt status file",
    )

    group2.add_argument(
        "-txtlog",
        "--txt_logfiles",
        action="store_true",
        default=False,
        help="Generates only txt status file(AS < 3.12b behaviour)",
    )

    return parser


@cli_function(
    args_parser=args_parser, options_type=MonitorOptions, validators=validate_expid
)
def main(opts: MonitorOptions) -> int | bool | None:
    return monitor(
        opts.expid,
        opts.output,
        opts.list,
        opts.filter_chunks,
        opts.filter_status,
        opts.filter_type,
        opts.hide,
        opts.text,
        opts.group_by,
        opts.expand,
        opts.expand_status,
        opts.hide_groups,
        opts.check_wrapper,
        opts.txt_logfiles
    )
