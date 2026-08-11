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

"""stats

Generates statistics files and plots from workflow jobs.

examples:

    # generate statistics for experiment a000:
    $ autosubmit stats a000

    # generate a PNG image:
    $ autosubmit stats a000 --output png

    # include section and job summaries:
    $ autosubmit stats a000 --section_summary --jobs_summary

    # filter jobs by type:
    $ autosubmit stats a000 --filter_type SIM

    # filter jobs from the last 24 hours:
    $ autosubmit stats a000 --filter_period 24
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class StatsOptions(ExpidOptions):
    """Options for the stats command."""

    filter_type: str
    filter_period: int
    output: str
    section_summary: bool
    jobs_summary: bool
    hide: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "-ft",
        "--filter_type",
        type=str,
        help="Select the job type to filter the list of jobs.",
    )
    parser.add_argument(
        "-fp",
        "--filter_period",
        type=int,
        help="Select the period to filter jobs from current time to the past in "
             "number of hours back (must be greater than 0).",
    )
    parser.add_argument(
        "-o",
        "--output",
        choices=("pdf", "png", "ps", "svg"),
        default="pdf",
        help="Type of output for generated plot.",
    )
    parser.add_argument(
        "--section_summary",
        action="store_true",
        default=False,
        help="Include section summary in the plot.",
    )
    parser.add_argument(
        "--jobs_summary",
        action="store_true",
        default=False,
        help="Include jobs summary in the plot.",
    )
    parser.add_argument(
        "--hide", action="store_true", default=False, help="Hides plot window."
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.WORKFLOW,
    options_type=StatsOptions,
    validators=validate_expid,
)
def main(opts: StatsOptions) -> int | bool | None:
    from autosubmit.workflow.manage import statistics

    statistics(
        opts.expid,
        opts.filter_type,
        opts.filter_period,
        opts.output,
        opts.section_summary,
        opts.jobs_summary,
        opts.hide,
    )
