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

"""inspect

Generates the job scripts for an experiment.

Reads the experiment configuration and generates the final job scripts that
will be submitted to the configured platforms. This is useful for checking
the generated scripts before running the experiment, including verifying
that configuration variables and template placeholders such as ``%VAR%``
have been replaced with their final values.

examples:

    # generate the job scripts for an experiment.
    $ autosubmit inspect a000

    # generate the scripts and overwrite existing ones.
    $ autosubmit inspect a000 --force

    # generate only one job per section to quickly check the configuration.
    $ autosubmit inspect a000 --quick

    # generate scripts only for jobs matching a specific status.
    $ autosubmit inspect a000 --filter_status READY
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class InspectOptions(ExpidOptions):
    """Options for the inspect command."""

    force: bool
    check_wrapper: bool
    quick: bool
    list: str
    filter_chunks: str
    filter_status: str
    filter_type: str


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument("-f", "--force", action="store_true", help="Overwrite all command files.")
    parser.add_argument(
        "-cw",
        "--check_wrapper",
        action="store_true",
        default=False,
        help="Generate possible wrapper in the current workflow.",
    )
    parser.add_argument(
        "-q",
        "--quick",
        action="store_true",
        help="Only check one job per each section.",
    )

    parser.add_argument(
        "-fl",
        "--list",
        type=str,
        help='Supply the list of job names to be filtered. Default = "Any". '
        'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim".',
    )
    parser.add_argument(
        "-fc",
        "--filter_chunks",
        type=str,
        help='Supply the list of chunks to filter the list of jobs. Default = "Any". '
        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]".',
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
        help="Select the original status to filter the list of jobs.",
    )
    parser.add_argument(
        "-ft",
        "--filter_type",
        type=str,
        help="Select the job type to filter the list of jobs.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.WORKFLOW,
    options_type=InspectOptions,
    validators=validate_expid,
)
def main(opts: InspectOptions) -> int | bool | None:
    from autosubmit.workflow.manage import inspect

    return inspect(
        opts.expid,
        opts.list,
        opts.filter_chunks,
        opts.filter_status,
        opts.filter_type,
        opts.force,
        opts.check_wrapper,
        opts.quick,
    )
