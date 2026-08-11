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

"""run

Starts or resumes the experiment workflow.

By default, the workflow is run using the configuration and state stored for
the specified experiment. Additional options can be used to control when the
workflow starts, restrict execution to specific ensemble members, or enable
profiling.

examples:

    # run an experiment normally:
    $ autosubmit run a000

    # run an experiment starting from a specific time:
    $ autosubmit run a000 --start_time 20250101T000000

    # start an experiment after another experiment has completed:
    $ autosubmit run a000 --start_after a001

    # run only selected ensemble members:
    $ autosubmit run a000 --run_only_members fc0,fc1,fc2
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


class RunOptions(ExpidOptions):
    """Options for the run command."""

    start_time: str
    start_after: str
    run_only_members: str
    profile_trace: bool
    profile_max_iterations: int


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "-st",
        "--start_time",
        required=False,
        help="Set the starting time for this experiment.",
    )
    parser.add_argument(
        "-sa",
        "--start_after",
        required=False,
        help="Set a experiment expid which completion will trigger the start of this experiment.",
    )
    parser.add_argument(
        "-rom",
        "--run_only_members",
        required=False,
        help="Set members allowed on this run.",
    )
    parser.add_argument(
        "--profile_trace",
        action="store_true",
        default=False,
        required=False,
        help="Enable trace output for profiling (requires --profile).",
    )
    parser.add_argument(
        "--profile_max_iterations",
        type=int,
        default=0,
        required=False,
        help="Optional maximum number of iterations for the profiler (0 = no hard cap).",
    )

    return parser


def _validate(_: str, opts: RunOptions):
    from autosubmit.log.log import Log

    # noinspection PyProtectedMember
    if opts.profile_trace and not opts.profile:
        Log.error(
            "Tracing is only available with profiling. Please add --profile flag to run with tracing."
        )
        exit(1)


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.WORKFLOW,
    options_type=RunOptions,
    validators=[validate_expid, _validate],
)
def main(opts: RunOptions) -> int | bool | None:
    from autosubmit.workflow.manage import run

    # noinspection PyProtectedMember
    return run(
        opts.expid,
        opts.start_time,
        opts.start_after,
        opts.run_only_members,
        opts._profiler,
        None,
    )
