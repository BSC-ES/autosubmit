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

"""stop

Stops running experiments.

Optionally, active jobs belonging to the experiment can also be cancelled
through the configured job platform (e.g., Slurm). Jobs can be filtered by
their current status and assigned a final status after cancellation.

Examples:

    # Stop a single experiment
    $ autosubmit stop a000

    # Stop multiple experiments
    $ autosubmit stop a000,a001,a002

    # Force-stop a single experiment
    $ autosubmit stop a000 --force

    # Stop all currently running Autosubmit processes
    $ autosubmit stop --all

    # Force-stop all currently running Autosubmit processes
    $ autosubmit stop --force_all

    # Stop all currently running Autosubmit processes without confirmation
    $ autosubmit stop --all --yes
"""

from argparse import Action, ArgumentParser

from autosubmit.scripts._args import (
    DefaultOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid
from autosubmit.workflow.manage import stop


class StopOptions(DefaultOptions):
    """Options for the stop command."""

    expid: str | None
    project: bool
    plot: bool
    stats: bool

    accepts_multiple_expids = True


class CancelAction(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, True)
        if (
            namespace.filter_status.upper() == "SUBMITTED, QUEUING, RUNNING "
            or namespace.target.upper() == "FAILED"
        ):
            pass
        else:
            parser.error("-fs and -t can only be used when --cancel is provided")


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "expid", help="experiment identifiers separated by commas", nargs="?"
    )
    parser.add_argument(
        "-f",
        "--force",
        default=False,
        action="store_true",
        help="Forces to stop autosubmit process, equivalent to kill -9",
    )
    group.add_argument(
        "-a",
        "--all",
        default=False,
        action="store_true",
        help="Stop all current running autosubmit processes, will ask for confirmation unless -y is used",
    )
    group.add_argument(
        "-fa",
        "--force_all",
        default=False,
        action="store_true",
        help="Stop all current running autosubmit processes",
    )
    parser.add_argument(
        "-y",
        "--yes",
        default=False,
        action="store_true",
        help="Automatically answer yes to prompts",
    )
    parser.add_argument(
        "-c",
        "--cancel",
        action=CancelAction,
        default=False,
        nargs=0,
        help="Orders to the schedulers to stop active jobs.",
    )
    parser.add_argument(
        "-fs",
        "--filter_status",
        type=str,
        default="SUBMITTED, QUEUING, RUNNING",
        help="Select the status (one or more) to filter the list of jobs.",
    )
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        default="FAILED",
        metavar="STATUS",
        help="Final status of killed jobs. Default is FAILED.",
    )

    return parser


@cli_function(
    args_parser=args_parser, options_type=StopOptions, validators=validate_expid
)
def main(opts: StopOptions) -> int | bool | None:
    return stop(
        opts.expid,
        opts.force,
        opts.all,
        opts.force_all,
        opts.cancel,
        opts.filter_status,
        opts.target,
        opts.yes,
    )
