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

"""recovery

Recovers an interrupted workflow.

Updates the local workflow state based on the current status of jobs on the
configured platforms. This is useful when an experiment is interrupted
before completion and needs to be resumed with ``autosubmit run``.

By default, active jobs are checked so that completed remote jobs can be
marked as completed locally. Jobs that are still running remain active.
Before resuming the experiment, make sure active jobs have been stopped to
avoid submitting duplicate jobs. Use ``--force`` together with ``--save``
to cancel active jobs during recovery.

examples:

    # recover the active jobs without saving changes.
    $ autosubmit recovery a000

    # recover and save the workflow state.
    $ autosubmit recovery a000 --save

    # recover all jobs, including completed jobs, and save the changes.
    $ autosubmit recovery a000 --all --save

    # recover only failed jobs and save the changes.
    $ autosubmit recovery a000 --filter_status FAILED --save
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class RecoveryOptions(ExpidOptions):
    """Options for the recovery command."""

    noplot: bool
    plot: bool
    all: bool
    list: str
    filter_chunks: str
    filter_status: str
    filter_type: str
    save: bool
    hide: bool
    group_by: str
    expand: str
    expand_status: str
    no_recover_logs: bool
    detail: bool
    force: bool
    offline: bool


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
        "--all",
        action="store_true",
        default=False,
        help="Get completed files to synchronize pickle (pkl) file.",
    )
    parser.add_argument(
        "-fl",
        "--list",
        type=str,
        help='Supply the list of job names to be recovered. Default = "Any". '
        'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim".',
    )
    parser.add_argument(
        "-fc",
        "--filter_chunks",
        type=str,
        help='Supply the list of chunks to be recovered. Default = "Any". '
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
        help='Select the status (one or more) of jobs to be recovered. Default = "Any". '
        "Valid values = ['Any', 'READY', 'COMPLETED', 'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN'].",
    )
    parser.add_argument(
        "-ft",
        "--filter_type",
        type=str,
        help='Select the job type and split to be recovered. Default split = "Any". '
        'LIST = "LOCALJOB [5-10] SIM".',
    )
    parser.add_argument(
        "-s", "--save", action="store_true", default=False, help="Save changes to disk."
    )
    parser.add_argument(
        "--hide", action="store_true", default=False, help="Hide the plot window."
    )
    parser.add_argument(
        "-group_by",
        choices=("date", "member", "chunk", "split", "automatic"),
        default=None,
        help="Groups the jobs automatically or by date, member, chunk or split.",
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
        "-nl",
        "--no_recover_logs",
        action="store_true",
        default=False,
        help="Disable logs recovery (deprecated).",
    )
    parser.add_argument(
        "-d",
        "--detail",
        action="store_true",
        default=False,
        help="Show job list view in terminal.",
    )
    parser.add_argument(
        "-f", "--force", action="store_true", default=False, help="Cancel active jobs."
    )
    parser.add_argument(
        "-off",
        "--offline",
        action="store_true",
        default=False,
        help="Offline recovery.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.WORKFLOW,
    options_type=RecoveryOptions,
    validators=validate_expid,
)
def main(opts: RecoveryOptions) -> int | bool | None:
    from warnings import warn

    from autosubmit.workflow.manage import recover

    if opts.no_recover_logs:
        warn(
            "no_recover_logs is deprecated and will be removed in a future major release!"
        )
    return recover(
        opts.expid,
        opts.noplot,
        opts.save,
        opts.all,
        opts.hide,
        opts.group_by,
        opts.expand,
        opts.expand_status,
        opts.detail,
        opts.force,
        opts.offline,
        opts.list,
        opts.filter_chunks,
        opts.filter_status,
        opts.filter_type,
    )
