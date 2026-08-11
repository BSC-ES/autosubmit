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

"""setstatus

Changes the status of selected jobs in the Autosubmit database.

NOTE: Use ``--save`` to persist the changes to disk!

This can be used to skip completed parts of a workflow and allow downstream
jobs to run.

For example, jobs that have already been completed outside Autosubmit can be
marked as ``COMPLETED`` so that a downstream job, such as ``CLEAN``, can run.

Examples:

    # Mark selected jobs as completed and save the changes.
    $ autosubmit setstatus a000 --status_final COMPLETED --save

    # Mark specific jobs as completed.
    $ autosubmit setstatus a000 --status_final COMPLETED --list "JOB1 JOB2" --save

    # Mark jobs in a specific status as completed.
    $ autosubmit setstatus a000 --status_final COMPLETED --filter_status WAITING --save

    # Mark jobs as completed for specific chunks.
    $ autosubmit setstatus a000 --status_final COMPLETED --filter_chunks "[ 19601101 [ fc0 [1 2 3 4] ] ]" --save
"""

from argparse import ArgumentParser

from autosubmit.job.manage import set_status
from autosubmit.scripts._args import (
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class SetstatusOptions(ExpidOptions):
    """Options for the setstatus command."""

    noplot: bool
    plot: bool
    save: bool
    status_final: str
    list: str
    filter_chunks: str
    filter_status: str
    filter_type: str
    filter_type_chunk: str
    filter_type_chunk_split: str
    hide: bool
    group_by: str
    expand: str
    expand_status: str
    notransitive: bool
    check_wrapper: bool
    detail: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    plot_group = parser.add_mutually_exclusive_group(required=False)
    plot_group.add_argument(
        "-np",
        "--noplot",
        action="store_true",
        dest="noplot",
        help="omit plot (default)",
    )
    plot_group.add_argument(
        "-plt",
        "--plot",
        action="store_false",
        dest="noplot",
        help="generate plot",
    )
    parser.set_defaults(noplot=True)
    parser.add_argument(
        "-s",
        "--save",
        action="store_true",
        default=False,
        help="Save changes to disk",
    )
    parser.add_argument(
        "-t",
        "--status_final",
        choices=(
            "READY",
            "COMPLETED",
            "WAITING",
            "SUSPENDED",
            "UNKNOWN",
            "HELD",
        ),
        required=True,
        help="Supply the target status",
    )
    parser.add_argument(
        "-fl",
        "--list",
        type=str,
        help='Supply the list of job names to be changed. Default = "Any". '
        'LIST = "b037_20101101_fc3_21_sim b037_20111101_fc4_26_sim"',
    )
    parser.add_argument(
        "-fc",
        "--filter_chunks",
        type=str,
        help='Supply the list of chunks to change the status. Default = "Any". '
        'LIST = "[ 19601101 [ fc0 [1 2 3 4] fc1 [1] ] 19651101 [ fc0 [16-30] ] ]"',
    )
    parser.add_argument(
        "-fs",
        "--filter_status",
        type=str,
        help="Select the status (one or more) to filter the list of jobs."
        "Valid values = ['Any', 'READY', 'COMPLETED', 'WAITING', 'SUSPENDED', 'FAILED', 'UNKNOWN']",
    )
    parser.add_argument(
        "-ft",
        "--filter_type",
        type=str,
        help='Select the job type and split to filter the list of jobs. Default split = "Any". '
        'LIST = "LOCALJOB [5-10] SIM"',
    )
    parser.add_argument(
        "-ftc",
        "--filter_type_chunk",
        type=str,
        help='[Deprecated] Equivalent behaviour can be achieved by combining -ft and -fc. \
                                   Supply the list of chunks to change the status. Default = "Any". When the member name "all" is set, all the chunks \
                                   selected from for that member will be updated for all the members. Example: all [1], will have as a result that the \
                                       chunks 1 for all the members will be updated. Follow the format: '
        '"[ 19601101 [ fc0 [1 2 3 4] Any [1] ] 19651101 [ fc0 [16-30] ] ],SIM,SIM2,SIM3"',
    )
    parser.add_argument(
        "-ftcs",
        "--filter_type_chunk_split",
        type=str,
        help='[Deprecated] Equivalent behaviour can be achieved by combining -ft and -fc. \
                                    Supply the list of chunks & splits to change the status. Default = "Any". When the member name "all" is set, all the chunks \
                                               selected from for that member will be updated for all the members. Example: all [1], will have as a result that the \
                                                   chunks 1 for all the members will be updated. Follow the format: '
        '"[ 19601101 [ fc0 [1 [1 2] 2 3 4] Any [1] ] 19651101 [ fc0 [16-30] ] ],SIM,SIM2,SIM3"',
    )

    parser.add_argument(
        "--hide", action="store_true", default=False, help="hides plot window"
    )
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
        "-expand_status", type=str, help="Select the statuses to be expanded"
    )
    parser.add_argument(
        "-nt",
        "--notransitive",
        action="store_true",
        default=False,
        help="Disable transitive reduction",
    )
    parser.add_argument(
        "-cw",
        "--check_wrapper",
        action="store_true",
        default=False,
        help="Generate possible wrapper in the current workflow",
    )
    parser.add_argument(
        "-d",
        "--detail",
        action="store_true",
        default=False,
        help="Generate detailed view of changes",
    )

    return parser


@cli_function(
    args_parser=args_parser, options_type=SetstatusOptions, validators=validate_expid
)
def main(opts: SetstatusOptions) -> int | bool | None:
    return set_status(
        opts.expid,
        opts.noplot,
        opts.save,
        opts.status_final,
        opts.list,
        opts.filter_chunks,
        opts.filter_status,
        opts.filter_type,
        opts.filter_type_chunk,
        opts.filter_type_chunk_split,
        opts.hide,
        opts.group_by,
        opts.expand,
        opts.expand_status,
        opts.check_wrapper,
        opts.detail,
    )
