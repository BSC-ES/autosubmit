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

"""testcase

Creates a test experiment.
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    DefaultOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid_required_args


class TestcaseOptions(DefaultOptions):
    """Options for the testcase command."""

    copy: bool
    minimal_configuration: bool
    description: str
    chunks: str
    member: str
    startdate: str
    HPC: str
    git_repo: str
    git_branch: str
    git_as_conf: str
    use_local_minimal: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-y", "--copy", help="Make a copy of the specified experiment")
    group.add_argument(
        "-min",
        "--minimal_configuration",
        action="store_true",
        help="Create a new experiment with minimal configuration, usually combined with -repo.",
    )
    parser.add_argument(
        "-d", "--description", required=True, help="Description of the test case."
    )
    parser.add_argument("-c", "--chunks", help="Chunks to run.")
    parser.add_argument("-m", "--member", help="Member to run.")
    parser.add_argument("-s", "--stardate", help="Star date to run.")
    parser.add_argument(
        "-H", "--HPC", required=True, help="HPC to run the experiment on it."
    )

    parser.add_argument(
        "-repo",
        "--git_repo",
        type=str,
        default="",
        required=False,
        help="Set a Git repository for the experiment.",
    )
    parser.add_argument(
        "-b",
        "--git_branch",
        type=str,
        default="",
        required=False,
        help="Set a Git branch for the experiment.",
    )
    parser.add_argument(
        "-conf",
        "--git_as_conf",
        type=str,
        default="",
        required=False,
        help="Set the Git path to as_conf.",
    )
    parser.add_argument(
        "-local",
        "--use_local_minimal",
        required=False,
        action="store_true",
        help="Uses local minimal file instead of Git.",
    )

    return parser


def _validate(_: str, opts: TestcaseOptions) -> None:
    from warnings import warn

    if opts.chunks:
        warn("chunks is deprecated and will be removed in a future major release!")
    if opts.member:
        warn("member is deprecated and will be removed in a future major release!")
    if opts.stardate:
        warn("stardate is deprecated and will be removed in a future major release!")


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=TestcaseOptions,
    validators=validate_expid_required_args,
)
def main(opts: TestcaseOptions) -> int | bool | None:
    from autosubmit.experiment.manage import expid_fn

    return (
        expid_fn(
            opts.description,
            opts.HPC,
            opts.copy,
            False,
            opts.minimal_configuration,
            opts.git_repo,
            opts.git_branch,
            opts.git_as_conf,
            opts.use_local_minimal,
            testcase=True,
        )
        != ""
    )
