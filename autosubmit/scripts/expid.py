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

"""expid

Creates a new experiment.

Creates a new experiment and returns its Experiment ID. The experiment can
be created with default or minimal configuration, copied from an existing
experiment, or configured to use files from a Git repository.

An HPC and description must be specified for the experiment, and the experiment
type can be set to operational, evaluation, or testcase.

examples:

    # create a new experiment with a description.
    $ autosubmit expid -d "My experiment"

    # create an experiment using a specific HPC.
    $ autosubmit expid -d "My experiment" -H marenostrum5

    # create a minimal experiment from a Git repository.
    $ autosubmit expid -d "My experiment" -min -repo https://github.com/example/project.git

    # create a new experiment by copying an existing experiment.
    $ autosubmit expid -d "My experiment copy" -y a000
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    DefaultOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid_required_args


class ExpidCommandOptions(DefaultOptions):
    """Options for the expid command."""

    operational: bool
    evaluation: bool
    testcase: bool
    dummy: bool
    minimal_configuration: bool
    filter_status: str
    copy: bool
    git_repo: str
    git_branch: str
    git_as_conf: str
    use_local_minimal: bool
    HPC: str
    description: str


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    group_experiment_types = parser.add_mutually_exclusive_group()
    group_experiment_types.add_argument(
        "-op",
        "--operational",
        action="store_true",
        help="Create a new experiment with operational experiment identifier.",
    )
    group_experiment_types.add_argument(
        "-ev",
        "--evaluation",
        action="store_true",
        help="Create a new experiment with evaluation experiment identifier.",
    )
    group_experiment_types.add_argument(
        "-t",
        "--testcase",
        action="store_true",
        help="Create a new experiment with testcase experiment identifier.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-dm",
        "--dummy",
        action="store_true",
        help="Create a new experiment with default values, usually for testing.",
    )
    group.add_argument(
        "-min",
        "--minimal_configuration",
        action="store_true",
        help="Create a new experiment with minimal configuration, usually combined with -repo.",
    )
    # TODO: This looks like a copy-pasta bug!
    group.add_argument(
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
    parser.add_argument("-y", "--copy", help="Make a copy of the specified experiment.")
    parser.add_argument(
        "-repo",
        "--git_repo",
        type=str,
        default="",
        required=False,
        help="Set a git repository for the experiment.",
    )
    parser.add_argument(
        "-b",
        "--git_branch",
        type=str,
        default="",
        required=False,
        help="Set a git branch for the experiment.",
    )
    parser.add_argument(
        "-conf",
        "--git_as_conf",
        type=str,
        default="",
        required=False,
        help="Set the git path to as_conf.",
    )
    parser.add_argument(
        "-local",
        "--use_local_minimal",
        required=False,
        action="store_true",
        help="Use local minimal file instead of Git.",
    )
    parser.add_argument(
        "-H",
        "--HPC",
        required=False,
        default="local",
        help="Specify the HPC to use for the experiment.",
    )
    parser.add_argument(
        "-d",
        "--description",
        type=str,
        required=True,
        help="Set a description for the experiment to store in the database.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=ExpidCommandOptions,
    validators=validate_expid_required_args,
)
def main(opts: ExpidCommandOptions) -> int | bool | None:
    from autosubmit.experiment.manage import expid_fn

    return (
        expid_fn(
            opts.description,
            opts.HPC,
            opts.copy,
            opts.dummy,
            opts.minimal_configuration,
            opts.git_repo,
            opts.git_branch,
            opts.git_as_conf,
            opts.operational,
            opts.testcase,
            opts.evaluation,
            opts.use_local_minimal,
        )
        != ""
    )
