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

"""refresh

Refreshes the project files for an experiment.

Synchronises the experiment's project files with their configured source.
When the project uses Git, the project is cloned again, replacing the local
copy and discarding any local modifications.

Use ``--model_conf`` or ``--jobs_conf`` to also overwrite the corresponding
configuration files from the refreshed project.

Examples:

    # Refresh the project files for an experiment.
    $ autosubmit refresh a000

    # Refresh the project and overwrite the model configuration file.
    $ autosubmit refresh a000 --model_conf

    # Refresh the project and overwrite the jobs configuration file.
    $ autosubmit refresh a000 --jobs_conf

    # Refresh the project and overwrite both configuration files.
    $ autosubmit refresh a000 --model_conf --jobs_conf
"""

from argparse import ArgumentParser

from autosubmit.experiment.manage import refresh
from autosubmit.scripts._args import (
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class RefreshOptions(ExpidOptions):
    """Options for the refresh command."""

    model_conf: bool
    jobs_conf: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "-mc",
        "--model_conf",
        default=False,
        action="store_true",
        help="overwrite model conf file",
    )
    parser.add_argument(
        "-jc",
        "--jobs_conf",
        default=False,
        action="store_true",
        help="overwrite jobs conf file",
    )

    return parser


@cli_function(
    args_parser=args_parser, options_type=RefreshOptions, validators=validate_expid
)
def main(opts: RefreshOptions) -> int | bool | None:
    return refresh(opts.expid, opts.model_conf, opts.jobs_conf)
