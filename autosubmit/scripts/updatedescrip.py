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

"""updatedescrip

Updates the description of an experiment.

Examples:

    $ autosubmit updatedescrip
"""

from argparse import ArgumentParser

from autosubmit.database.db_common import update_experiment_description_version
from autosubmit.scripts._args import (
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class UpdatedescripOptions(ExpidOptions):
    """Options for the updatedescrip command."""

    description: str


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument("description", help="New description.")
    return parser


@cli_function(
    args_parser=args_parser,
    options_type=UpdatedescripOptions,
    validators=validate_expid,
)
def main(opts: UpdatedescripOptions) -> int | bool | None:
    return update_experiment_description_version(opts.expid, opts.description)
