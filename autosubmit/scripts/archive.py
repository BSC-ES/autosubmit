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

"""archive

Archives an experiment.

Compresses the experiment directory and moves it to the archival location.
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class ArchiveOptions(ExpidOptions):
    """Options for the archive command."""

    noclean: bool
    uncompress: bool
    rocrate: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "-nclean",
        "--noclean",
        default=False,
        action="store_true",
        help="Do not clean the experiment folder.",
    )
    parser.add_argument(
        "-uc",
        "--uncompress",
        default=False,
        action="store_true",
        help="Create the container without compression.",
    )
    parser.add_argument(
        "--rocrate", action="store_true", default=False, help="produce an RO-Crate file"
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=ArchiveOptions,
    validators=validate_expid,
)
def main(opts: ArchiveOptions) -> int | bool | None:
    from autosubmit.experiment.manage import archive
    return archive(
        opts.expid,
        noclean=opts.noclean,
        uncompress=opts.uncompress,
        create_rocrate=opts.rocrate,
    )
