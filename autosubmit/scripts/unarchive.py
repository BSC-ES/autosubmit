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

"""unarchive

Unarchives an experiment.

Decompresses the experiment archive, ensuring the experiment can be used again.
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function


class UnarchiveOptions(ExpidOptions):
    """Options for the unarchive command."""

    noclean: bool
    uncompressed: bool
    rocrate: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "-nclean",
        "--noclean",
        default=False,
        action="store_true",
        help="Avoid cleaning of experiment folder.",
    )
    parser.add_argument(
        "-uc",
        "--uncompressed",
        default=False,
        action="store_true",
        help="Extract files of the tar file without gzip compression.",
    )
    parser.add_argument(
        "--rocrate",
        action="store_true",
        default=False,
        help="Unarchive an RO-Crate file.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=UnarchiveOptions,
)
def main(opts: UnarchiveOptions) -> int | bool | None:
    from autosubmit.experiment.manage import unarchive

    return unarchive(
        opts.expid, uncompressed=opts.uncompressed, create_rocrate=opts.rocrate
    )
