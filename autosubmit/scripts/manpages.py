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

"""manpages

Generate Autosubmit manual pages.

The generated manual pages are written to the specified output directory.

NOTE: This is an experimental command.
"""

from argparse import ArgumentParser
from pathlib import Path

from autosubmit.scripts._args import (
    AutosubmitOptions,
    CommandGroup,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._entry_points import get_commands

INTERNAL = True


class ManpagesOptions(AutosubmitOptions):
    """Options for the manpages command."""

    output_dir: Path


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("man"),
        help="Directory in which to write the manual pages.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.CONFIGURATION,
    options_type=ManpagesOptions,
)
def main(opts: ManpagesOptions) -> int | bool | None:
    from autosubmit.scripts._manpages import write_man_pages

    write_man_pages(get_commands(), opts.output_dir)
    return 0
