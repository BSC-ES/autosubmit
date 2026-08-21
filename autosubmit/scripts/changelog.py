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

"""changelog

Displays the CHANGELOG.md file.
"""

from argparse import ArgumentParser
from pathlib import Path

from autosubmit.log.log import Log
from autosubmit.scripts._args import (
    AutosubmitOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function


class ChangelogOptions(AutosubmitOptions):
    """Options for the changelog command."""


def args_parser() -> ArgumentParser:
    """Create the argparse parser for the changelog command."""
    return create_argparse_parser(__doc__)


@cli_function(args_parser=args_parser, options_type=ChangelogOptions)
def main(_: ChangelogOptions) -> int | bool | None:
    project_root_dir = Path(__file__).parents[2]
    changelog_path = project_root_dir / "CHANGELOG.md"
    Log.info(changelog_path.read_text())
    return 0
