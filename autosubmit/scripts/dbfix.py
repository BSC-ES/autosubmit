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

"""dbfix

Tries to fix a corrupted jobs database using backup SQL files.

Removed in 4.2.0 (joblist pull request)!
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid

INTERNAL = True


class DbfixOptions(ExpidOptions):
    """Options for the dbfix command."""


def args_parser() -> ArgumentParser:
    """Create the argparse parser for the dbfix command."""
    return create_argparse_parser(__doc__)


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.CONFIGURATION,
    options_type=DbfixOptions,
    validators=validate_expid,
)
def main(_: DbfixOptions) -> int | bool | None:
    from autosubmit.log.log import Log

    Log.error("Removed in 4.2.0 (joblist pull request)!")
    return 1
