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

"""dbmigrate

Migrates an experiment's databases from SQLite to PostgreSQL. (Not implemented yet.)

NOTE: Placeholder for #1286. It will be implemented in a future release.

examples:

    # migrate experiment a000 from SQLite to PostgreSQL:
    $ autosubmit dbmigrate a000
"""

from argparse import ArgumentParser
from sys import exit

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid

INTERNAL = True


class DbMigrateOptions(ExpidOptions):
    """Options for the dbmigrate command."""


def args_parser() -> ArgumentParser:
    """Create the argparse parser for the dbmigrate command."""
    return create_argparse_parser(__doc__)


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=DbMigrateOptions,
    validators=validate_expid,
)
def main(_: DbMigrateOptions) -> int | bool | None:
    from autosubmit.log.log import Log

    Log.error(
        "Migrating an experiment's database (SQLite -> PostgreSQL) is not implemented "
        "yet and will be added in a future release (see #1286)"
    )
    exit(1)
