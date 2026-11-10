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

"""configure

Configures the environment settings for Autosubmit.

This command creates configuration settings for database and for the files
and directories needed for Autosubmit to function correctly.

Configuration can be performed at machine, user, or local level. By default,
Autosubmit uses SQLite and stores its database and experiment files under
``$HOME/autosubmit``.

The database backend can be changed to PostgreSQL by using ``--database-backend
postgres`` and providing the corresponding database connection URL.

examples:

    # configure Autosubmit using the default SQLite settings.
    $ autosubmit configure

    # configure Autosubmit for all users.
    $ autosubmit configure --all

    # configure Autosubmit for use only from the current path.
    $ autosubmit configure --local

    # configure the database using PostgreSQL.
    $ autosubmit configure --database-backend postgres \\
        --database-conn-url "postgresql://user:password@host:5432/autosubmit"

    # specify a custom SQLite database path and filename.
    $ autosubmit configure \\
        --databasepath /path/to/autosubmit \\
        --databasefilename autosubmit.db

    # specify a custom experiments root path.
    $ autosubmit configure \\
        --localrootpath /path/to/experiments

    # specify the default platforms and jobs configuration files.
    $ autosubmit configure \\
        --platformsconfpath /path/to/platforms.yml \\
        --jobsconfpath /path/to/jobs.yml

    # configure SMTP settings for email notifications.
    $ autosubmit configure \\
        --smtphostname smtp.example.com \\
        --mailfrom autosubmit@example.com

    # open the advanced Autosubmit configuration.
    $ autosubmit configure --advanced
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    AutosubmitOptions,
    CommandGroup,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function


class ConfigureOptions(AutosubmitOptions):
    """Options for the configure command."""

    advanced: bool
    database_backend: str
    database_conn_url: str | None
    databasepath: str | None
    databasefilename: str | None
    localrootpath: str | None
    platformsconfpath: str | None
    jobsconfpath: str | None
    smtphostname: str | None
    mailfrom: str | None
    all: bool
    local: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "--advanced",
        action="store_true",
        help="Open advanced configuration of autosubmit.",
    )
    parser.add_argument(
        "--database-backend",
        choices=("sqlite", "postgres"),
        default="sqlite",
        help="Select the database backend to use. Default is sqlite.",
    )
    parser.add_argument(
        "--database-conn-url",
        default=None,
        help="Database connection URL string. Required for Postgres backend.",
    )
    parser.add_argument(
        "-db",
        "--databasepath",
        default=None,
        help="Path to SQLite database. Defaults to $HOME/autosubmit if not supplied. "
        "Required for SQLite backend.",
    )
    parser.add_argument(
        "-dbf", "--databasefilename", default=None, help="Database filename."
    )
    parser.add_argument(
        "-lr",
        "--localrootpath",
        default=None,
        help="Path to store experiments. Defaults to $HOME/autosubmit if not supplied.",
    )
    parser.add_argument(
        "-pc",
        "--platformsconfpath",
        default=None,
        help="Optional path to platforms.yml file to use by default.",
    )
    parser.add_argument(
        "-jc",
        "--jobsconfpath",
        default=None,
        help="Optional path to jobs.yml file to use by default.",
    )
    parser.add_argument(
        "-sm", "--smtphostname", default=None, help="Optional SMTP server hostname."
    )
    parser.add_argument(
        "-mf", "--mailfrom", default=None, help="Optional notifications sender address."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="Configure for all users.")
    group.add_argument(
        "--local",
        action="store_true",
        help="Configure only for using Autosubmit from this path.",
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.CONFIGURATION,
    options_type=ConfigureOptions,
)
def main(opts: ConfigureOptions) -> int | bool | None:
    from autosubmit.install import configure

    return configure(
        opts.advanced,
        opts.databasepath,
        opts.databasefilename,
        opts.localrootpath,
        opts.platformsconfpath,
        opts.jobsconfpath,
        opts.smtphostname,
        opts.mailfrom,
        opts.all,
        opts.local,
        opts.database_backend,
        opts.database_conn_url,
    )
