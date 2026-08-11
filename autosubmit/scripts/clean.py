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

"""clean

Cleans an experiment, reducing disk space usage.

It removes the project directory and outdated plots or stats.

When it deletes plots or stats, it keeps the two newest files
(i.e. two newest statistics files, and two newest plot files).

examples:

    # clean an experiment.
    $ autosubmit clean a000

    # clean the Autosubmit project folder of an experiment.
    $ autosubmit clean --project a000

    # clean the plot and stat files of an experiment.
    $ autosubmit clean --plot --stats a000
"""

from argparse import ArgumentParser

from autosubmit.scripts._args import (
    CommandGroup,
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class CleanOptions(ExpidOptions):
    """Options for the clean command."""

    project: bool
    plot: bool
    stats: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument("-pr", "--project", action="store_true", help="Clean project.")
    parser.add_argument(
        "-p", "--plot", action="store_true", help="Keep only the two most recently created plots."
    )
    parser.add_argument(
        "-s", "--stats", action="store_true", help="Keep only the most recently created statistics."
    )

    return parser


@cli_function(
    args_parser=args_parser,
    group=CommandGroup.EXPERIMENT,
    options_type=CleanOptions,
    validators=validate_expid,
)
def main(opts: CleanOptions) -> int | bool | None:
    from autosubmit.experiment.manage import clean

    return clean(opts.expid, opts.project, opts.plot, opts.stats)
