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

"""provenance

Creates a provenance file for an experiment.

The provenance file describes the experiment and its workflow execution,
providing information that can be used to document and reproduce the
experiment.

Use ``--rocrate`` to additionally produce the provenance information
as an RO-Crate package. At the moment only RO-Crate is supported.

Examples:

    # Create a provenance file for an experiment.
    $ autosubmit provenance a000

    # Create provenance information in RO-Crate format.
    $ autosubmit provenance --rocrate a000
"""

from argparse import ArgumentParser

from autosubmit.experiment.manage import provenance
from autosubmit.scripts._args import (
    ExpidOptions,
    create_argparse_parser,
)
from autosubmit.scripts._cli_function import cli_function
from autosubmit.scripts._validation import validate_expid


class ProvenanceOptions(ExpidOptions):
    """Options for the provenance command."""

    rocrate: bool


def args_parser() -> ArgumentParser:
    parser = create_argparse_parser(__doc__)

    parser.add_argument(
        "--rocrate", action="store_true", default=False, help="produce an RO-Crate file"
    )

    return parser


@cli_function(
    args_parser=args_parser, options_type=ProvenanceOptions, validators=validate_expid
)
def main(opts: ProvenanceOptions) -> int | bool | None:
    return provenance(opts.expid, create_rocrate=opts.rocrate)
