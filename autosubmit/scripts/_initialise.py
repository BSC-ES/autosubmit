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

"""Sub-commands initialisation code."""

import locale
from contextlib import suppress
from sys import exit
from typing import TYPE_CHECKING

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database.db_common import update_experiment_description_version
from autosubmit.helpers.version import get_version
from autosubmit.log.log import Log
from autosubmit.scripts._args import parse_expids

if TYPE_CHECKING:
    from autosubmit.scripts._args import AutosubmitOptions


__all__ = ["initialise_command"]


def _set_locale() -> None:
    """Set a UTF-8 locale for consistent terminal and file output.

    Tries common UTF-8 locales and falls back to the ``C`` locale if none
    are available.
    """
    for locale_name in ("C.UTF-8", "C.utf8", "en_GB", "es_ES"):
        with suppress(locale.Error):
            locale.setlocale(locale.LC_ALL, locale_name)
            break
    else:
        Log.info("UTF-8 locale not found, using 'C' as fallback.")
        locale.setlocale(locale.LC_ALL, "C")


def initialise_command(command: str, opts: "AutosubmitOptions") -> None:
    """Initialise an Autosubmit sub-command.

    It is assumed this code is running after the validators. This is important
    as assumptions include that the user running the command is an owner, that
    the experiment IDs are valid, etc.

    This function:

    * sets the locale;
    * loads the Autosubmit configuration for each experiment;
    * checks that the configuration contains YAML data where required;
    * records the command as the last command used.

    :param command: The Autosubmit sub-command name.
    :param opts: Autosubmit options.
    """
    _set_locale()

    expid = getattr(opts, "expid", None)
    if not expid:
        return

    expids = parse_expids(expid)

    autosubmit_version = get_version()

    for expid in expids:
        as_conf = AutosubmitConfig(expid)
        as_conf.reload(force_load=True)

        # Check that the YAML data looks about right.
        if command not in ["expid", "upgrade"] and not as_conf.experiment_data:
            Log.error(
                f"Experiment '{expid}' contains no YAML configuration.\n"
                f'Please upgrade it with: "autosubmit upgrade {expid}"'
            )
            exit(1)

        # Update the experiment version, except if the user is archiving or upgrading.
        if command not in ["archive", "delete", "upgrade", "updateversion"]:
            if opts.update_version:
                if as_conf.get_version() != autosubmit_version:
                    Log.info(
                        f"The {expid} experiment {as_conf.get_version()} version is being "
                        f"updated to {autosubmit_version} to match the "
                        "Autosubmit version."
                    )
                    as_conf.set_version(autosubmit_version)
                    update_experiment_description_version(
                        expid,
                        version=autosubmit_version,
                    )
            elif (
                as_conf.get_version() is not None
                and as_conf.get_version() != autosubmit_version
            ):
                Log.error(
                    f"Current experiment uses ({as_conf.get_version()}) which is not "
                    f"the running Autosubmit version"
                    f"\nPlease, update the experiment version if you wish to continue "
                    f"using AutoSubmit {autosubmit_version}"
                    f"\nYou can achieve this using the command autosubmit "
                    f"updateversion {expid}"
                    f"\nOr with the -v parameter: autosubmit {command} {expid} -v"
                )
                exit(1)

        # Set the last command used. This is important as some sub-commands like ``run``, or
        # ``recovery`` check it and may fail if the last command (state) is not what is expected.
        as_conf.set_last_as_command(command)
