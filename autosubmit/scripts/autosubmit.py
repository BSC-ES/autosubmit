#!/usr/bin/env python

# Copyright 2015 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Script for handling experiment monitoring"""
import argparse
import sys
from typing import Optional
from contextlib import suppress

# noinspection PyUnresolvedReferences
from log.log import Log, AutosubmitCritical, AutosubmitError  # noqa: E402
import sys
import os

from autosubmit import delete_lock_file, exit_from_error  # noqa: E402
from autosubmit.autosubmit import Autosubmit  # noqa: E402
from autosubmitconfigparser.config.configcommon import AutosubmitConfig  # noqa: E402


# noinspection PyProtectedMember
def main():
    args = Optional[argparse.Namespace]
    try:
        return_value, args = Autosubmit.parse_args()
        if args:
            return_value = Autosubmit.run_command(args)
        delete_lock_file()
    except BaseException as e:
        delete_lock_file()
        command = "<no command provided>"
        expid = "<no expid provided>"
        version = "<no version found>"
        if args:
            if 'command' in args and args.command:
                command = f"<{args.command}>"
            if 'expid' in args and args.expid:
                expid = f"<{args.expid}>"
                with suppress(BaseException):
                    as_conf = AutosubmitConfig(args.expid)
                    as_conf.reload()
                    version = f"{as_conf.experiment_data.get('CONFIG', {}).get('AUTOSUBMIT_VERSION', 'unknown')}"
        Log.error(f"Arguments provided: {str(args)}")
        Log.error(f"This is the experiment: {expid} which had an issue with the command: {command} and it is currently using the Autosubmit Version: {version}.")
        return_value = exit_from_error(e)
    return return_value


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)  # Sys.exit ensures a proper cleanup of the program, while os._exit() does not.
