# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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

"""Script for handling experiment monitoring."""

import argparse
import traceback
from contextlib import suppress
from pathlib import Path
from typing import Optional, Union

from portalocker.exceptions import BaseLockException

from autosubmit.autosubmit import Autosubmit  # noqa: E402
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig  # noqa: E402
from autosubmit.log.log import Log, AutosubmitCritical, AutosubmitError  # noqa: E402

# Extend this set if a new lock-acquiring command is added.
LOCK_OWNING_COMMANDS = frozenset({'run', 'create', 'recovery', 'setstatus', 'pklfix'})

def delete_lock_file(base_path: Optional[Union[str, Path]] = None, lock_file: str = 'autosubmit.lock') -> None:
    """Delete lock file if it exists. Suppresses permission errors raised.

    :param base_path: Base path to locate the lock file. Defaults to the experiment ``tmp`` directory.
    :type base_path: Optional[Union[str, Path]]
    :param lock_file: The name of the lock file. Defaults to ``autosubmit.lock``.
    :type lock_file: str
    :return: None
    """
    if base_path is None:
        base_path = Log.file_path
    with suppress(PermissionError):
        Path(base_path, lock_file).unlink(missing_ok=True)


def _get_lock_path(args: Optional[argparse.Namespace]) -> Optional[Path]:
    if args and getattr(args, 'expid', None):
        return BasicConfig.expid_tmp_dir(args.expid)
    return None


def _owns_lock(args: Optional[argparse.Namespace]) -> bool:
    """Return True if the command in args is one that acquires the autosubmit lock."""
    return bool(args and getattr(args, 'command', None) in LOCK_OWNING_COMMANDS)


def exit_from_error(e: BaseException, lock_path: Optional[Union[str, Path]] = None) -> int:
    """Called by ``Autosubmit`` when an exception is raised during a command execution.

    Prints the exception in ``CRITICAL`` if is it an ``AutosubmitCritical`` or an
    ``AutosubmitError`` exception, including any trace attached to the exception.

    Exceptions raised by the ``portalocker`` library print a message informing the
    user about the locked experiment. For non-lock errors, the lock file is removed
    only when ``lock_path`` is provided (i.e. only when the failing command was
    a lock-owning command).

    Returns the resulting error code so the caller can use it as the process exit status.

    :param e: The exception being raised.
    :type e: BaseException
    :param lock_path: Path to the experiment tmp directory holding the lock file.
        ``None`` when the failing command does not own the lock; in that case no
        deletion is attempted.
    :type lock_path: Optional[Union[str, Path]]
    :return: The error code corresponding to the exception.
    :rtype: int
    """
    err_code = 1
    trace = traceback.format_exc()
    try:
        Log.critical(trace)
    except BaseException:
        print(trace)

    is_portalocker_error = isinstance(e, BaseLockException)
    is_autosubmit_error = isinstance(e, (AutosubmitCritical, AutosubmitError))

    if isinstance(e, BaseLockException):
        lock_file = Path(lock_path, 'autosubmit.lock') if lock_path else 'autosubmit.lock'
        Log.warning(f'Another Autosubmit instance is using the experiment.\n Stop the other instance(s) '
                    f'or delete the lock file located at: {lock_file}')
    elif lock_path is not None:
        delete_lock_file(lock_path)

    if is_autosubmit_error:
        e: Union[AutosubmitError, AutosubmitCritical] = e
        if e.trace:
            Log.critical(f"Trace: {str(e.trace)}")
        Log.critical(f"{e.message} [eCode={e.code}]")
        err_code = e.code

    if not is_portalocker_error and not is_autosubmit_error:
        msg = "Unexpected error: {0}.\n Please report it to Autosubmit Developers through Git: https://github.com/BSC-ES/autosubmit/issues"
        args = [str(e)]
        Log.critical(msg.format(*args))
        err_code = 7000

    Log.info("More info at https://autosubmit.readthedocs.io/en/master/troubleshooting/error-codes.html")
    return err_code


# noinspection PyProtectedMember
def main():
    args: Optional[argparse.Namespace] = None
    try:
        return_value, args = Autosubmit.parse_args()
        if args:
            return_value = Autosubmit.run_command(args)
        if _owns_lock(args):
            delete_lock_file(_get_lock_path(args))
    except BaseException as e:
        command = "<no command provided>"
        expid = "<no expid provided>"
        version = "<no version found>"
        if args:
            if getattr(args, 'command', None):
                command = f"<{args.command}>"
            if getattr(args, 'expid', None):
                expid = f"<{args.expid}>"
                with suppress(BaseException):
                    as_conf = AutosubmitConfig(args.expid)
                    as_conf.reload()
                    version = f"{as_conf.experiment_data.get('CONFIG', {}).get('AUTOSUBMIT_VERSION', 'unknown')}"
        Log.error(f"Arguments provided: {str(args)}")
        Log.error(f"This is the experiment: {expid} which had an issue with the command: {command} and it is currently using the Autosubmit Version: {version}.")
        return_value = exit_from_error(e, _get_lock_path(args) if _owns_lock(args) else None)
    # TODO: we need to define whether the function called here will return an int or bool
    if type(return_value) is bool:
        return_value = 0 if return_value else 1
    return return_value
