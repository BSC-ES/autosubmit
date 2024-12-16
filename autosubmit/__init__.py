import traceback
from contextlib import suppress
from os import _exit  # type: ignore
from pathlib import Path
from typing import Union

from portalocker.exceptions import BaseLockException

from log.log import Log, AutosubmitCritical, AutosubmitError


def delete_lock_file(base_path: str = Log.file_path, lock_file: str = 'autosubmit.lock') -> None:
    """Delete lock file if it exists. Suppresses permission errors raised.

    :param base_path: Base path to locate the lock file. Defaults to the experiment ``tmp`` directory.
    :type base_path: str
    :param lock_file: The name of the lock file. Defaults to ``autosubmit.lock``.
    :type lock_file: str
    :return: None
    """
    with suppress(PermissionError):
        Path(base_path, lock_file).unlink(missing_ok=True)


def exit_from_error(e: BaseException) -> int:
    """Called by ``Autosubmit`` when an exception is raised during a command execution.

    Prints the exception in ``DEBUG`` level.

    Prints the exception in ``CRITICAL`` if is it an ``AutosubmitCritical`` or an
    ``AutosubmitError`` exception.

    Exceptions raised by ``porta-locker` library print a message informing the user
    about the locked experiment. Other exceptions raised cause the lock to be deleted.

    After printing the exception, this function calls ``os._exit(1)``, which will
    forcefully exit the executable running.

    :param e: The exception being raised.
    :type e: BaseException
    :return: None
    """
    err_code = 1
    trace = traceback.format_exc()
    try:
        Log.debug(trace)
    except:
        print(trace)

    is_portalocker_error = isinstance(e, BaseLockException)
    is_autosubmit_error = isinstance(e, (AutosubmitCritical, AutosubmitError))

    if isinstance(e, BaseLockException):
        Log.warning('Another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are '
                    'using the experiment or delete autosubmit.lock file located on the /tmp folder.')
    else:
        delete_lock_file()

    if is_autosubmit_error:
        e: Union[AutosubmitError, AutosubmitCritical] = e  # type: ignore
        if e.trace:
            Log.debug("Trace: {0}", str(e.trace))
        Log.critical("{1} [eCode={0}]", e.code, e.message)
        err_code = e.code

    if not is_portalocker_error and not is_autosubmit_error:
        msg = "Unexpected error: {0}.\n Please report it to Autosubmit Developers through Git"
        args = [str(e)]
        Log.critical(msg.format(*args))
        err_code = 7000

    Log.info("More info at https://autosubmit.readthedocs.io/en/master/troubleshooting/error-codes.html")
    return err_code
