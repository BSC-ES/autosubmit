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

import re
from getpass import getuser
from pathlib import Path
from typing import List, Optional

from psutil import process_iter

from log.log import Log

"""Functions to handle linux processes."""

# TODO: Small note: in reality expids could have five or more letters.
#       Even if unlikely...
_EXPID_REGEX = re.compile(r'''
    ^[a-zA-Z0-9]{4}$            # An alpha-numeric string with 4 characters.
''', re.IGNORECASE | re.VERBOSE)
"""Regular expression to match an Autosubmit experiment ID."""


def _match_expid(token: str, expid=None) -> bool:
    """Match the expected experiment ID, or use a regex to match any.

    >>> _match_expid('123456')
    False
    >>> _match_expid('a000')
    True
    >>> _match_expid('a000', 'a001')
    False
    """
    if expid:
        return token == expid
    return re.search(_EXPID_REGEX, token) is not None


def _match_autosubmit_cmdline(cmdline: List[str], command='run', expid: Optional[str] = None) -> Optional[str]:
    """Guess if the command line is for ``autosubmit <COMMAND> <EXPID>``.

    The function tries to guess it by iterating the list of tokens produced by
    ``psutil`` (see their docs for ``cmdline``).

    It works by filtering the list for values like "autosubmit", "run", "<EXPID>"
    (where <EXPID> is retrieve with a regex). Then, it confirms that it in the
    final, and filtered, list we have only the values "autosubmit", "run",
    and the <EXPID>, in order.

    It would probably be more efficient and less error-prone to use the same
    argparse subparser that we use for the official ``autosubmit`` command,
    although it could change between versions...

    TODO: this still fails for things like ``autosubmit run a000 a000``;
          perhaps a better alternative would be for Autosubmit to keep
          track of the PID of experiments, and to also use the list of
          running experiments in the DB (we have an issue to make the
          DB status more reliable -- as at the moment it is not).

    >>> _match_autosubmit_cmdline(['autosubmit', 'create', 'a000'], 'create')
    'a000'
    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'a000'], 'run')
    'a000'
    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'a000', '-v'], 'run')
    'a000'
    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'a002'], 'run', 'a002')
    'a002'
    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'a000'])
    'a000'
    >>> _match_autosubmit_cmdline(['autosubmit', '-lc', 'DEBUG', 'run', 'a001'])
    'a001'
    >>> _match_autosubmit_cmdline(['autosubmit', '-lc', 'DEBUG', 'run', '--notransitive', 'a000'])
    'a000'
    >>> _match_autosubmit_cmdline(['autosubmit', '-lc', 'DEBUG', 'run', '--notransitive', 'a000'], 'run', None)
    'a000'
    >>> _match_autosubmit_cmdline(['/home/panda/envs/autosubmit/venv/bin/autosubmit', 'create', 'a000'], 'create')
    'a000'
    >>> _match_autosubmit_cmdline(['autoautosubmit', 'run', 'a000'])

    >>> _match_autosubmit_cmdline(['run', 'autosubmit', 'a000'])

    >>> _match_autosubmit_cmdline(['run', 'a000', 'autosubmit'])

    >>> _match_autosubmit_cmdline(['autosubmit', 'running', 'a000'])

    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'experiment'])

    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'a000'], 'create')

    >>> _match_autosubmit_cmdline(['autosubmit', 'run', 'a000'], 'run', 'a001')

    :param cmdline: A list of arguments of a command line (e.g. from shlex, or psutil).
    :type cmdline: List[str]
    :param command: An Autosubmit (sub)command.
    :type command: str
    :param expid: An Autosubmit experiment ID.
    :type expid: str
    :return: ``True`` if the command line matches the expected sequence, ``False`` otherwise.
    :rtype: bool
    """

    # Filter for cmdline tokens that are paths and end with "autosubmit", match exactly
    # the ``command`` provided, or are identical or similar to the ``expid`` provided.
    filtered_list = list(
        filter(
            lambda token: Path(token).name == 'autosubmit' or token == command or _match_expid(token, expid),
            cmdline
        )
    )
    # Verify we have the expected number of filtered elements, and in order - i.e. the
    # ``anything/anything/autosubmit``, followed by the ``command`` like "run", and
    # finally the expid at the end.
    if len(filtered_list) != 3:
        return None

    if not Path(filtered_list[0]).name == 'autosubmit':
        return None

    if filtered_list[1] != command:
        return None

    if not _match_expid(filtered_list[2]):
        return None

    return filtered_list[2]


def retrieve_expids() -> List[str]:
    """Retrieve all expids in use by autosubmit attached to the current user.

    :return: A list of Autosubmit experiment IDs of running experiments for the current user.
    :rtype: List[str]
    """
    user: str = getuser()
    expids: List[str] = []
    for process in [p for p in process_iter(['pid', 'cmdline', 'username']) if p.username() == user]:
        expid = _match_autosubmit_cmdline(process.cmdline())
        if expid:
            expids.append(expid)

    return expids


# TODO: check with Dani if the platform is needed (no processes using it on hub or destine vm?)
def process_id(expid: str, command="run") -> Optional[int]:
    """Retrieve the process id of the autosubmit process.

    :param expid: An Autosubmit experiment ID.
    :type expid: str
    :param command: An Autosubmit (sub)command.
    :type command: str
    :return: The process ID or an empty string if no process running.
    :rtype: Optional[int]
    """
    user: str = getuser()
    processes = [
        p for p
        in process_iter(['pid', 'cmdline', 'username'])
        if p.username() == user and _match_autosubmit_cmdline(p.cmdline(), command, expid)
    ]

    if not processes:
        return None

    if len(processes) > 1:
        Log.warning(f'Found more than one processes for "autosubmit {command} {expid}": {len(processes)}')

    return processes[0].pid
