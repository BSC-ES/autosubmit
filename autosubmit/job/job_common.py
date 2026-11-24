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

import datetime
import re
from collections.abc import Iterable, Sequence
from functools import lru_cache

from autosubmit.log.log import AutosubmitCritical

__all__ = [
    "DEFAULT_START_CONDITION",
    "START_CONDITION_ALIASES",
    "SUCCEEDED_ENDINGS",
    "Status",
    "Type",
    "bcolors",
    "get_job_status",
    "increase_wallclock_by_chunk",
    "is_edge_satisfied",
    "max_wallclock_seconds",
    "parse_output_number",
    "parse_start_conditions",
    "separate_section_entries",
    "wallclock_to_seconds",
]

_WALLCLOCK_RE = re.compile(r"(((?P<hours>\d+):)((?P<minutes>\d+)))(:(?P<seconds>\d+))?")


def wallclock_to_seconds(wallclock: str | None) -> int | None:
    """Convert a ``HH:MM[:SS]`` wallclock to seconds.

    :param wallclock: Wallclock to convert, e.g. ``'07:30'`` or ``'07:30:00'``.
    :return: The wallclock in seconds, or ``None`` if it is empty, not a string or cannot be parsed.
    """
    if not wallclock or not isinstance(wallclock, str):
        return None
    match = _WALLCLOCK_RE.match(wallclock)
    if not match:
        return None
    parts = match.groupdict()
    hours = int(parts["hours"])
    minutes = int(parts["minutes"]) if parts["minutes"] else 0
    seconds = int(parts["seconds"]) if parts["seconds"] else 0
    return hours * 3600 + minutes * 60 + seconds


def max_wallclock_seconds(wallclocks: Iterable[str], platform_max_wallclock: str | None = None,
                          fallback: int = 0) -> int:
    """Return the longest wallclock (seconds) among the given ones.

    Entries without a wallclock (empty, unparseable or ``'00:00'``) fall back to the platform
    maximum wallclock. If no wallclock is available at all, ``fallback`` is returned.

    :param wallclocks: Wallclocks to evaluate.
    :param platform_max_wallclock: Platform maximum wallclock to use as fallback.
    :param fallback: Value returned when no wallclock is available.
    :return: The longest wallclock in seconds.
    """
    platform_max = wallclock_to_seconds(platform_max_wallclock) or 0
    longest = max((wallclock_to_seconds(wallclock) or platform_max for wallclock in wallclocks), default=0)
    return longest or fallback


class Status:
    """
    Class to handle the status of a job
    """
    WAITING = 0
    READY = 1
    SUBMITTED = 2
    QUEUING = 3
    RUNNING = 4
    COMPLETED = 5
    HELD = 6
    PREPARED = 7
    SKIPPED = 8
    FAILED = -1
    DELAYED = 9
    UNKNOWN = -2
    SUSPENDED = -3
    #######
    # Note: any change on constants must be applied on the dict below!!!
    VALUE_TO_KEY = {-3: 'SUSPENDED', -2: 'UNKNOWN', -1: 'FAILED', 0: 'WAITING', 1: 'READY', 2: 'SUBMITTED',
                    3: 'QUEUING', 4: 'RUNNING', 5: 'COMPLETED', 6: 'HELD', 7: 'PREPARED', 8: 'SKIPPED', 9: 'DELAYED'}
    KEY_TO_VALUE = {'SUSPENDED': -3, 'UNKNOWN': -2, 'FAILED': -1, 'WAITING': 0, 'READY': 1, 'SUBMITTED': 2,
                    'QUEUING': 3, 'RUNNING': 4, 'COMPLETED': 5, 'HELD': 6, 'PREPARED': 7, 'SKIPPED': 8, 'DELAYED': 9}
    LOGICAL_ORDER = ["SUSPENDED", "WAITING", "DELAYED", "PREPARED", "READY", "SUBMITTED", "HELD", "QUEUING", "RUNNING", "SKIPPED",
                     "FAILED", "UNKNOWN", "COMPLETED"]
    # Statuses with a job running on the platform that must be cancelled before leaving them.
    ACTIVE = (SUBMITTED, QUEUING, RUNNING)
    # Statuses that will be scheduled again: per-attempt state must be reset when entering them.
    RE_RUNNABLE = (WAITING, READY, DELAYED, PREPARED, SUSPENDED)

    def retval(self, value):
        return getattr(self, value)


START_CONDITION_ALIASES: dict[str, tuple[str, ...]] = {
    "SUCCEEDED": ("COMPLETED", "SKIPPED"),
    "FINISHED": ("COMPLETED", "SKIPPED", "FAILED"),
}
"""Aliases users can combine with exact status names."""

SUCCEEDED_ENDINGS = frozenset(START_CONDITION_ALIASES["SUCCEEDED"])
"""Statuses that always count as a successful ending."""

DEFAULT_START_CONDITION = ",".join(START_CONDITION_ALIASES["SUCCEEDED"])
"""The default start condition accepts any successful ending."""

_ALLOWED_START_CONDITIONS = frozenset(Status.KEY_TO_VALUE) - {"UNKNOWN", "SUSPENDED"}
"""Statuses that can be used as a start condition. ``UNKNOWN`` and ``SUSPENDED`` never are."""


def _status_name(status: int | str | None) -> str | None:
    """Return the canonical ``Status`` name for an integer code or a status name.

    :param status: A ``Status`` code, a status name, or ``None``.
    :return: The canonical status name, or ``None`` when it is unknown.
    """
    if isinstance(status, bool):
        return None
    if isinstance(status, str):
        name = status.upper()
        return name if name in Status.KEY_TO_VALUE else None
    if isinstance(status, int):
        return Status.VALUE_TO_KEY.get(status)
    return None


def split_start_condition_tokens(value: str) -> list[str]:
    """Split a comma or whitespace separated start condition string into raw tokens.

    :param value: Raw ``STATUS`` string.
    :return: Raw tokens, without normalizing case or expanding aliases.
    """
    return value.replace(",", " ").split()


def _start_condition_tokens(value: int | str | Sequence[str | int]) -> Iterable[str]:
    """Yield the raw start condition tokens from a scalar or ordered sequence.

    :param value: Status name, status code, comma or whitespace separated string, or sequence of those.
    :return: Raw tokens as strings, without normalizing case or expanding aliases.
    :raise ValueError: If the value is a boolean, an unordered container or an invalid code.
    """
    if isinstance(value, bool):
        raise ValueError(f"Invalid STATUS code '{value}'.")
    if isinstance(value, (set, frozenset, dict)):
        raise ValueError(
            "Invalid STATUS value. Use a status name or an ordered list of status names."
        )
    if isinstance(value, int):
        name = Status.VALUE_TO_KEY.get(value)
        if name is None:
            raise ValueError(f"Invalid STATUS code '{value}'.")
        yield name
        return
    if isinstance(value, str):
        yield from split_start_condition_tokens(value)
        return
    for item in value:
        yield from _start_condition_tokens(item)


def _expand_start_conditions(tokens: Iterable[str]) -> list[str]:
    """Expand aliases and deduplicate the given start condition tokens.

    :param tokens: Raw status names, without normalizing case or expanding aliases.
    :return: Canonical status names, or the default condition when no token is given.
    :raise ValueError: If a token is not a valid start condition status.
    """
    names: list[str] = []
    for token in tokens:
        name = token.upper()
        expanded = START_CONDITION_ALIASES.get(name, (name,))
        for status_name in expanded:
            if status_name not in _ALLOWED_START_CONDITIONS:
                allowed = ", ".join(sorted(_ALLOWED_START_CONDITIONS | set(START_CONDITION_ALIASES)))
                raise ValueError(
                    f"Invalid STATUS '{name}'. Allowed values are: {allowed}."
                )
            if status_name not in names:
                names.append(status_name)
    return names or list(START_CONDITION_ALIASES["SUCCEEDED"])


@lru_cache(maxsize=1024)
def _parse_start_condition_string(value: str) -> tuple[str, ...]:
    """Return the canonical status names for a comma or whitespace separated start condition.

    :param value: Comma or whitespace separated status names or aliases.
    :return: Canonical status names accepted by the condition.
    :raise ValueError: If a token is not a valid start condition status.
    """
    tokens = split_start_condition_tokens(value)
    return tuple(_expand_start_conditions(tokens))


def parse_start_conditions(
    value: int | str | Sequence[str | int] | None,
) -> list[str]:
    """Return the canonical status names accepted by a start condition.

    ``SUCCEEDED`` expands to ``COMPLETED, SKIPPED`` and ``FINISHED`` expands to
    ``COMPLETED, SKIPPED, FAILED``. Names are case-insensitive, can come as a comma or
    whitespace separated string or an ordered sequence, and are deduplicated preserving their
    order. An empty value returns the default condition, ``COMPLETED, SKIPPED``.

    :param value: Status name, status code, comma or whitespace separated string, sequence of those, or ``None``.
    :return: Canonical status names accepted by the condition.
    :raise ValueError: If a token is not a valid start condition status.
    """
    if value is None:
        return list(START_CONDITION_ALIASES["SUCCEEDED"])
    if isinstance(value, str):
        return list(_parse_start_condition_string(value))
    return _expand_start_conditions(_start_condition_tokens(value))


def is_edge_satisfied(
    parent_status: int | str,
    accepted_statuses: int | str | Sequence[str | int] | None,
    from_step: int = 0,
    parent_checkpoint_step: int = 0,
) -> bool:
    """Return whether a parent satisfies the start condition of a dependency edge.

    ``STATUS`` is a set of exact statuses. When no condition is given, the edge waits for a
    successful ending (``COMPLETED`` or ``SKIPPED``). ``COMPLETED``, ``SKIPPED`` and ``FAILED``
    are distinct statuses. ``FROM_STEP`` applies to ``RUNNING`` and ``FAILED``, which use the
    checkpoint step reached by the parent.

    :param parent_status: Current status of the parent job, as a ``Status`` code or its name.
    :param accepted_statuses: Accepted statuses, as names, codes, a comma or whitespace separated
        string or an ordered sequence of those.
    :param from_step: Internal step the parent must have reached, used with ``RUNNING`` and ``FAILED``.
    :param parent_checkpoint_step: Checkpoint step reached by the parent job.
    :return: ``True`` if the parent satisfies the edge, ``False`` otherwise.
    :raise AutosubmitCritical: If the start condition is not valid.
    """
    parent = _status_name(parent_status)
    if parent is None or parent in ("SUSPENDED", "UNKNOWN"):
        return False
    try:
        accepted = set(parse_start_conditions(accepted_statuses))
    except ValueError as error:
        raise AutosubmitCritical(
            f"Invalid start condition '{accepted_statuses}'. {error}",
            7014,
        ) from error
    if parent not in accepted:
        return False
    threshold = int(from_step or 0)
    if threshold == 0:
        return True
    if parent in ("RUNNING", "FAILED"):
        return int(parent_checkpoint_step or 0) >= threshold
    return True


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    # Status Colors
    UNKNOWN = '\033[37;1m'
    WAITING = '\033[37m'

    READY = '\033[36;1m'
    SUBMITTED = '\033[36m'
    QUEUING = '\033[35;1m'
    RUNNING = '\033[32m'
    COMPLETED = '\033[33m'
    SKIPPED = '\033[33m'
    PREPARED = '\033[34;2m'
    HELD = '\033[34;1m'
    FAILED = '\033[31m'
    DELAYED = '\033[36;1m'
    SUSPENDED = '\033[31;1m'
    CODE_TO_COLOR = {-3: SUSPENDED, -2: UNKNOWN, -1: FAILED, 0: WAITING, 1: READY,
                     2: SUBMITTED, 3: QUEUING, 4: RUNNING, 5: COMPLETED, 6: HELD, 7: PREPARED, 8: SKIPPED, 9: DELAYED}

class Type:
    """
    Class to handle the status of a job
    """
    BASH = 0
    PYTHON = 1
    R = 2
    PYTHON2 = 3
    PYTHON3 = 4

    def retval(self, value):
        return getattr(self, value)


# TODO: Statistics classes refactor proposal: replace tailer by footer

def parse_output_number(string_number: str) -> float:
    """
    Parses number in format 1.0K 1.0M 1.0G

    :param string_number: String representation of number
    :return: convertion to float format
    """
    number = 0.0
    if string_number:
        last_letter = string_number.strip()[-1]
        multiplier = 1
        if last_letter == "G":
            multiplier = 1000000000
            number = string_number[:-1]
        elif last_letter == "M":
            multiplier = 1000000
            number = string_number[:-1]
        elif last_letter == "K":
            multiplier = 1000
            number = string_number[:-1]
        else:
            number = string_number
        try:
            number = float(number) * multiplier
        except Exception:
            number = 0.0
    return number

def increase_wallclock_by_chunk(current: str, increase: str, chunk: int) -> str:
    """
    Receives the wallclock times an increases it according to a quantity times the number of the current chunk.
    The result cannot be larger than the platform max_wallclock.
    If Chunk = 0 then no increment.

    :param current: WALLCLOCK HH:MM
    :param increase: WCHUNKINC HH:MM
    :param chunk: chunk number
    :return: HH:MM wallclock
    """
    # Pipeline is not testing this since mock is not well-made
    try:
        if current and increase and chunk and chunk > 0:
            wallclock = current.split(":")
            increase = increase.split(":")
            current_time = datetime.timedelta(
                hours=int(wallclock[0]), minutes=int(wallclock[1]))
            increase_time = datetime.timedelta(
                hours=int(increase[0]), minutes=int(increase[1])) * (chunk - 1)
            final_time = current_time + increase_time
            hours = int(final_time.total_seconds() // 3600)
            minutes = int((final_time.total_seconds() // 60) - (hours * 60))
            if hours > 48 or (hours >= 48 and minutes > 0):
                hours = 48
                minutes = 0
            return "%02d:%02d" % (hours, minutes)
        return current
    except Exception:
        # print(exp)
        return current


def separate_section_entries(filter_entries: str) -> list[str]:
    """Separate section entries with optional splits separated by comma into a list.

    :param filter_entries: string with the entries separated by comma
    :return: list of entries
    """
    text = filter_entries.strip()
    if not text:
        return []

    entries = []
    for entry in text.split(","):
        entry = entry.strip()
        if not entry:
            continue
        entries.append(entry.upper())
    return entries


def get_job_status(status: str) -> int | None:
    """Convert job status from text to integer code.

    :param status: Status text.
    :return: The integer status value.
    """
    status = status.upper()
    if status == "READY":
        return Status.READY
    elif status == "COMPLETED":
        return Status.COMPLETED
    elif status == "WAITING":
        return Status.WAITING
    elif status == "HELD":
        return Status.HELD
    elif status == "SUSPENDED":
        return Status.SUSPENDED
    elif status == "FAILED":
        return Status.FAILED
    elif status == "RUNNING":
        return Status.RUNNING
    elif status == "QUEUING":
        return Status.QUEUING
    elif status == "SUBMITTED":
        return Status.SUBMITTED
    elif status == "UNKNOWN":
        return Status.UNKNOWN
    return None
